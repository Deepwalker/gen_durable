# Architecture map

Fast-orientation map of the codebase — read this before grepping. It stays high-level
(module responsibilities, the data model, the flows, where invariants live) so it doesn't rot;
for the *why* and the exact mechanics follow the pointers in [Going deeper](#going-deeper).

## Mental model in one paragraph

An FSM instance is a **row**, not a process. `insert` writes a `runnable` row; a per-queue
**Scheduler** claims it (`runnable → executing`, leased), an ephemeral **Task** runs one
`step/2`, and the **outcome** commits the next state (`→ runnable`/`awaiting_*`/`done`/`failed`)
**before** execution proceeds — that commit is the durability guarantee. A crash before it
re-runs the step (at-least-once). Everything else — leases + reaper, the K=1 unique arbiter,
out-of-band rate/concurrency admission, signals, await, fan-out — exists to make that loop
correct and fast under concurrency and node death.

## Module map (`lib/gen_durable/`)

| Module | Responsibility |
|---|---|
| `GenDurable` (`gen_durable.ex`) | Public API: `insert`/`insert_all`/`await`/`signal`/`child_spec`; builds insert params; fires pokes. |
| `Supervisor` (`supervisor.ex`) | Engine supervisor. Parses opts, builds the `config` (into `:persistent_term` `{GenDurable, name}`), seeds limiter policy, starts the per-node component tree. **Start here to see what runs.** |
| `Scheduler` (`scheduler.ex`) | Per-queue feeder+executor loop. Claims into a small buffer, spawns ≤`concurrency` Tasks, heartbeats claimed rows, discovers work by poke/poll, drains on shutdown. |
| `Executor` (`executor.ex`) | Runs one picked job: resolve FSM → load state → `step/2` (guarded) → apply outcome → credit the limiter slot → poke downstream wakes. |
| `Queries` (`queries.ex`) | **Every SQL statement, one function each.** The pick (`@claim_sql`), the `complete_*` outcomes, heartbeat/reap/reclaim, GC + bucket reconcile, insert/signal. The single largest and most invariant-dense file. |
| `Limiter` (`limiter.ex`) | Behaviour + dispatch for **out-of-band admission** of configured limits: `admit`/`credit`/`renew`/`sync_config`/`reconcile`. A limiter is a `{module, handle}`. |
| `Limiter.Postgres` (`limiter/postgres.ex`) | Default backend: the sharded `gen_durable_buckets` admission math (`@admit_sql`), as standalone statements over the claimed batch. |
| `Limiter.Redis` (`limiter/redis.ex`) | Redis backend: a lease-scored ZSET semaphore (self-heals on lease expiry) + a Lua token bucket. Single-node Redis. Requires optional `:redix`. |
| `Reaper` (`reaper.ex`) | Sweeps expired leases (`executing` past `lease_expires_at`) back to `runnable` — the crash-recovery floor. Optional per node. |
| `GC` (`gc.ex`) | Prunes terminal rows past retention; drives `Limiter.reconcile` (bucket self-heal). Optional per node. |
| `Poke` (`poke.ex`) | The poke transport (`:local`/`:cluster`/`{:redis,_}`): announce new runnable work to schedulers so it's discovered without waiting for the poll. Best-effort. |
| `Await` (`await.ex`) | Sync-over-async: the Watcher (same-node waiter table + batched cross-node poller) backing `await/3`. |
| `FSM` (`fsm.ex`) | `use GenDurable.FSM` — the `step/2` / `perform/1` contract, queue binding, state-module wiring. |
| `State` (`state.ex`) | `use GenDurable.State` — the typed `embedded_schema` state and its jsonb `to_db`/`from_db`. |
| `Context` (`context.ex`) | The `ctx` struct handed to `step/2`: id, step, state, `awaited`/`all` (signals), `childs`. |
| `Outcome` (`outcome.ex`) | Validates/normalizes a step's return: `:next` / `:retry` / `:await` / `:done` / `:stop` / `:schedule_childs`. |
| `Registry` (`registry.ex`) | FSM-module resolution (dynamic by `fsm` column, or explicit `:fsms`); the supervisor-owned ETS table. |
| `Migration` (`migration.ex`) | The DDL. `up/1` applies only the version increments an install is missing (`@latest_version = 2`). |
| `Testing` (`testing.ex`) | Inline synchronous `drain/1` for host tests (runs the real pick/executor in the calling process). |

## Data model (schema v2)

Four tables. `gen_durable` is the instance; the rest support it.

- **`gen_durable`** — one row per FSM instance. Drives everything through `status`
  (`runnable` → `executing` → `awaiting_signal`/`awaiting_children` → `done`/`failed`).
  Key columns: `fsm`/`fsm_version`/`step`/`state`(jsonb); `queue`/`priority`/`eligible_at`
  (scheduling); `concurrency_key`/`concurrency_shard`/`rate_limit`/`weight` (limits);
  `locked_by`/`lease_expires_at`/`attempt` (claim + leasing); `awaits`/`await_deadline`
  (signals); `parent_id`/`children_pending` (fan-out join); `correlation_key`/`_guard`/`_scope`
  (identity/dedup); `result`/`last_error`.
- **`signals`** — the durable inbox. `(id, target_id→gen_durable, name, payload jsonb,
  dedup_key)`, `UNIQUE (target_id, dedup_key)` for at-most-once delivery.
- **`gen_durable_bucket_configs`** — `(kind, name, rate, capacity, shards)`, `kind ∈ {rate,conc}`.
  The limiter **policy** (seeded from engine opts at boot).
- **`gen_durable_buckets`** — `(kind, key, shard, capacity, available, last_refill)`. The PG
  limiter's sharded counters (minted lazily, swept when idle). *Unused by the Redis backend.*

Indexes worth knowing (each backs a specific invariant/scan):

| Index | Backs |
|---|---|
| `gen_durable_pick (queue, priority, eligible_at) WHERE runnable` | the pick scan |
| `gen_durable_concurrency_active UNIQUE (concurrency_key) WHERE executing AND concurrency_shard IS NULL` | **the K=1 arbiter** — at most one executing row per *unconfigured* key (gated rows carry a shard and drop out) |
| `gen_durable_correlation UNIQUE (correlation_guard)` | identity/dedup + signal addressing |
| `gen_durable_lease (lease_expires_at)` | the reaper sweep |
| `gen_durable_await_deadline` / `gen_durable_parent` / `gen_durable_gc (updated_at)` | await-timeout sweep / fan-out join / retention sweep |
| `signals_target (target_id, name)` | inbox lookup + wake |

## Runtime topology (per node, under `GenDurable.Supervisor`)

- a `:pg` **scope** (poke membership — schedulers join their queue's group)
- **`Await.Watcher`** (idle-free; backs `await/3`)
- a **`Task.Supervisor`** (runs the step Tasks)
- **poke children** — a Redis publisher + `Poke.Listener`, only under `{:redis,_}`
- **limiter children** — a Redix connection, only under `limiter: {:redis,_}`
- **`Reaper`** and **`GC`** — each optional per node (`reaper: false`/`gc: false` for
  web-only/worker-only topologies; the cluster needs ≥1 of each somewhere)
- one **`Scheduler`** per configured queue

Config lives in `:persistent_term` under `{GenDurable, name}`, so several named instances
coexist and the public API resolves the right one.

## Core flows

- **Insert → run.** `insert` writes a `runnable` row and pokes the queue → the `Scheduler`
  `pick`s (claim `runnable→executing` leased, K=1 in-band, configured limits admitted
  out-of-band — see below) → an `Executor` Task runs `step/2` → a `complete_*` outcome commits
  the next state under the ownership guard (`locked_by` + `executing`) and credits the limiter
  slot → downstream pokes fire.
- **Limiter (configured rate/concurrency).** The pick claims candidates lock-light, then
  `Limiter.admit/2` decides who runs (debiting tokens / taking slots) as a separate statement;
  denied rows are released back to `runnable`. On outcome, `Limiter.credit/2` returns the slot;
  the heartbeat calls `Limiter.renew/2` (Redis bumps lease scores; PG no-ops); GC calls
  `Limiter.reconcile/1` (PG recomputes counters from the executing truth; Redis relies on lease
  expiry). K=1 for *unconfigured* keys is NOT the limiter — it's the arbiter index.
- **Signals & await.** `signal` resolves a live target (by id or `correlation_guard`), inserts
  into `signals`, and wakes a parked row in one statement. `await/3` reads the settled row,
  pushed to same-node waiters instantly and polled cross-node by the Watcher.
- **Fan-out.** `:schedule_childs` inserts children (in their queues) and parks the parent on a
  `children_pending` join barrier; each terminal child decrements it, waking the parent at zero.
- **Crash recovery.** No outcome committed ⇒ the lease expires ⇒ the `Reaper` returns the row
  to `runnable` ⇒ it's re-picked and the step re-runs. A stale worker's late outcome is dropped
  by the ownership guard.

## Invariants (and where they live)

- **Durability / at-least-once** — the outcome commits before proceeding (`Queries.complete_*`);
  a pre-commit crash re-runs the step.
- **Ownership guard** — every outcome commits only while `locked_by = $worker AND status =
  'executing'`; side effects ride the guarded UPDATE's `RETURNING` (`Queries`).
- **K=1 mutual exclusion (unconfigured keys)** — `gen_durable_concurrency_active` unique index.
- **Configured limits** — out-of-band `GenDurable.Limiter` (`admit`/`credit`/`renew`/`reconcile`).
- **Leasing** — `Scheduler` heartbeat + `Reaper`; startup reclaim releases a dead predecessor's
  claims early.
- **Self-heal** — PG `reconcile_concurrency` recomputes counters from executing rows; Redis
  slots self-heal on lease expiry; both limiters mint cold buckets/keys **pre-debited**
  (zero-lag cold path).

## Going deeper

- **`guides/internals.md`** — the actual DB mechanics: the pick, outcomes, locking, self-heal.
- **`PERFORMANCE.md`** — the cost model, statement/round-trip counts, EXPLAIN plans, §6 known
  pathologies.
- **`ISSUES.md`** — the design-review findings ledger: *why* many mechanisms exist (each closes
  a specific race). Read the relevant entry before touching a subsystem.
- **`guides/*.md`** — per-feature docs (jobs, machines, signals, children, rate_limiting,
  concurrency, identity, scheduling, testing, operations).
- **`CLAUDE.md`** — how to test, the versioning/migration/doc conventions.
