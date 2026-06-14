# gen_durable — implementation plan

Status: draft v1. Companion to `gen_durable_spec.md` (normative). This document is the **how**;
the spec is the **what**. Where this plan and the spec disagree, the spec wins — fix this plan.

The engine is an Elixir library: a Postgres-backed durable FSM runtime on top of GenServer.
The one guarantee we are building toward: *on step completion, the new state is committed to the
database before execution proceeds* (spec §1). Everything below serves that sentence.

---

## 0. Tech stack and key decisions

These are the choices that shape every later phase. Each has a recommendation and the reasoning;
items marked **CONFIRM** are worth a yes/no before we lean on them.

| Decision | Recommendation | Why |
|---|---|---|
| Language / build | Elixir library, Mix, `lib/gen_durable/…` | Spec is GenServer-based. |
| DB driver | `ecto_sql` + `postgrex` (**DECIDED**) | Use `Ecto.Migration` for the DDL and an `Ecto.Repo` for connection pooling, but run hot-path statements as **raw SQL** (`Repo.query!/Postgrex`). The spec is SQL-first (`FOR UPDATE SKIP LOCKED`, generated columns, advisory locks); we do not hide those behind the query DSL. |
| New-work latency | **Polling only. `LISTEN/NOTIFY` is banned, permanently.** (**DECIDED**) | Spec §6 mandates a "dumb picker". Polling is trivially correct; NOTIFY is not an option in this codebase. Do not add it, do not propose it. |
| `state` encoding | **Typed struct per FSM**, jsonb ↔ struct via Ecto embedded schema (**DECIDED**) | Each FSM declares its own state struct; the engine loads jsonb → struct before `step/2` and dumps struct → jsonb on the outcome. `ctx.state` is that struct, not a raw map. Living with a plain map is allowed but **unsupported** — you're on your own. |
| FSM definition | `use GenDurable.FSM` behaviour, `step/2` + `handle/2` callbacks, `state:` struct module | The `step` text column maps to a function-clause head; `state:` names the embedded schema. |
| FSM versioning | **Explicit registry** `{fsm_name, fsm_version} => module` in config/start (**DECIDED**) | Spec §8: old instances finish on their `fsm_version`. Old versions (`Checkout.V1`, `Checkout.V2`) coexist as explicitly-registered modules; we resolve module per row, never assume "latest", no compile-time magic. |
| Defaults (timings) | lease 60s · heartbeat 20s · poll 1s · reaper 30s ("Balanced", **DECIDED**) | All config-overridable. Margin: `heartbeat × 3 = lease`. |
| Signal consume | **Name-scoped (spec §5)** — on a progressing outcome the engine deletes the inbox signals whose `name = awaits`, then clears `awaits` (**DECIDED → option (b)**) | Non-matching signals survive until their own await. Delivery keeps `awaits`; `:await` also guards against a pre-arrived signal (no lost wake-up). Supersedes the earlier snapshot-delete idea. |
| Step ↔ connection | one DB connection is **held for the whole step** when `partition_key` is set | Session-level `pg_try_advisory_lock` must live on a connection that survives the step, which runs *outside* any transaction. This ties a connection to each in-flight partitioned step — call it out in capacity planning. |

### The advisory-lock subtlety (read this before Phase 9)

`partition_key` serialization (spec §6, Pick) uses a **session-level** advisory lock, not
`pg_advisory_xact_lock`, because the step body runs outside a transaction. Consequences:

- The lock must be taken on, and held by, the *same connection* for the lock's lifetime, then
  released after the outcome commits (`pg_advisory_unlock`, or drop the connection).
- The picking `UPDATE` runs in a short transaction on a pooled connection; the lock attempt and the
  step run on a (possibly different) checked-out connection. Be deliberate about which connection
  owns the lock.
- Worker death drops the connection → Postgres releases the advisory lock automatically. This is the
  free recovery the spec relies on; do not "optimize" it by reusing connections across steps.

---

## 1. Architecture overview

```
GenDurable.Application (supervisor)
├── GenDurable.Repo                      (Ecto repo / Postgrex pool)
├── GenDurable.Registry                  ({fsm, version} -> module)
├── GenDurable.Reaper                    (periodic: expired leases -> runnable, attempt+1)
└── GenDurable.Queue.Supervisor          (one child per configured queue/pool)
    └── GenDurable.Queue "default"
        ├── Scheduler  (GenServer: poll, run Pick for free slots, hand rows to workers)
        ├── Heartbeat  (extends lease_expires_at for in-flight rows)
        └── Task.Supervisor  (one Task per executing step; local concurrency = pool size)
```

Data flow of one step:

1. **Scheduler** picks `runnable` rows (short txn, `FOR UPDATE SKIP LOCKED`) → `executing` + lease.
2. If `partition_key` set, **Executor** takes the advisory lock; on conflict, return row to `runnable`.
3. **Executor** loads pending signals (if relevant), resolves the module, calls `step/2` under `try`.
4. Outcome (or `handle/2` outcome on exception) is written in **one transaction**, deleting consumed
   signals in that same transaction (spec §5).
5. Advisory lock released, heartbeat stopped.

Crash with no outcome → lease expires → **Reaper** resets to `runnable`, `attempt + 1`, step re-runs
from scratch (spec §4.3). `handle/2` is *not* called on a reap — that is the at-least-once floor.

---

## 2. Module layout

```
lib/gen_durable.ex                  # public API: insert/2, insert_all/2, signal/4
lib/gen_durable/application.ex
lib/gen_durable/repo.ex
lib/gen_durable/fsm.ex               # `use GenDurable.FSM`, behaviour + __using__
lib/gen_durable/state.ex             # `use GenDurable.State` — embedded schema + load/dump codec
lib/gen_durable/outcome.ex           # outcome constructors / validation (the §3 table)
lib/gen_durable/context.ex           # ctx struct passed to step/2 and handle/2
lib/gen_durable/registry.ex          # {name, version} -> module
lib/gen_durable/queries.ex           # ALL raw SQL from spec §10, one function each
lib/gen_durable/executor.ex          # run one picked row end-to-end
lib/gen_durable/scheduler.ex         # per-queue picker loop + concurrency accounting
lib/gen_durable/heartbeat.ex
lib/gen_durable/reaper.ex
lib/gen_durable/signals.ex           # sender-side delivery
lib/gen_durable/migration.ex         # GenDurable.Migration.up/1 down/1 — host app calls it (Oban-style)
test/support/test_repo/migrations/..._setup.exs   # test repo only; just calls GenDurable.Migration.up()
```

`queries.ex` is the single home for the spec's SQL so §10 maps 1:1 to functions and stays auditable.
The library ships **no** Ecto migration files of its own — the DDL lives in `GenDurable.Migration`
and the host app writes a one-line migration that delegates to it (see §4).

---

## 3. Public API (target shape)

```elixir
# Typed state struct — one per FSM, an Ecto embedded schema
defmodule Checkout.State do
  use GenDurable.State            # thin wrapper over `embedded_schema`
  embedded_schema do
    field :order,   :integer
    field :n,       :integer, default: 0
    field :shipped, :boolean, default: false
  end
end

# Defining a machine
defmodule Checkout do
  use GenDurable.FSM, version: 1, queue: "checkout", state: Checkout.State

  @impl true
  def step("start",     %{state: s} = _ctx), do: {:next, "await_pay", %{s | n: s.n + 1}}
  def step("await_pay", ctx),                 do: {:await, "payment_confirmed", ctx.state}
  def step("ship",      %{state: s} = _ctx),  do: {:done, %{"shipped" => true}}

  @impl true
  def handle(reason, ctx) do
    if ctx.attempt < 5, do: {:replay, ctx.state, backoff(ctx.attempt)}, else: {:stop, reason}
  end
end

# Enqueue — `state:` is cast into Checkout.State
{:ok, id} = GenDurable.insert(Checkout,
  state: %{order: 42},
  partition_key: "order:42",
  priority: 0,
  unique_key: <<...>>, unique_scope: ["runnable", "executing", "awaiting_signal"])

# Batch (single statement, dedup via the partial unique index)
GenDurable.insert_all(Checkout, [ %{...}, %{...} ])

# Signal into an await (durable, at-least-once)
GenDurable.signal(id, "payment_confirmed", %{"amount" => 100}, dedup_key: "evt-7")
```

`ctx.state` is a **`%Checkout.State{}` struct** (loaded from jsonb before the call); `step/2`/`handle/2`
return a struct of the same type in `:next`/`:replay`/`:await`, which the engine dumps back to jsonb.
`result` in `:done` stays a plain string-keyed map (terminal payload, never re-loaded into step code).

`ctx` for `step/2`: `%Context{id, fsm, fsm_version, step, attempt, state, signals}`.
`ctx` for `handle/2`: same minus `signals` (spec §4.2: `%{id, fsm, fsm_version, step, attempt, state}`).

Outcomes returned by `step/2` and `handle/2` are exactly the spec §3 table:
`{:next, step, state}` · `{:replay, state, delay_ms}` · `{:await, name, state}` ·
`{:done, result}` · `{:stop, reason}`. `Outcome` validates the shape before it touches SQL.

---

## 4. Data model

### Migration: library-owned, Oban-style

The DDL lives in `GenDurable.Migration`, not in copy-pasted Ecto migration files. The host app writes
a thin migration that delegates:

```elixir
defmodule MyApp.Repo.Migrations.SetupGenDurable do
  use Ecto.Migration
  def up,   do: GenDurable.Migration.up()        # opts: [prefix: "public", version: :latest]
  def down, do: GenDurable.Migration.down()
end
```

`GenDurable.Migration`:
- `up(opts \\ [])` / `down(opts \\ [])`, both run `execute/1` of the spec §9 DDL **verbatim** (the
  generated column, partial indexes, and enum are clearer as raw SQL than via the migration DSL).
- `:prefix` — target Postgres schema (default `public`); `:version` — schema version to migrate to
  (default latest). v1 ships **version 1 only**.
- The installed schema version is recorded in `COMMENT ON TABLE gen_durable` (Oban's trick), read on
  `up` so only missing increments apply. This keeps the user-facing call stable when we add a v2.

Tables created: `gen_durable`, `signals`, the `durable_status` enum, the three indexes
(`gen_durable_pick`, `gen_durable_lease`, `gen_durable_unique`) plus `signals_target`.

### Spec deviation D1 — `unique_scope` must be `durable_status[]`, not `text[]`

Spec §7/§9 define `unique_scope text[]` and the generated column as
`status::text = any(unique_scope)`. **Postgres rejects this**: a `generated always as (...) stored`
expression must be IMMUTABLE, but the enum→text cast (`enum_out`) is only STABLE (enum labels can be
renamed). Result: `ERROR 42P17 generation expression is not immutable`.

Fix applied in `GenDurable.Migration` (v1): store `unique_scope` as `durable_status[]` and compare
`status = any(unique_scope)` (enum = enum, `enum_eq` is IMMUTABLE — the cast disappears). The §7
semantics are unchanged. Downstream impact: inserts pass scope as `$scope::durable_status[]`, and the
engine reads scope as enum-label strings. **The normative spec §7/§9 should be patched to match.**

Notes to verify during implementation:
- `unique_guard` is `generated always as (...) stored`; the partial unique index is on it.
- Batch insert uses `ON CONFLICT (unique_guard) WHERE unique_guard is not null DO NOTHING` — the
  inference predicate must match the partial index predicate exactly, or PG won't pick the index.
- `signals` has `unique (target_id, dedup_key)`; `dedup_key NULL` ⇒ no dedup (NULLs don't conflict).

---

## 5. Component design

### 5.1 Queries (`queries.ex`)
One function per spec §10 block, all parameterized, all raw SQL:
`pick/3`, `heartbeat/2`, `reap/0`, `complete_next/…`, `complete_replay/…`, `complete_await/…`,
`complete_done/…`, `complete_stop/…`, `deliver_signal/…`, `insert/…`, `insert_all/…`.
Progressing `complete_*` run a name-scoped signal `DELETE` (signals whose `name = awaits`) **and** the
outcome `UPDATE` (which clears `awaits`) in one txn; `complete_await` guards a pre-arrived signal.

### 5.2 Executor (`executor.ex`)
Given a picked row map, run the step to a committed outcome:
1. If `partition_key`, `pg_try_advisory_lock(hashtext(partition_key))` on the step connection; on
   `false`, reset row to `runnable` (drop lease) and stop — another worker owns the key.
2. Load pending signals for `id` into `ctx.signals` (read-only view; deletion is name-scoped in SQL
   on the outcome, not by these ids).
3. Resolve module via `Registry.fetch!({fsm, fsm_version})`; **load jsonb → state struct** (the FSM's
   `state:` embedded schema, via `Ecto.embedded_load`); build `Context` with the struct.
4. `try` → `module.step(step, ctx)`; on raised exception → `module.handle(reason, ctx)`; if
   `handle/2` itself raises → write `:stop`/`failed` (spec §4.2).
5. **Dump the returned state struct → jsonb** (`Ecto.embedded_dump`), then apply the outcome via the
   matching `complete_*` query, which name-scope-deletes the awaited signals in the same txn.
6. `pg_advisory_unlock`, stop heartbeat.

A worker process crash before step 5 means *no outcome row* → reaper path. We do **not** trap and
convert crashes into `handle/2`; crash ≠ caught exception (spec §4 distinguishes the three sources).

### 5.3 Scheduler (`scheduler.ex`) — feeder + executor with backpressure
Per queue. A finished Task refills immediately, so throughput is bounded by
`concurrency / step_time`, **not** by `poll_interval` (the poll timer only governs idle discovery
latency). Backpressure-driven with four aggressiveness knobs (see F5 below and the module doc):
- `concurrency` — executor width (max Tasks at once).
- `prefetch` — extra rows claimed into an in-memory buffer **beyond** the running slots (`0` ⇒ today's
  pick-exactly-free-slots). Buffered rows are `executing`+leased and are **heartbeated** (the
  heartbeat set is `buffer ++ in_flight`), so buffer depth is decoupled from `lease_ttl` — they never
  go stale. Cost of depth: cross-node fairness (claimed rows are invisible to other nodes) and
  priority freshness; crash blip is bounded by the (short) TTL, not by depth.
- `min_demand` — batch gate: don't pick unless ≥ this many slots are free (fat picks), bypassed when
  fully idle to avoid starvation.
- `poll_interval` / `max_poll_interval` — idle backoff: an empty pick on a fully idle queue doubles
  the interval up to the ceiling; any fetched or in-flight work snaps back to the base. The lever that
  cuts idle DB load (NOTIFY is banned → poll adaptively, not constantly).

Loop: `refill` (pick up to `demand = concurrency + prefetch − claimed` into the buffer when the gate
allows) → `drain` (spawn buffered jobs, highest-priority first, into free slots). Demand-driven, so
we never claim more than `concurrency + prefetch`.

### 5.4 Heartbeat (`heartbeat.ex`)
Extends `lease_expires_at` for in-flight rows on an interval `< lease_ttl` (e.g. `lease_ttl/3`).
Implementable as one process per queue ticking over the in-flight set, or per-step timer. Lease TTL
and heartbeat interval are config; document the safety margin (`heartbeat × k < lease_ttl`).

### 5.5 Reaper (`reaper.ex`)
Periodic GenServer running the spec §10 Reaper `UPDATE`. Global (not per-queue). Interval config.

### 5.6 Signals (`signals.ex`)
`signal/4`: the spec §10 sender transaction — insert the signal row (with `ON CONFLICT … DO NOTHING`
for dedup) **then** flip `awaiting_signal`+matching `awaits` to `runnable`. Insert-before-flip so the
re-executed step already sees the signal in its inbox (spec §5).

---

## 6. Milestones

**Status: M0–M10 all ✅ DONE.** 33 tests green (Elixir 1.18 / OTP 27 + Postgres 17 in the
devcontainer). The whole engine — typed/map state, `:next` loop, `await`/`signal`, `handle`→
`:replay`→`:stop`, reaper crash-recovery, uniqueness, and `partition_key` serialization — runs
end-to-end. See module map in §2; tests in `test/`.

Each milestone ends in something testable. Dependencies are roughly linear; signals (M7) and
uniqueness (M8) can proceed in parallel once persistence (M3) lands.

- **M0 — Scaffolding. ✅ DONE.** Mix project (Elixir 1.18 / OTP 27 via `.devcontainer` + Postgres),
  deps (`ecto_sql` 3.14, `postgrex` 0.22, `jason`), `GenDurable.Migration.up/down` (spec §9 DDL,
  Oban-style facade with version-in-table-comment), `GenDurable.Test.Repo`, test migration, smoke
  tests. `down → up` round-trip verified (enum + comment torn down cleanly). All green.
  **Deviation found & applied:** see "Spec deviation D1" below.
- **M1 — Core types (pure). ✅** `FSM` behaviour + `__using__`, `State` (embedded schema + jsonb
  load/dump codec), `Outcome`, `Context`, `Registry`, step dispatch. No DB. Tests:
  `outcome_test`, `state_test`, `registry_test`.
- **M2 — Persistence ops. ✅** All of `queries.ex` against a real DB, called directly. Tests:
  `queries_test` drives pick → each outcome, reaper, signals, uniqueness, batch insert.
- **M3 — Executor. ✅** `executor.ex` runs one picked row end-to-end incl. `try`/`handle/2`/
  `handle`-raises. Crash ≠ caught exception (no `handle/2` on a process crash).
- **M4 — Scheduler + pool + heartbeat. ✅** `scheduler.ex` — per-queue picker loop, concurrency cap,
  batched lease heartbeat. Test: Counter/MapCounter drive to `:done`.
- **M5 — Reaper. ✅** `reaper.ex`. Test: `Reborn` kills the worker mid-step → reaper resets →
  `attempt+1` → succeeds; `handle/2` not invoked.
- **M6 — `:replay` & backoff plumbing. ✅** `attempt` reset on `:next`, `+1` on `:replay`,
  `eligible_at` honored (see `queries_test` + `Crasher`).
- **M7 — await + signals. ✅** `:await` parks; `signal/4` delivers durably (insert-before-flip);
  re-executed step sees the signal and the engine deletes it in the outcome txn. Test: `Awaiter`.
- **M8 — Uniqueness + batch insert. ✅** `unique_guard` dedup, per-job scope, single-statement
  `insert_all`. Test: dedup vs existing rows and within the batch; scope-exit frees the key.
- **M9 — partition_key serialization. ✅** Session advisory lock on a checked-out connection held for
  the step; contended keys returned to `runnable`; auto-release on worker death. Test: `PartitionInc`
  — N instances sharing a key, no lost updates.
- **M10 — Hardening. ✅** Telemetry (`[:gen_durable, :step, :stop]`, `[:gen_durable, :reaper,
  :reaped]`), config surface (`GenDurable.Supervisor` opts), README, `--warnings-as-errors` clean.

### M11 — `schedule_childs` (spec §11) ✅ DONE
Fan-out + fan-in barrier as a first-class primitive. Since the schema was never deployed, the v2
columns were **folded into the v1 migration** (no incremental migration — per "никто не использовал"):
enum value `awaiting_children`, columns `parent_id` / `children_pending`, index `gen_durable_parent`.
- Outcome `{:schedule_childs, next_step, children, state}` (`Outcome.validate`); child spec is
  `{FsmModule, insert_opts}` or a bare module.
- `Queries.complete_schedule_childs` — batch-insert children (`parent_id` stamped) + park in one txn,
  `children_pending` = rows actually inserted, zero ⇒ straight to `next_step` runnable.
- Child→parent join decrement (`notify_parent`) appended to `complete_done`/`complete_stop` (no-op
  when `parent_id` is null); the decrement that hits zero releases the barrier.
- `ctx.childs` loaded on every run (one indexed SELECT; optimizable like the always-load-signals).
- Tests: all-children join, a `failed` child still releasing its slot, zero-children → immediate
  `next_step`. 36 tests green.

### Fixed (F1–F3)
- **F1 / D2 — resolved.** Spec §10 `:done` no longer lists `state = $state`; it matches the engine
  (writes `result` only, leaves `state`).
- **F1 / signals (option b) — resolved.** Name-scoped delete + `awaits` kept on delivery + `:await`
  pre-arrival guard (no lost wake-up). Non-matching signals survive (spec §5). Tests in `queries_test`.
- **F2 / test coverage — added.** Priority ordering, per-queue routing, `schedule_in` eligibility,
  heartbeat keeps a long lease (no spurious reap), cross-key partition parallelism. (`engine_test`)
- **F3 / scheduling sugar — added.** `insert/2` accepts `:schedule_in` (ms from now) / `:schedule_at`
  (`DateTime`); precedence `:eligible_at` > `:schedule_at` > `:schedule_in`.

### F5 — Feeder / backpressure + tunable aggressiveness ✅ DONE
The per-queue `Scheduler` is now a feeder: it claims work into a small in-memory buffer and drains it
into `concurrency` Tasks, with four knobs (`prefetch`, `min_demand`, `poll_interval` /
`max_poll_interval`) exposed via `GenDurable.Supervisor` opts. Defaults (`prefetch: 0`, `min_demand:
1`, `max_poll_interval: 5_000`) reproduce the prior behaviour plus idle backoff. **Buffered rows are
heartbeated** (`heartbeat` set = `buffer ++ in_flight`), so over-fetch never risks a spurious reap —
buffer depth is decoupled from `lease_ttl`. Test: `prefetch: 5` + `concurrency: 1` holds the tail
buffered past a 300ms lease and still completes with `attempt == 0`. 43 tests green.
- Knob trade-offs (cross-node fairness, priority freshness, crash blip) are documented in the module
  doc; defaults are conservative (fair on a cluster), aggression is opt-in per deployment.
- **Follow-up (deferred):** per-queue (vs engine-wide) knobs; graceful drain that resets the buffer
  on shutdown instead of waiting out the lease.

### F6 — Picker-side partition_key dedup ✅ DONE
The picker (`Queries.pick`) no longer claims work it can't run, killing the claim→`try_lock`→
`reset_to_runnable` churn that `prefetch` amplifies on hot keys. Two guards in one statement:
- **`NOT EXISTS`** — exclude a runnable row whose `partition_key` is already `executing` (so a sibling
  isn't claimed only to bounce off the advisory lock). `partition_key IS NULL` short-circuits the
  guard, so non-partitioned work pays nothing. Backed by a new partial index
  `gen_durable_partition_active (partition_key) WHERE status='executing'` (folded into v1).
- **`DISTINCT ON (coalesce(partition_key, id::text))`** — at most one row per key per batch (the most
  urgent); NULL keys fall back to `id` so each is its own group and is never collapsed.
The advisory lock stays the correctness guard (cross-node / unlock-gap races still possible); dedup is
the optimization that makes contention rare. New telemetry `[:gen_durable, :partition, :contended]`
fires on the residual bounce. Deterministic picker tests in `queries_test`; 46 tests green.
### F7 — Picker performance: queue equality + bounded dedup window ✅ DONE
Two fixes, measured in `PERFORMANCE.md` (real EXPLAIN on 1M rows, Postgres 17):
- **`queue = $1` (equality), never `ANY`.** Each scheduler owns one queue, so the picker filters by a
  single value. This lets the `gen_durable_pick (queue, priority, eligible_at)` index supply rows
  already ordered, so the `LIMIT` stops after ~`batch` rows. With `ANY`, the planner cannot trust the
  index order and scans + top-N sorts the *entire* runnable set: measured **613 ms → 0.7 ms (~850×)**
  at 295k runnable. `Queries.pick/5` now takes a single queue string; `Scheduler` passes `opts.queue`.
- **Bounded dedup window.** The dedup `scan` is `LIMIT $2` (the batch) on the index-ordered inner
  scan, so DISTINCT ON sorts ≤ `batch` rows, never the backlog. The earlier unbounded `candidates`
  scan is gone. A same-key cluster filling the window underfills the batch; completion-driven refill
  closes it next pick. Residual limit: a hot key with a huge runnable backlog still makes the scan
  skip past its excluded siblings (one index probe each) — picker sharding by key hash is the
  deferred fix (noted in PERFORMANCE.md §6).
All hot-path statements are PK/partial-index driven and sub-ms; see `PERFORMANCE.md` for plans, the
round-trip throughput model, and the F4 round-trip-reduction backlog. 46 tests green.

### Open follow-ups (post-v1, not blocking)
- **F4 — Always-load `signals` + `childs`:** every step does two `target/parent` SELECTs. Cheap, but
  a gate-on-FSM-flag (`use GenDurable.FSM, awaits: true, childs: true`) is a clean optimization.
- partition_key busy-spin on a hot key (picker-sharding, §6) — v2.
- Telemetry breadth, graceful drain — v2.

---

## 7. Testing strategy

The guarantee is concurrency- and crash-shaped, so tests must be too. `Ecto.Adapters.SQL.Sandbox`
fights advisory locks and multi-connection flows — run engine tests against a **real** DB,
`async: false`, with explicit cleanup.

- **Outcome correctness:** for each §3 outcome, assert the resulting row matches the §10 `UPDATE`
  (status, `attempt`, `eligible_at`, lease cleared, signals deleted).
- **At-least-once on crash:** kill the step Task mid-flight; assert reap → re-run, and that an
  effect guarded by idempotency runs at-least-once (and an unguarded one may run twice — documented).
- **await/signal:** loss-free delivery (signal always wakes), duplicate-tolerant (dedup_key), and the
  "insert before flip" ordering so the woken step sees its signal.
- **Uniqueness:** concurrent `insert_all` from multiple connections dedups; leaving an occupied
  status frees the key for re-insertion; `unique_key IS NULL` never conflicts.
- **partition_key:** spawn many machines sharing a key; assert steps never overlap and preserve order;
  assert cross-key parallelism; assert lock auto-release on simulated connection drop.
- **Property test (optional):** random interleavings of pick/heartbeat/reap/outcome preserve "state
  committed before proceed" and never lose a signal.

---

## 8. Decisions (all settled)

1. **DB:** `ecto_sql` + raw hot-path SQL.
2. **Latency:** polling only; `LISTEN/NOTIFY` banned permanently — do not add it.
3. **State:** typed struct per FSM (Ecto embedded schema), jsonb ↔ struct; plain-map state is
   unsupported. `result` stays a plain string-keyed map.
4. **Timings (Balanced):** lease 60s · heartbeat 20s · poll 1s · reaper 30s, all config-overridable.
5. **Registry:** explicit `{name, version} => module` config; old versions coexist as registered modules.
6. **Signals:** name-scoped consumption (spec §5, option b) — a progressing outcome deletes inbox
   signals whose `name = awaits` and clears `awaits`; non-matching signals survive; delivery keeps
   `awaits`; `:await` guards a pre-arrived signal against lost wake-up.

## 9. Risks / watch-items

- **Connection pressure from partitioned steps** (§0): each in-flight partitioned step holds a
  connection for its whole duration. Pool sizing must account for it.
- **ON CONFLICT inference on the generated column** must match the partial index predicate exactly.
- **Heartbeat vs lease race:** if heartbeat starves (slow DB), a live step can be reaped and run
  twice. The TTL margin and the at-least-once contract make this safe, but tune the margin.
- **`handle/2` re-entrancy:** a caught exception calls `handle/2`, which may `:replay`; ensure the
  re-run path is identical to a fresh run (no leftover lease/lock state).
