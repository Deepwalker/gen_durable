# Changelog

All notable changes to `gen_durable` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); this project is pre-1.0 and makes
**no backward-compatibility guarantees**. Now that there is a live install, schema changes
ship as versioned migration increments — `GenDurable.Migration.up/1` applies only the ones an
install is missing (before the first deployment they were edited into v1 in place).

## 0.2.11

### Changed
- **Picker: gate membership is a stored column, not a per-row string parse (hot path).** The
  claim used to decide "is this concurrency_key a configured gate?" per candidate row with
  `split_part(concurrency_key, ':', 1)` (a substring allocation) tested via a correlated
  `EXISTS` **and** a `LEFT JOIN` to `gen_durable_bucket_configs`, plus a synthetic dedup
  partition string `coalesce('k:' || concurrency_key, 'i:' || id::text)`. That is now a STORED
  generated column `concurrency_name` (`split_part(...)`, materialized **once per write** by
  Postgres); the claim tests `concurrency_name = ANY($5)` against the configured-gate array
  threaded from the caller (`config.concurrency_limit_names` — the `conc`-only set the executor
  already trusts) and partitions by the **raw** `concurrency_key`. The pick reads **no config
  table** and does **no per-row `split_part`**.

  **Schema (`change(3)`, migration required).** `ADD COLUMN concurrency_name text GENERATED
  ALWAYS AS (split_part(concurrency_key, ':', 1)) STORED`. This rewrites the table once under
  `ACCESS EXCLUSIVE` (~125 ms / 50k rows measured; scales with row count — a maintenance-window
  migration on a large install). `GenDurable.Migration.up/1` applies it as the v2→v3 increment.

  **Tradeoff / why.** Round-trips and statement counts are unchanged (claim is still one
  statement; `test/perf_test.exs` holds at 1 / 3). The per-pick win is modest (~12 %, 2.25 →
  1.98 ms at batch 200) because `LIMIT` caps the parsed rows and the `UPDATE` dominates a single
  pick — the real saving is **~1.5–2 µs of CPU per candidate row**, which scales with
  `batch × pick-frequency` (a visible slice of the picking floor at high throughput). Also
  removes the per-batch join to `gen_durable_bucket_configs` (cleaner plan) and **supersedes**
  the item-18 `'k:'/'i:'` partition-prefix collision fix (`id` no longer participates in the
  partition, so a numeric key can't collide with an id by construction). `Queries.pick/6 →
  pick/7` (new trailing `gate_names` arg, defaulting to `[]`). See `PERFORMANCE.md` §2.7 and
  `ISSUES.md` #29.

## 0.2.10

### Added
- **Inline execution (run-ahead): `use GenDurable.FSM, inline_execution: true`.** A `:next`
  outcome can now run the next step **in the same worker task**, holding the same claim,
  instead of committing the row back to `runnable` and paying a full re-pick (claim scan +
  admit + enrich + task respawn) between every step. It's opt-in per FSM, and a single
  `:next` may override the default with `inline_execution: true|false` in its opts (mark a
  boundary step to requeue even in an inline FSM, or force one transition inline).

  This leans on the out-of-band limiter (0.2.9): before running the next step the executor
  secures its tokens as separate statements — a rate token via `Limiter.admit`, a slot for a
  **configured** new `concurrency_key` via `Limiter.admit`, and an **unconfigured** new key
  in-band through the K=1 arbiter (the guarded `continue_next` commit itself). If any is
  denied, the row is requeued and the picker admits it exactly as before — inline is a fast
  path, never a semantic change. `concurrency_key: :keep` (the default) holds the same slot
  across steps with no admission round-trip.

  **Hot-path cost.** A chained step is one guarded `continue_next` (keeps the row
  `executing`, commits the next state — durability unchanged) plus the two batched enrich
  loads (a fresh inbox/children snapshot, identical to a re-pick), and one `Limiter.admit`
  only when the step is rate-limited or adopts a configured key. It skips the pick's claim
  scan, the requeue write, and the task respawn. Default-off, so existing pick/outcome
  statement counts are unchanged; see `test/perf_test.exs` and `PERFORMANCE.md`.

  **Ordering (correctness).** The guarded `continue_next` runs **before** `Limiter.admit`:
  the guard re-proves ownership, so an orphaned chained task (lease expired, row reclaimed)
  commits nothing (`:stale`) and never reaches the unguarded admit — which stamps
  `concurrency_shard` by id and would otherwise corrupt a new claimant's row and break K=1.
  A changed slot is reported to the scheduler (`{:slot_swap, …}`) so its heartbeat renews
  the current slot (a lease-native limiter would prune one it stops hearing about).
  See `ISSUES.md` for the tradeoffs (priority staleness across a chain; the rate-token leak
  on a mid-chain `:stale`).

## 0.2.9

### Changed
- **Admission for configured limits moved out of the pick, behind a `GenDurable.Limiter`
  behaviour.** The fused claim (the `c_*`/`r_*` CTEs) welded rate/concurrency admission and
  its bucket `FOR UPDATE` locks into the single pick statement; under fan-out that claim
  locks hard, and sharding the buckets (0.2.7) smears the contention without curing it. The
  pick now (1) claims candidate rows into `executing` in one lock-light statement, then
  (2) asks the limiter to `admit/2` them (a separate, short statement that holds only the
  bucket locks), then (3) keeps the admitted and releases the denied. Concurrency credit
  is out-of-band too: the outcome's `credit_gate` rider is gone — the executor calls
  `Limiter.credit/2` after a committed outcome (a stale outcome credits nothing, exactly as
  the rider did). The K=1 dedup of **unconfigured** concurrency keys stays in-band (the
  `gen_durable_concurrency_active` arbiter) — it is intrinsic to the durable row.
- **Trade-off:** claim and admit are no longer one statement, so a saturated gate
  over-claims up to the batch and the limiter releases the excess (the scheduler's poke
  debounce, below, bounds the churn), and a crash between the two windows leaks a slot in the
  safe direction (under-admission), healed by the reconciler — the same self-heal that already
  backed crash recovery. The observable limits are unchanged. A failure *after* the claim
  commits (admit's retries spent, or a transient DB error in admit/release/enrich) now
  releases the whole claimed batch before reraising, so it can't strand rows as `executing`
  until the reaper — mirroring the old fused pick's atomic rollback.
- **Poke debounce (picker anti-thrash).** Because an *empty* pick on a saturated limit now
  WRITES (claim + release) instead of only reading, the local poke leg — which has no
  sender-side dedup, one poke per insert/signal/join — could hammer the picker. An idle
  scheduler now coalesces every poke in a short window into a single fill (one armed timer),
  bounding poke-driven picks to ≤1 per window regardless of the poke rate. Busy schedulers
  still drop pokes (they self-serve via completion-refill); the ≤10 ms added idle→work
  latency is negligible.

### Added
- **A pluggable limiter backend, selected by the `:limiter` engine option.**
  - `:postgres` (default) — `GenDurable.Limiter.Postgres`: the same sharded
    `gen_durable_buckets` admission math, now run as standalone statements over the claimed
    batch instead of fused into the pick. No new dependency.
  - `{:redis, url_or_opts}` — `GenDurable.Limiter.Redis`: concurrency is a **lease-scored
    ZSET** semaphore per key (members are holders, scores are lease expiries) that
    **self-heals on lease expiry** — a crashed holder is pruned on the next admit, no Postgres
    reconcile; the heartbeat renews live holders so a long step is never pruned. Rate is a
    Lua token bucket. `admit`/`renew` are single atomic `EVAL`s; caps live in `persistent_term`
    (seeded at startup), so admission needs no config round-trip. Requires the optional `:redix`
    dep. **Single-node Redis** (one `admit` touches every batched key; Cluster would span hash
    slots). No schema change — the K=1 arbiter and `concurrency_shard` stay a Postgres-row
    concern regardless of backend.
- Migration-free: 0.2.9 adds **no** DDL; both backends reuse the existing tables / no tables.

## 0.2.8

### Changed
- **Poke no longer fans out into a DB thundering herd.** Enabling a
  `:cluster`/`{:redis, _}` poke transport made every insert wake every node,
  each firing a pick (one winning, the rest wasted) — the pick rate became
  `insert_rate × nodes`. Two changes fix it: (1) a poke now only wakes an
  **idle** scheduler — a queue with work in flight drops it and rediscovers new
  work via completion-refill (the poll stays the floor), so busy nodes stop
  self-poking; (2) the Redis transport gates each broadcast behind a per-queue
  **distributed dedup lock** (`SET NX PX`, one `EVAL`), collapsing a burst/stream
  of inserts into at most one broadcast per ~100 ms window across the whole
  fleet — the fan-out no longer scales with the insert rate. Both keep
  best-effort semantics (a lost poke costs one poll interval, never correctness).

## 0.2.7

### Changed
- **Rate limits and concurrency gates are now one sharded mechanism.** The four limiter tables
  collapse into `gen_durable_bucket_configs` and `gen_durable_buckets`, both keyed by
  `(kind, …)` so a rate limit and a gate may share a name without colliding. Rate limits gain a
  `shards:` option (gates already had one); each pick locks a key's shards with
  `FOR UPDATE OF b SKIP LOCKED`, so concurrent (cross-node) pickers grab **disjoint** shards
  and admit in parallel instead of serializing — or blocking a whole pick — on one bucket row
  held across the claim. A lone picker still grabs every shard (full `burst`/`cap`), `SKIP
  LOCKED` makes bucket deadlock structurally impossible, and rate's consumed weight is debited
  proportionally across the grabbed shards (rate rows carry no shard — it has no credit-back).
  Default `shards` is 1 — behaviour-preserving; opt into parallelism per limit, sizing
  `shards ≥ contending nodes`.

### Migration
- **First versioned schema increment (`@latest_version` → 2).** `change(1)` is frozen;
  `change(2)` drops the four old limiter tables and creates the two unified ones. They are
  ephemeral (configs re-seed from options at boot, buckets mint on demand), so it is a pure
  drop-and-recreate with nothing to preserve — an existing install runs only `change(2)`, a
  fresh one runs both in one `up()`. Re-run your existing one-line `GenDurable.Migration.up()`
  migration.

## 0.2.6

### Added
- **`GenDurable.await/3` — the sync-over-async bridge.** Insert, poke does the discovery,
  `await(id, timeout)` holds the caller until the instance settles: `{:done, result}`,
  `{:failed, error}`, `{:awaiting, snap}` (parked; `until: :terminal` waits through it),
  `{:busy, snap}` on deadline (the work continues — hand the client the id as a retry
  token; calling `await` again with it is the whole retry protocol), `:not_found`. The
  answer is always read from the row; a same-node result is pushed to waiters by the
  executor within milliseconds, cross-node results land within the batched watcher's
  tick (`await: [tick: 25]` engine option — one probe query per tick covers every waiter
  on the node; idle costs nothing). Works with a bare repo too (plain poll loop).

- **Poke-on-insert: zero discovery latency, three transports.** `insert`/`insert_all`
  nudge the schedulers of the queues that just received due rows, so work is picked
  immediately instead of waiting out `poll_interval`. Configured per instance with
  `poke:` — `:local` (default, caller's node only), `:cluster` (every node over Erlang
  distribution; membership via an OTP `:pg` scope, so only nodes actually running the
  queue are reached), or `{:redis, url_or_opts}` (Redis Pub/Sub for clusters without
  distribution; requires the new **optional** `:redix` dependency; the caller's node is
  poked directly and publishes are origin-tagged so a node never double-pokes itself).
  Pokes also announce every engine-driven wake: a signal flipping a parked row (the
  wake's queue rides back out of the delivery statement), a fan-out's children in their
  own queues, and a parent whose join the last child completed — a cross-queue fan-out
  round trip needs no poll at any hop. Best-effort in every mode — a lost poke costs one
  poll interval, never correctness; the poll remains the floor for retry backoffs and
  the reaper's wakes (await timeouts, crash reclaims). Future-scheduled rows wake
  nobody. Poke bursts coalesce into one pick, go through the
  normal demand gates, and never stretch the idle backoff on a miss. See
  `GenDurable.Poke`.

### Changed
- **Instance-row claims lock with `FOR NO KEY UPDATE`** (pick, heartbeat, reaper, await
  sweeps, startup reclaim, shutdown release, the schedule_childs guard). Strong enough
  for the claim/outcome mutual exclusion, but compatible with the `FOR KEY SHARE` that
  signal-insert FK checks take on the target row — an in-flight signal insert no longer
  makes the pick skip its target row. (Signal *delivery* still queues behind the claim
  at its wake `UPDATE`, as any write to the row must.) Safe from
  lock-upgrade deadlocks by schema (the only *full* unique index is the immutable PK;
  the correlation/concurrency uniques are partial, which Postgres excludes from lock
  strengthening — revisit if a full unique index over a mutable column is ever added).
  A/B-benched: throughput identical within noise. Bucket locks stay `FOR UPDATE` (no FK
  traffic exists there).

## 0.2.5

### Added
- **Per-node runtime topology.** `reaper: false` / `gc: false` skip those processes on a
  node; `queues: []` (as before) runs no schedulers. Together they express worker-only,
  web-only (insert/signal only), and maintenance-only nodes — the operations guide gained
  a Topologies section with the three configs and the cluster invariant (at least one
  node must run each sweeper; duplicates are safe, sweeps claim via `SKIP LOCKED`).
  Disabled components log one line at boot. Invalid component options fail the boot with
  an `ArgumentError` before any side effect.

### Changed
- **GC/reaper options consolidated** (breaking, pre-1.0): `reap_interval` →
  `reaper: [interval:]`; `gc_interval`/`gc_retention`/`gc_batch` →
  `gc: [interval:, retention:, batch:]`; `gc_interval: nil` → `gc: false`. Partial
  keywords merge over defaults.

### Documentation
- New guide: **Database internals** — the schema map (every table and index and what it
  serves), who reads and writes what per statement family, the locking discipline, and
  constraints as the cross-node protocol.

## 0.2.4

### Fixed
- **Rate-limit buckets moved to the zero-lag mint** (the gates' 0.2.3 design; the rate
  limiter was the last ensure/heal user). The pick admits a cold rate key against its
  virtual full bucket (`burst` by definition) and inserts the row already debited — a
  key whose idle bucket the GC swept no longer pays a one-pick lag (and no longer emits
  a false `:throttled` with granted=0), and drain over a cold bucket completes in one
  call. The ensure riders on `insert`/`insert_all`/`:next`/`schedule_childs` and the
  pick's rate `heal` CTE are deleted; inserts are rider-free single statements. Two
  picks racing the first-ever grant of a key resolve on the bucket's primary key with a
  bounded retry — observable via the new `[:gen_durable, :rate_limit, :contended]`
  telemetry event. Measured: every rate scenario within ±3 % of baseline; conservation
  under 8 racing workers exact (see PERFORMANCE §2b).

### Documentation
- The `#23`-class EPQ suspicion on the rate writeback was audited under adversarial
  load and no violation reproduced (ISSUES #26 records the protocol and the tripwires).
- New honest-list pathology (PERFORMANCE §6 #4, surfaced by that audit): rows denied
  **after** the candidate window — rate-throttled rows and rows of a saturated
  configured gate alike — keep occupying the pick's `LIMIT` slots, so a saturated key
  with a backlog deeper than batch × concurrent picks starves same-priority work behind
  it. Both guides now say: give heavily capped flows their own queue.

## 0.2.3

### Fixed
- **Cold gates admit with zero lag** (external report: a mid-flight `concurrency_key`
  switch onto a bucketless gate returned an empty pick, breaking
  `GenDurable.Testing.drain/1`'s quiescence contract and stretching the production
  scheduler's idle backoff). The mint is now part of admission: the pick computes a cold
  gate's virtual full shards from the config, admits against them, and materializes the
  buckets pre-debited in the same statement; racing double-mints merge via ON CONFLICT
  with the CHECK + retry backstop. The gate ensure riders on the insert paths and the
  pick's heal CTE are deleted — "no buckets" is indistinguishable from "buckets full".
  The GC reconciler also backfills missing in-range shards, so a `shards:` increase no
  longer silently shrinks a live gate's capacity.

## 0.2.2

### Added
- **Concurrency gates — `concurrency_key` grew a limit.** A key is now a semaphore of
  size K: `concurrency_limits: [stripe: [limit: 100, shards: 1]]` caps in-flight
  executions per key (cluster-wide, `{:stripe, tenant}` partitioning as in rate limits);
  an unconfigured key keeps the default K = 1 (mutual exclusion, the previous behavior,
  still enforced by the unique arbiter at zero cost). Gated keys are admitted against
  sharded slot counters fenced by a database CHECK (over-admission is uncommittable),
  debited in one batched pass per pick and credited back addressed by every outcome;
  crash leaks err strict-side and the GC reconciler repairs counters from the
  executing-rows truth. `shards:` multiplies the per-key completion ceiling (completions
  serialize per shard row; size `shards ≥ limit × commit_latency / step_duration`).
  `:next` can release or switch the key per transition (`concurrency_key: nil | value`;
  absent keeps it). Telemetry: `[:gen_durable, :concurrency, :throttled]`, and
  `[:gc, :swept]` gained a `gates` measurement. **Careful**: a config name captures every
  key with that prefix — see the concurrency guide's warning. Schema: `concurrency_shard`
  column + two gate tables (v1 DDL edited in place — re-create the schema).

### Changed
- **`concurrency_key` serialization moved into the database**: the
  `gen_durable_concurrency_active` partial index is now UNIQUE, so a second executing row
  per key is uncommittable — the claim itself is the lock, spanning exactly the step
  window and released by any outcome (or the reaper on a crash). The whole advisory-lock
  layer is gone: no session locks, no `Repo.checkout`, and keyed steps **no longer pin a
  pool connection for the step duration** (the former known-limit #2). A cross-node claim
  race now resolves as a unique violation and a bounded pick retry;
  `[:gen_durable, :concurrency, :contended]` metadata changed to `%{queue}`.
  Schema: the index became UNIQUE (v1 DDL edited in place — re-create the schema).

## 0.2.1

### Added
- **`GenDurable.Testing` — inline test mode**: `drain/1` synchronously runs everything
  runnable in the calling process through the production pick/executor path (no engine,
  sandbox-compatible), collapsing scheduled/backoff delays by default and failing fast on
  runaway FSMs (`max_steps`); assertion helpers (`assert_status`, `assert_awaiting`,
  `assert_done`, `assert_failed`, `durable/1`, `fire_timeouts/0`) bound to the repo via
  `use GenDurable.Testing, repo: MyRepo`. See the Testing guide.
- **Await timeouts**: `{:await, names, next_step, state, timeout: ms}` wakes the instance
  after the deadline even without a signal — a wake, not a failure (`attempt` untouched);
  a fresh await distinguishes by empty `ctx.awaited`, the accumulate pattern proceeds with
  its partial pack. Resolution is bounded by `:reap_interval`. New `await_deadline` column
  + partial index (v1 DDL edited in place, per the pre-1.0 stance — re-create the schema).
  Telemetry: `[:gen_durable, :await, :timeout]`.

## 0.2.0

A design-review hardening release: two correctness races closed, outcome commits made
ownership-safe, the engine made multi-instance, and the per-step round-trip count cut to ~1.
Findings and their resolutions are tracked in `ISSUES.md`.

### Fixed
- **Lost-wakeup race** between parking (`:await`) and signal delivery under READ COMMITTED:
  a signal racing the park could strand a parked instance with its wake-up already in the
  inbox. Delivery now always takes the instance row lock (the flip condition moved into the
  statement, not its WHERE), and parking rechecks the inbox under the same lock — `:await`
  is now deliberately a two-statement transaction.
- **Cross-pick deadlock on rate buckets**: bucket rows are locked in key order; every
  multi-row bucket writer (ensure CTEs, config upsert) got deterministic ordering too.
- **Stale-worker overwrite**: every outcome now commits only while the worker still owns
  the claim (`locked_by` + `executing` guard). An orphaned task (its scheduler crashed;
  the row was reclaimed) gets its late commit **dropped** — observable as
  `[:gen_durable, :outcome, :stale]` — instead of rewinding step/state, silencing the new
  claimant's heartbeat, or double-firing terminal side effects (inbox purge, parent join
  decrement).
- **Double-encoded jsonb**: `state`/`result`/signal payloads were stored as jsonb *scalar
  strings* (invisible to `->>` and jsonb indexes). All JSON parameters are now parsed
  server-side (`::text::jsonb`) and stored as objects; rows in the old format still decode.
- **Batch-size ceiling**: `insert_all` and `schedule_childs` children ride in as `unnest`
  parallel arrays — the old per-row placeholders hit the wire protocol's 65535-parameter
  cap at ~5400 rows.
- **Arbiter-order deadlock on concurrent batch inserts**: batches insert in
  `correlation_key` order (server-side), so two nodes racing the same new keys can no
  longer deadlock on the dedup index. Ids are consequently assigned in key order, not
  entry order.
- An uncaught `throw` in a step routes to `handle/2` as `{:throw, value}` instead of
  crashing the task and waiting out a full lease.
- **A second adversarial review pass** (findings 13–21 in `ISSUES.md`): the pick
  self-heals a swept rate bucket (a slept-past-refill row can no longer stall forever and
  starve the queue); re-awaiting with already-presented signals parks cleanly instead of
  spinning (the accumulate-a-pack pattern no longer busy-loops); unserializable outcomes
  (an unencodable `:done` result, a bad child spec) route to `handle/2` instead of looping
  through the reaper forever; startup reclaim requires a stale lease, so claim-prefix
  collisions (containers with identical hostnames, BEAM as pid 1) can never release a live
  VM's claims; all multi-row maintenance statements claim rows via ordered
  `FOR UPDATE SKIP LOCKED` (no maintenance-vs-maintenance deadlocks); a numeric
  concurrency_key can no longer collide with a row id in the dedup window; `min_demand` is
  clamped to the claim ceiling; `:await` resets `attempt` like `:next`.

### Changed (behavior)
- `GenDurable.signal/4` returns `{:error, :no_target}` for a terminal or nonexistent
  target — for integer ids too (previously `:ok` with a durably stored orphan signal, or
  an FK violation for a missing id).
- The default supervisor registration is now `GenDurable` (was `GenDurable.Supervisor`);
  the `child_spec` id follows.
- Worker ids are now `<instance>:<queue>@<vm>-<uniq>` (opaque, stored in `locked_by`).

### Added
- **Multi-instance engines**: give each a `:name` (default `GenDurable`) and route API
  calls with `name:` — config, task supervisor, and FSM registry are per-instance; a
  duplicate name fails with `:already_started`.
- **Startup reclaim**: a starting scheduler releases claims left by a dead predecessor
  (same instance+queue+VM) instead of letting them wait out the lease
  (`[:gen_durable, :scheduler, :reclaimed]`).
- **Rate-bucket GC**: the GC sweep prunes buckets idle past their refill horizon and
  buckets whose named limit was removed (`[:gen_durable, :gc, :swept]` gained a `buckets`
  measurement) — partitioned keys no longer grow the bucket table without bound.

### Performance
- **~1 round-trip per step**: the pick batch-loads signal inboxes and children for its
  whole claim set (3 statements per batch), removing the two per-step loads; the inbox
  snapshot is taken at pick time (consumption stays exact — it deletes the ids the step
  actually saw).
- **`deliver_signal` is one statement** (was a transaction of up to 5 round trips).
- **Prepared-statement caching** for every query (`cache_statement:`): parse + plan once
  per connection. Hosts behind a transaction-pooling proxy set `prepare: :unnamed`.
- Advisory locks hash concurrency keys with `hashtextextended` (64-bit).

## 0.1.8

### Added
- **Rate limiting — token bucket, per step (spec §12).** A step opts into a named limit by
  returning `{:next, step, state, rate_limit: :stripe}` (or `{:stripe, partition}` for a bucket
  per tenant). Configured at start: `rate_limits: [stripe: [allowed: 100, period: {1, :minute}]]`
  (`burst` defaults to `allowed`). Enforced in the picker (one statement) by a per-bucket token
  counter locked with `FOR UPDATE` — correct across nodes, and measured to cost nothing on the
  common path (NULL `rate_limit` short-circuits; the rate CTEs are `never executed`).
- **Weighted steps.** `{:next, step, state, rate_limit: :stripe, weight: 50}` — a step may consume
  more than one budget unit. Grants take the urgency prefix whose cumulative weight fits (strict
  order, free head-of-line reservation). `weight ≤ burst` is the caller's responsibility and is
  **not** validated — a too-fat step freezes its bucket; split the step instead.
- New tables `gen_durable_rate_configs` / `gen_durable_rate_buckets`; new `gen_durable.rate_limit`
  and `gen_durable.weight` columns. `:next` now normalizes to a 4-tuple carrying a per-transition
  opts map (`rate_limit`, `weight`). `insert`, `insert_all`, and `schedule_childs` children all
  carry the columns and ensure their buckets.
- Telemetry: `[:gen_durable, :rate_limit, :throttled]` (a bucket granted fewer than wanted) and
  `[:gen_durable, :rate_limit, :unknown]` (a step named an unconfigured rate-limit).

### Deliberately not added (settled decisions)
- **Weighted-step poison guard** (`weight ≤ burst`): a too-fat step freezes its bucket — the caller's
  responsibility (split the step), consistent with the engine's "you own correctness" stance.
- **Sliding/fixed-window rate algorithms**: token bucket only (its `rate`+`burst` knobs cover the
  spectrum; sliding-log breaks single-row locking).
- **Boot-time validation / `on_unknown` policy**: an unknown rate-limit key stalls the row and emits
  `[:gen_durable, :rate_limit, :unknown]` — that is the chosen v1 behaviour (no fail-fast at boot, no
  `:run`/`:stop`/`:defer` knob).

## 0.1.7

### Changed
- Renamed `partition_key` → `concurrency_key` (column, insert option, SQL, the
  `gen_durable_concurrency_active` index, and the `[:gen_durable, :concurrency, :contended]`
  telemetry event). Pure rename; the SQL `PARTITION BY` window keyword is untouched.

## 0.1.6

### Changed
- Dropped the `:unique` policy enum (`:live`/`:global`). `correlation_scope` (a `durable_status[]`)
  is now passed directly, defaulting to the non-terminal statuses. This also removes the surprising
  built-in `:global` behaviour where a finished instance silently swallowed signals.

## 0.1.5

### Changed
- Renamed the same-step outcome `:replay` → `:retry` (it redoes the step with `attempt += 1` after a
  delay; the old name read like event-sourcing replay).
- Merged addressing and uniqueness into one `correlation_key` (Temporal/DBOS workflow-id model): the
  business key you `signal/4` by is the same key the engine deduplicates on. Replaces the separate
  `external_id` (addressing) and `unique_key`/`unique_scope` (dedup). One partial unique index does
  both jobs.
- Dropped the misleading "on top of GenServer" framing: an FSM is a row, not a process — there is no
  GenServer per instance; the runtime backbone (scheduler/reaper/GC) is a small set of GenServers.

## 0.1.4

### Added
- Built-in GC of terminal (`done`/`failed`) rows: `GenDurable.GC`, configurable `:gc_interval` /
  `:gc_retention` (default 1 day) / `:gc_batch`; `[:gen_durable, :gc, :swept]` telemetry. The delete
  is O(batch) (select ids, then `DELETE … WHERE id = ANY`), not O(table).

## 0.1.3

### Changed
- `await` waits on a **set** of signal names; the woken step sees the matched subset as `ctx.awaited`
  (full inbox in `ctx.all`). Consumption is by received id on progress (latecomers survive; packs can
  accumulate via re-await); a terminal outcome clears the whole inbox.

## 0.1.2

### Added
- Job form: define `perform/1`/`perform/2` instead of `step/2` for a one-shot durable job with
  built-in retry/backoff. Folded into `GenDurable.FSM`.

## 0.1.1

### Added
- Nested `State` embedded-schema adopted by convention (no `state:` option needed).

## 0.1.0

### Added
- Initial durable FSM engine: Postgres-backed, state committed before each step proceeds
  (at-least-once, whole-step re-execution). Steps and outcomes (`:next`/`:retry`/`:await`/`:done`/
  `:stop`), `schedule_childs` fan-out + fan-in barrier (§11), durable signals/await (§5), queues with
  concurrency, priority, scheduling sugar, lease + reaper crash recovery, `concurrency_key`
  serialization, uniqueness, single-round-trip outcomes, feeder backpressure, graceful drain, broad
  telemetry, dynamic FSM resolution, library-owned migration.
