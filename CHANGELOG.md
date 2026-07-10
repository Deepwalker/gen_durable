# Changelog

All notable changes to `gen_durable` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); this project is pre-1.0 and makes
**no backward-compatibility guarantees** — there is one schema version and migrations are
edited in place until the MVP settles.

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
