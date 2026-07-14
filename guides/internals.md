# Database internals

Everything the engine knows lives in Postgres: an FSM instance is a row, its inbox is rows,
the limiter counters are rows. The runtime processes (schedulers, reaper, GC) hold no state
worth preserving — kill any of them and the database still describes exactly where every
instance is. This page is the map: the tables, the indexes, who reads and writes what, and
the locking/constraint discipline that makes concurrent nodes safe. For the plans and
measured costs of these statements, see [PERFORMANCE](../PERFORMANCE.md); for the feature
semantics, the individual guides.

Two design rules explain most of what follows:

- **One statement per hot-path operation.** Claims, outcomes, and signal delivery are each a
  single data-modifying CTE chain — one round-trip, atomic without an explicit transaction.
  User step code runs *between* statements, outside any transaction.
- **Invalid states are uncommittable, not checked-for.** Where two nodes can race, the
  schema carries a constraint that makes the losing write impossible to commit; the loser
  retries against the winner's committed truth.

## The tables

### `gen_durable` — the instance table

One row per instance (or job — a job is a one-step instance). Column groups:

| Group | Columns | Notes |
|---|---|---|
| what runs | `fsm`, `fsm_version`, `step`, `state jsonb`, `attempt`, `result`, `last_error` | `state` is the durable FSM state, rewritten on every transition |
| lifecycle | `status` | enum `durable_status`: `runnable → executing → awaiting_signal / awaiting_children / done / failed`; the whole protocol is flips of this column |
| scheduling | `queue`, `priority`, `eligible_at` | the pick order is exactly `(queue, priority, eligible_at)` |
| claim | `locked_by`, `lease_expires_at` | who is executing it and until when the claim is trusted |
| admission | `concurrency_key`, `concurrency_shard`, `rate_limit`, `weight` | `concurrency_shard` is set at claim time for gated keys (the release credits that shard back); `rate_limit`/`weight` describe the *current* step |
| coordination | `awaits text[]`, `await_deadline`, `parent_id`, `children_pending` | signal parking and child fan-out join state |
| identity | `correlation_key`, `correlation_scope durable_status[]`, `correlation_guard` | `correlation_guard` is a **stored generated column**: equals the key while `status = any(scope)`, else NULL — computed by the database, never by application code |
| bookkeeping | `inserted_at`, `updated_at` | `updated_at` doubles as the termination instant for GC retention |

Ownership between statements is not a database lock: a step *owns* its row iff
`locked_by = $worker AND status = 'executing'`, and every outcome statement carries that
guard. A worker whose lease expired (row reclaimed, possibly re-claimed by someone else)
commits nothing — the guard matches zero rows and the late outcome is dropped.

### `signals` — the durable inbox

`(id, target_id → gen_durable ON DELETE CASCADE, name, payload jsonb, dedup_key, inserted_at)`
with `UNIQUE (target_id, dedup_key)` — redelivery with the same dedup key is a no-op at the
schema level. Rows are deleted by the outcome that consumed them (a rider CTE), and die with
their instance via the cascade.

### Limiter policy and counters

| Table | Shape | Role |
|---|---|---|
| `gen_durable_bucket_configs` | `(kind, name) PK, rate, capacity, shards` | rate **and** gate policy in one table (`kind` = `'rate'`/`'conc'`; `rate` is NULL for gates, `capacity` = burst or cap), upserted at boot from `rate_limits:`/`concurrency_limits:` |
| `gen_durable_buckets` | `(kind, key, shard) PK, capacity, available, last_refill`, `CHECK (0 ≤ available ≤ capacity)` | sharded counters for both limiters (`available` = tokens or free slots; `last_refill` NULL for gates); minted **pre-debited by the pick**, swept by GC/reconciler when idle. `kind` in the key lets a rate limit and a gate share a name/partition without colliding. The CHECK is the hard cap — over-admission and double-credit are uncommittable |

## The indexes

Each index exists for exactly one hot query; every partial predicate matches its query's
`WHERE` clause 1:1.

| Index | Definition | Serves |
|---|---|---|
| `gen_durable_pick` | `(queue, priority, eligible_at) WHERE status = 'runnable'` | the picker's candidate scan — equality on `queue` keeps the index pre-ordered so `LIMIT batch` stops after ~batch rows |
| `gen_durable_lease` | `(lease_expires_at) WHERE status = 'executing'` | the reaper's expired-lease sweep; also the executing set the picker's K = 1 guard probes |
| `gen_durable_await_deadline` | partial over parked rows with an armed deadline | the await-timeout sweep |
| `gen_durable_concurrency_active` | **UNIQUE** `(concurrency_key) WHERE executing AND key IS NOT NULL AND shard IS NULL` | the K = 1 arbiter: a second executing row per unconfigured key cannot commit. Gated claims set `concurrency_shard` and drop out of the predicate |
| `gen_durable_correlation` | **UNIQUE** `(correlation_guard) WHERE NOT NULL` | double duty: uniqueness among "occupied" statuses *and* the signal address lookup |
| `gen_durable_parent` | `(parent_id) WHERE NOT NULL` | the parent join when a child terminates; the GC's mid-join guard |
| `gen_durable_gc` | `(updated_at) WHERE status IN ('done','failed')` | the GC candidate scan, ordered by termination instant |
| `signals_target` | `(target_id, name)` | inbox loads and consumption deletes |

## Who reads and writes what

### Inserts — `insert`, `insert_all`, the children of `schedule_childs`

A plain `INSERT ... ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO
NOTHING RETURNING id` — dedup by business identity costs nothing when no key is given
(NULLs never conflict). Batch forms ship rows as 12 parallel arrays through `unnest`, so the
SQL text is static (statement-cacheable) and the parameter count is fixed for any batch
size. Rows are inserted `ORDER BY correlation_key`: two nodes creating the same new keys in
opposite orders would deadlock on the unique index's uncommitted entries; in one order, the
race is a clean conflict instead. Inserts touch **no other table** — limiter buckets are the
pick's business.

### The pick — claim, then out-of-band admission

Admission for **configured** limits lives behind the `GenDurable.Limiter` behaviour, not in
the claim. The pick is three steps (`Queries.pick/6`). Config lookups filter by `kind` — a
`concurrency_key` whose prefix equals a *rate*-limit name must not read as a gate:

1. **claim** (`@claim_sql`, one lock-light statement) — up to `batch` runnable rows via
   `gen_durable_pick`, locked in-scan with `FOR NO KEY UPDATE SKIP LOCKED`, flipped to
   `executing`. The K = 1 guard rides in the `WHERE` (an *unconfigured* keyed row with an
   executing sibling is filtered *before* the `LIMIT`), and `gen_durable_concurrency_active`
   is its correctness backstop. A configured gate keeps ALL its candidates — a provisional
   non-null `concurrency_shard` drops them out of that arbiter (their cap is the bucket). No
   bucket table is touched here.
2. **admit** (`Limiter.admit/2`) — the claimed batch, ordered `(priority, eligible_at)`, goes
   to the backend. `Limiter.Postgres` runs the same capacity math as the old fused pick — gate
   ranges over `grabbed ∪ cold` shards; cumulative rate weight over per-shard refilled
   availability (`LEAST(burst/shards, tokens + elapsed × rate/shards)`); debit only the
   finally-admitted (gate **and** rate); cold shards minted **already debited** (`c_mint`
   merges racing gate mints via `ON CONFLICT`, a racing rate mint is a bounded PK-violation
   retry) — as ONE statement holding only the bucket `FOR UPDATE OF b SKIP LOCKED` locks, not
   the row claim. It stamps the drawn shard onto the admitted rows and returns
   `%{admitted, denied}`.
3. **keep / release** — denied rows go back to `runnable` (`release_claims`); a saturated gate
   thus over-claims up to `batch` and releases the excess, the price of not holding the row
   locks across the admit round-trip (scheduler backoff bounds the churn). `:throttled`
   telemetry is derived from the denials.

An unlimited pick calls no backend at all (an empty admit short-circuits in Elixir): it is the
same claim plus two batched `SELECT`s enriching the whole claim set (pending signals, live
children) — three statements per batch, regardless of batch size. A limited pick adds the
admit round-trip (and a release when a limit bites).

### Outcomes — one guarded statement each

Every outcome is `UPDATE gen_durable SET ... WHERE id = $1 AND locked_by = $worker AND
status = 'executing'` as the guarded head of a CTE chain, with riders gated on it
(`WHERE EXISTS (SELECT 1 FROM committed)`), so a stale outcome commits *nothing at all*:

| Outcome | Row flip | Riders |
|---|---|---|
| `:next` | → `runnable`, new step/state, new `rate_limit`/`weight`, optional `concurrency_key` change, `concurrency_shard` cleared | `consumed` (DELETE handled signals) |
| `:retry` | → `runnable` with backoff, `attempt` kept incrementing, `awaits` kept | — |
| `:await` | → `awaiting_signal` + `awaits`/deadline | the one **multi-statement** op: park + recheck in a short transaction (the recheck closes the signal-raced-the-park window) |
| `:done` / `:stop` | → `done`/`failed` + `result`/`last_error` | `consumed`, and the parent join: decrement the parent's `children_pending`, waking it at zero |
| `schedule_childs` | → `awaiting_children` (or `runnable` when none inserted) | `consumed`, the children `INSERT` (same unnest/conflict shape as `insert_all`) |

Every outcome clears `concurrency_shard` (the row is leaving `executing`), but the gate slot
is **credited out-of-band**: the executor calls `Limiter.credit/2` after a *committed*
(non-stale) outcome, with the `{key, shard}` the job held. A stale outcome credits nothing
(the executor skips it, exactly as the old inline `credit` rider affected zero rows under a
failed guard); a crash between commit and credit leaks the slot in the safe direction
(under-admission), healed by the reconciler.

### Inline continuation — `continue_next`, the run-ahead commit

An `inline_execution:` FSM's `:next` does **not** leave `executing`. `continue_next` is the
same guarded head (`WHERE id = $1 AND locked_by = $worker AND status = 'executing'`) but
commits the next step/state while the row **stays `executing` under the same worker** and
extends the lease — so the executor runs the next step in the same task, skipping the requeue
→ re-pick round-trip. Durability is identical to a requeued `:next`: the next state is
committed before that step runs; a crash re-runs it via the reaper.

The concurrency handoff between steps is what the extra parameters drive:

| Next `concurrency_key` | `continue_next` sets | Admission (out-of-band, AFTER the commit) |
|---|---|---|
| `:keep` (default) | nothing — same key/shard/slot held across the step | none (still holding the slot) |
| `nil` (release) | `concurrency_key` = NULL, `concurrency_shard` = NULL | none; the old slot is credited |
| configured new key | key = new, shard = **provisional 0** (out of the K=1 index) | `Limiter.admit` debits the bucket, **stamps the real shard**, draws the slot; old slot credited |
| unconfigured new key | key = new, shard = NULL (re-enters the K=1 arbiter) | none; a `gen_durable_concurrency_active` violation ⇒ **`:contended`**, the row requeues and the picker serializes the key; old slot credited |

**Ordering is a correctness invariant, not a preference.** The guarded `continue_next` runs
**before** `Limiter.admit`, because the PG limiter's admit `stamp`s `concurrency_shard` by id
**unguarded** by `locked_by`. If an orphaned chained task (its lease expired, its row
reclaimed) reached admit, that stamp would land on a row a new claimant owns — corrupting its
shard and, for an unconfigured key, dropping it out of the K=1 index (breaking exclusion). By
committing first, an orphan fails the guard (`:stale`) and never admits. If admit then denies
(rate token unaffordable / configured slot full), the row is requeued through the normal
outcome path and the picker admits it — inline never over-runs a limit.

A slot that changed mid-chain is reported to the scheduler (`{:slot_swap, id, slot}`) so the
heartbeat's `Limiter.renew` bumps the **current** slot; a lease-native backend (Redis) would
otherwise prune a slot it stops hearing about and double-book the key.

### Signals — `deliver_signal`, one statement

`target` (resolve an id or a `correlation_guard` among live instances) → `ins` (inbox
INSERT, `ON CONFLICT DO NOTHING` for dedup) → `wake` (UPDATE the target with the flip
condition in a `CASE`, **not** in the `WHERE`). The CASE placement is the lost-wakeup fix:
the wake always locks the target row, so racing a park it queues behind the park's row lock
and re-evaluates against the committed parked row. A status-filtering WHERE would skip the
not-yet-parked row without waiting.

### Maintenance sweeps

| Statement | Reads | Writes |
|---|---|---|
| heartbeat | — | extends `lease_expires_at` of the claimed set, ownership-guarded |
| reaper | expired leases via `gen_durable_lease`, ordered `SKIP LOCKED` claim | → `runnable`, `attempt + 1`; a parallel sweep fires await timeouts via `gen_durable_await_deadline` |
| GC (terminal) | ids via `gen_durable_gc`, sparing terminal children of mid-join parents | two-step: `SELECT` the batch, `DELETE ... WHERE id = ANY` — the delete is PK-driven, never a scan |
| GC (rate buckets) | idle-refilled and orphaned rate keys, ordered `SKIP LOCKED` | `DELETE` per key **all-shards-or-nothing** — safe because the pick re-mints a missing key full-minus-taken (cold ⇒ no rows at all), zero lag |
| GC (gate reconciler) | one transaction: ordered `SKIP LOCKED` lock of the `kind='conc'` shards | heal `capacity`/`available` from the executing-rows truth, sweep orphaned/idle keys whole, backfill shards missing after a `shards:` increase |
| scheduler startup | claims of a dead predecessor (same instance/queue/VM) | → `runnable` immediately instead of waiting out the lease |

## The locking discipline

- **Instance rows are only ever claimed with `FOR NO KEY UPDATE SKIP LOCKED`** (pick, reaper,
  GC, startup reclaim). A claim never waits, so instance-row locks cannot appear in any
  deadlock cycle — contention costs a skip, not a queue. NO KEY strength keeps claims
  compatible with the `FOR KEY SHARE` that FK checks (signal inserts) take on the target row
  — an in-flight signal insert no longer makes the pick skip its target row (the wake
  `UPDATE` inside signal delivery still queues behind a claim, as any write must). This is safe
  from lock-upgrade deadlocks *by schema*: lock strength escalates only when an UPDATE
  modifies columns of a **full** unique index, and the only full unique index is the
  immutable PK (the correlation/concurrency uniques are partial, which Postgres excludes) —
  adding a full unique index over a mutable column would invalidate this.
- **Counter shards are locked with `FOR UPDATE OF b SKIP LOCKED`, in sorted `(key, shard)`
  order.** Concurrent (cross-node) pickers grab *disjoint* shard subsets of a hot key and
  admit over what each grabbed, so they run in parallel instead of serializing on one bucket
  held across the whole claim; a lone picker grabs every shard and behaves exactly as an
  unsharded counter. Because a skip-locked acquisition never waits, counter locks — like
  instance-row claims — cannot appear in any deadlock cycle. (This reverses an earlier
  choice: with a single *unsharded* counter, blocking `FOR UPDATE` beat `SKIP LOCKED`, which
  spin-retried with no alternative work. Sharding removes the one hot row, so skipping wins —
  size `shards ≥ contending nodes`, or an under-sharded hot key degrades back to that spin.)
  The out-of-band credit (`Limiter.credit/2`) and the reconciler still take plain `FOR UPDATE`
  on the specific shard they address, in sorted order.
- **Racing inserts meet at unique indexes, in sorted order.** Ordered insertion turns
  "deadlock on each other's uncommitted index entries" into "one clean unique violation",
  which the caller resolves (pick retry) or ignores (`ON CONFLICT DO NOTHING`).
- **No advisory locks, no long transactions.** The only multi-statement transactions are
  `:await`'s park+recheck and the GC reconciler; user code never runs inside one.

## Constraints as the cross-node protocol

Where two nodes race, the schema is the referee — the loser's statement aborts and the
caller retries against the winner's committed state (bounded, observable via `:contended`
telemetry):

| Constraint | Race it settles |
|---|---|
| `gen_durable_concurrency_active` (unique) | two picks claiming one K = 1 key |
| `gen_durable_buckets` CHECK | residual gate over-admission; double credit (gates mint with `ON CONFLICT`, so only they trip the CHECK) |
| `gen_durable_buckets` PK | two picks minting the same cold rate key (rate mints without `ON CONFLICT`, so only it trips the PK) |
| `gen_durable_correlation` (unique) | duplicate business identity across concurrent inserts |

The meta-rule behind this (learned the hard way — see ISSUES #23): under `READ COMMITTED`,
values carried *across CTE boundaries* from rows other transactions are writing are not
trustworthy — locked scans can observe EPQ artifacts under contention. So counters
accumulate via row-resident read-modify-write (`SET x = x + 1`), admission math is fenced by
constraints, and a violated constraint means "recompute from committed truth", never "crash".

## Statement caching

Every statement has a static SQL text and goes through the connection-level prepared-
statement cache (`cache_statement:`), so Postgres parses and plans each shape once per
connection. Batch inputs ride in as typed arrays (`unnest`) rather than interpolated
placeholders — that is what keeps the texts static at any batch size.
