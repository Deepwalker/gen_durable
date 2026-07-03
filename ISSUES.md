# Design review findings

Review of the SQL layer and processing organization (2026-07-03). Items are
ordered by severity within each section. Line references are as of this commit.

## Correctness

### 1. Lost wakeup: `complete_await` racing `deliver_signal` — FIXED

**Status: fixed.** `deliver_signal`'s wake UPDATE now matches by id alone (flip
condition in CASE, terminal rows excluded) so it always takes the row lock and
EPQ-flips a park that commits first; `complete_await` is now park + recheck in
one transaction, catching a delivery that committed first. Whichever side
commits second sees the other. No stress test: the window requires
transaction-level interleaving control to reproduce deterministically;
correctness is argued by the locking analysis below.

`complete_await` (queries.ex:315) closes the "signal arrived before the park
committed" race with an `EXISTS` check inside the park UPDATE. Under READ
COMMITTED that only covers signals **committed before the statement's
snapshot**. When the two run concurrently, both sides can miss:

- `complete_await` takes snapshot S_a; its `EXISTS` does not see the signal
  (deliver has not committed yet).
- `deliver_signal`'s wake UPDATE (queries.ex:492) sees the row still
  `executing` (the park has not committed), so the `status =
  'awaiting_signal'` predicate does not match — the UPDATE **skips the row
  without locking or waiting** (Postgres only locks rows that match the
  predicate under the statement snapshot).
- Both commit. The instance is parked, a matching signal sits in the inbox,
  nothing ever wakes it (until another signal with the same name arrives).

Row locks do not help as-is: an UPDATE never waits on rows that fail its
predicate, and the FK `KEY SHARE` taken by the signal INSERT is compatible
with the park's `NO KEY UPDATE`.

Condition: each statement's snapshot precedes the other's commit — a
sub-millisecond window, but it is exactly the signals-racing-parks scenario
the `EXISTS` was built for, so it will fire under load. Consequence: unbounded
stall of the instance (not woken until an unrelated later signal).

Fix sketch (needs an interleaving proof + stress test before trusting):

1. Make the wake flip in `deliver_signal` match unconditionally — `WHERE id =
   $1` with the flip inside a `CASE` in `SET`. It then always locks the row,
   so when it commits second, EvalPlanQual re-evaluates the CASE against the
   final committed tuple and performs the flip.
2. Add a post-park recheck to `complete_await`: a second statement
   `UPDATE ... SET status = 'runnable' WHERE id = $1 AND status =
   'awaiting_signal' AND EXISTS (matching signal)`. Its fresh snapshot catches
   a deliver that committed first.

Together these cover all interleavings; cost is extra round trips on `:await`
(now a two-statement transaction). If item 5's single-statement deliver rewrite
is ever done, it must preserve the unconditional-match wake (the lock is the
fix).

### 2. Potential deadlock on rate buckets in the pick — FIXED

**Status: fixed.** The `locked` CTE now has `ORDER BY b.key` (the sort happens
before `LockRows`, so all concurrent picks acquire bucket locks in the same
order). The same ordering discipline was applied to every other multi-row
bucket writer: `bucket_keys/1` sorts, the `ensure` CTEs insert `ORDER BY k`
(two statements creating the same new keys via the arbiter index in opposite
orders can also deadlock), and `upsert_rate_configs` sorts by name (`DO
UPDATE` locks existing rows in VALUES order; rolling deploys could race with
reordered configs).

Original finding: the `locked` CTE locked bucket rows `FOR UPDATE OF b`
without ORDER BY — lock acquisition order was plan-dependent (hash join over
`DISTINCT rkey` yields arbitrary order). Two concurrent picks (different
nodes/queues — buckets are shared across queues) needing buckets X and Y in
opposite orders could deadlock. Postgres aborts one after `deadlock_timeout`
(1s); `repo.query!` raises, the scheduler crashes, and its claimed set waits
out `lease_ttl` (see also items 9 and 11 for that amplification).

### 3. Parameter ceiling in `insert_all` / `complete_schedule_childs` — FIXED

**Status: fixed.** Both are rewritten as `INSERT ... SELECT FROM unnest(...)`:
rows travel as 12 parallel arrays, so the parameter count is fixed (13 for
`insert_all`, 17 for `schedule_childs`) for any batch size, and the SQL text is
static (cacheable, item 4). `correlation_scope` (an array per row — arrays of
arrays would have to be rectangular) travels comma-joined and is split back
server-side; enum labels contain no commas. Covered by a 6000-row test
(72000 placeholders in the old form). A side effect: `insert_all([])` is now
valid (empty arrays insert nothing) instead of generating broken SQL.

Original finding: both built VALUES placeholders at 12 params per row. The
Postgres protocol caps a statement at 65535 parameters, so batches above ~5400
rows failed with an opaque protocol error.

## Performance

### 4. No prepared-statement caching — every call re-parses and re-plans — FIXED

**Status: fixed.** Every static statement now goes through `cache_statement:
"gen_durable/<name>"` (via the `q!/4` helper in Queries) — pick, heartbeat,
reap, gc, all `complete_*`, signal insert/wake, resolve_target, loads,
advisory locks, release. The unnest rewrite (item 3) made the bulk inserts
static-text so they cache too. The one remaining uncached statement is
`upsert_rate_configs` (dynamic VALUES, runs once at boot). Verified against
`pg_prepared_statements`: repeated calls reuse one named statement per query
per connection. Hosts behind a transaction-pooling proxy set `prepare:
:unnamed` on the repo, which bypasses the cache gracefully. The win (skipped
parse+plan per call) is not benchmarked — a before/after of DB CPU under load
would quantify it.

Original finding: `Repo.query!` went through Postgrex unnamed statements —
every execution of every query was parsed and planned server-side from
scratch. For the pick (the largest CTE query, run on every poll of every queue
on every node plus every completion-driven refill) this was a pure DB CPU tax.

### 5. Two extra round trips per step; five per correlation-key signal — FIXED

**Status: fixed**, both halves.

*Per-step loads*: the pick now batch-enriches its claims — one
`WHERE target_id = ANY($ids)` for signals and one `WHERE parent_id = ANY($ids)`
for children per picked batch, attached to the job maps; `Executor.run` reads
them instead of querying (2 statements per batch instead of 2×N; `load_childs`
and the per-job loads are gone). The inbox snapshot moves from execution start
to pick time; consumption stays exact (a progressing outcome deletes the
`ctx.awaited` ids the step actually saw — a signal landing after the pick
stays in the inbox for the next wake, same as one landing mid-step). With
`prefetch > 0` buffered jobs hold their snapshot until they run (documented on
the knob). Guarded by statement-count tests: a pick with claims is exactly 3
statements, an empty pick is 1.

*deliver_signal*: collapsed into ONE statement (was up to 5 round trips) —
CTEs `target` (resolve id or correlation_guard, live rows only) → `ins`
(inbox insert, dedup via ON CONFLICT) → `wake` (unconditional-lock CASE flip —
the item-1 race fix is preserved: the flip condition lives in CASE, not
WHERE, and ins+wake commit atomically). `SELECT count(*) FROM target`
distinguishes `:no_target`. Behavior change riding along: signaling a
terminal id now returns `{:error, :no_target}` instead of silently storing
garbage, and a missing id returns `:no_target` instead of raising an FK
violation (closes the "signaling a terminal instance" minor item).

### 6. `gen_durable_rate_buckets` grows without bound — FIXED

**Status: fixed.** `Queries.gc_buckets/1` rides on the GC process: a bucket
idle longer than `burst/rate` seconds is deleted (recreation-full equals its
refilled state, so nothing is lost), a zero-rate bucket is never swept (it
never refills — deleting would grant a fresh burst), and a bucket whose named
config was removed is swept unconditionally (unusable: both the pick and
`ensure` join configs). The race with a concurrent pick's writeback is closed
by the EPQ recheck after the lock wait (fresh `last_refill` ⇒ skipped). The
`[:gen_durable, :gc, :swept]` event gained a `buckets` measurement.

Original finding: with partitioned keys (`{:api, user_id}` → one bucket per
user) the bucket table accumulated a row per partition ever seen and was never
cleaned.

### 7. Conditional pick-index pathology: far-future schedules across priorities — DOCUMENTED

**Status: documented** in PERFORMANCE.md §6 ("Known limits / pathologies" #3),
with the triggering condition and the workload guidance (keep delayed
scheduling on one priority). The structural cure (a `scheduled` status plus a
promoter sweep) stays unbuilt until a real workload hits it — as recommended
below.

Index `(queue, priority, eligible_at) WHERE runnable`: within one priority
group, eligible rows sort before future ones — fine. But to reach priority 1
the scan must pass through **all future-eligible rows of priority 0**,
filtering them out. Pathology: a large backlog of delayed jobs on an urgent
priority plus live work on a less urgent one → every pick scans the whole
backlog for nothing. If delayed scheduling lives on a single priority (the
typical case) there is no problem. The radical cure is a separate `scheduled`
status plus a promoter sweep (like Oban's Stager) — a new background process;
document the limitation in PERFORMANCE.md until a real workload demands it.

## Processing organization

### 8. The engine is a hard singleton — FIXED

**Status: fixed.** The `:name` option (an atom, default `GenDurable`) is now
the instance identity, and every global surface is keyed by it:

- config lives at `:persistent_term` key `{GenDurable, name}`; the public API
  resolves `opts[:name] || GenDurable` and raises a clear "no GenDurable
  instance named X" on a miss (no more silent misrouting);
- the FSM registry is no longer a process or a named ETS table — the engine
  supervisor owns an unnamed protected table (`Registry.new/1`), its tid
  travels in the config, and `Registry.fetch!/3` takes it explicitly;
- the Task.Supervisor name is derived (`Module.concat(name, TaskSupervisor)`);
- Reaper and GC lost their global names entirely (nothing calls them);
- `child_spec` id and the supervisor registration default to `GenDurable`
  (previously `GenDurable.Supervisor`) — a duplicate `:name` fails with the
  standard `:already_started`.

Covered by a two-instance engine test (separate queues, `name:`-routed insert,
loud error for an unknown name). Deliberately not done: the persistent_term
entry is not erased on shutdown (a Supervisor has no terminate hook; a stale
entry is harmless — writes against a stopped engine legally queue rows in the
DB), and telemetry metadata does not carry the instance name yet (mechanical,
touches every emit site — worth doing if anyone actually runs two engines).

Original finding: the config key was fixed (`{GenDurable, :config}`) and
`GenDurable.TaskSupervisor` / `Registry` / `Reaper` were singleton-named — a
second instance either crashed on a name clash or silently routed
`GenDurable.insert/signal` to the other instance's repo.

### 9. A scheduler crash strands claimed rows for a full lease_ttl — FIXED

**Status: fixed.** Worker ids are now built by the scheduler as
`<instance>:<queue>@<vm>-<uniq>` (`Scheduler.claim_prefix/2`), and at init the
scheduler releases every row whose `locked_by` carries its prefix — claims of
a dead predecessor go straight back to `runnable` instead of waiting out the
lease. Observable as `[:gen_durable, :scheduler, :reclaimed]`. The prefix pins
the VM: the node name when distributed, else hostname + OS pid — two
non-distributed VMs would otherwise both be `nonode@nohost` and reclaim each
other's live claims. Safe on top of item 11: a predecessor's orphaned tasks
may still be running, and their late commits are dropped by the ownership
guard (early re-run is plain at-least-once). Prefix matching uses
`left(locked_by, ...)` rather than LIKE, so names containing `%`/`_` need no
escaping.

Original finding: a crash-restart (including the item-2 deadlock) left
buffer + in-flight rows waiting for the reaper, up to 60s by default.

### 10. `throw`/`exit` in a step bypasses `handle/2` and costs a lease_ttl — FIXED

**Status: fixed.** An uncaught `throw` is now caught and routed to `handle/2`
as `{:throw, value}` — a controlled non-local return, not a crash, so it costs
nothing beyond the handler's decision. A bare `exit` deliberately still takes
the crash path (reaper, at-least-once floor): it is a process-level event.
Documented in the machines guide and Executor moduledoc; covered by an engine
test (a throwing FSM reaches `failed` promptly under a 60s lease).

Original finding: `invoke` used `rescue` only — a user `throw` crashed the
Task → treated as a worker crash → the row waited out the lease (60s) plus an
attempt bump.

## Minor

- **`hashtext` → `hashtextextended`** — FIXED: advisory locks now hash the
  concurrency key with `hashtextextended` (64-bit); a collision only ever
  falsely serialized two keys, and the bigint keyspace makes it negligible.
- **Signaling a terminal instance** — FIXED with item 5: the single-statement
  deliver resolves live rows only, so a terminal or missing id returns
  `{:error, :no_target}` (previously `:ok` + garbage, or an FK violation).
- **`Jason.encode!` → `$::jsonb`** — FIXED: passing an Elixir *string* to a
  jsonb parameter made Postgrex JSON-encode the string, so `state`/`result`/
  signal payloads were stored as **jsonb scalar strings**, not objects
  (`state->>'n'` was NULL; jsonb indexes impossible). All JSON parameters are
  now bound as text and parsed server-side (`$n::text::jsonb`), so every write
  path stores real objects — guarded by a `jsonb_typeof` regression test over
  insert, insert_all, complete_next, complete_done, and deliver_signal. Rows
  written in the old format still decode (binary branch in `decode_json` /
  `State.from_db`), so the change is backward-compatible; old rows migrate to
  the object form the next time their state is rewritten by a transition.
- **Reaper telemetry carries the full id list** — FIXED: the
  `[:gen_durable, :reaper, :reaped]` event now carries `count` only.
- **`reap` has no LIMIT** — a mass reclaim in one UPDATE is a long transaction
  on very large sets. Rare event; low priority.

## Addendum (found while fixing item 1)

### 11. Outcome commits are not guarded by `locked_by` (stale-worker overwrite) — FIXED

**Status: fixed.** Every outcome now commits only while the worker still owns
the claim: the worker id rides in the job map (set by `pick`), and every
`complete_*` (plus `reset_to_runnable`) guards with `locked_by = $worker AND
status = 'executing'`. Side effects are gated on the guarded UPDATE via CTE
references — `consumed` and the parent-join decrement read the guarded
UPDATE's `RETURNING` (never re-read the table, which would see the pre-update
snapshot and fire even when the guard EPQ-failed), and `schedule_childs` uses
a leading `claim` CTE (`SELECT … FOR UPDATE`) because its parent park depends
on the inserted-children count and so cannot itself be the guard. A stale
outcome affects zero rows, returns `:stale`, and the executor emits
`[:gen_durable, :outcome, :stale]`; the current claimant redoes the step
(at-least-once). Covered by tests: a reclaimed row rejects all outcome kinds;
a stale terminal outcome touches neither the inbox nor the parent join
barrier; a stale `schedule_childs` spawns no children and consumes nothing.
This also unblocks item 9 (startup reclaim), which widens the reclaim window
and needed this guard in place first.

Original finding: every `complete_*` updated `WHERE id = $1` unconditionally.
A scheduler crash does not kill its running Tasks (`async_nolink`), but the
new incarnation has a new worker id and does not heartbeat the old claims:
their leases expire, the reaper returns the rows to `runnable`, another worker
picks them — and an orphaned Task later commits its outcome **over the
re-claimed row**, rewinding step/state mid-flight of the new claimant, nulling
its `locked_by` (which silences its heartbeat), or — terminally — deleting the
inbox and decrementing the parent join barrier.

### 12. Arbiter-order deadlock on concurrent batch inserts with shared new keys — FIXED

**Status: fixed.** `insert_all` and `complete_schedule_childs` insert
`ORDER BY t.correlation_key` (server-side, inside the unnest SELECT), so every
node acquires arbiter-index entries in the same global order — the cycle is
impossible by construction, the same discipline item 2 applied to the bucket
writers. Side effect: ids are assigned in key order, not entry order —
indistinguishable to callers, since dropped duplicates already break any
positional mapping (documented on `insert_all/3`). Keyless rows never touch
the arbiter index and are unaffected. Like the item-1 race, this is argued,
not tested — a deterministic reproduction needs transaction-level interleaving
control; the existing dedup and 6000-row tests guard against regressions.

Original finding: same class as item 2's `ensure` case, on `gen_durable`
itself — two concurrent batches inserting the same NEW correlation keys in
opposite orders (`ON CONFLICT` waits on the other uncommitted transaction's
index entry) could deadlock; insertion order was the caller's entry order,
which is arbitrary when batches are built from maps/sets.

## Verified sound (checked deliberately)

Single-statement outcomes with data-modifying CTEs instead of transactions;
the pick is exactly the canonical SKIP LOCKED shape with window dedup and no
second locking pass; the two-step GC with its Seq Scan rationale; the
concurrency_key layering (picker guard as optimization, advisory lock as
correctness — the cross-node pick race is covered); partial indexes matching
query predicates 1:1; the heartbeat decoupling buffer depth from lease_ttl.
