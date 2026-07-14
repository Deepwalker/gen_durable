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

## Re-review findings (second adversarial pass, post-hardening)

An independent adversarial review of the final state verified items 1–5, 8–12
as sound (full interleaving arguments for the lost-wakeup fix and the
ownership guard) and found the following. All fixed unless noted.

### 13. `gc_buckets` could permanently stall rate-limited rows — FIXED (was a regression of item 6)

The pick grants a rate-limited winner only if its bucket row exists (`locked`
inner-joins buckets), and buckets were created only by the `ensure` CTEs at
transition time — never by the pick. A row that slept past its bucket's refill
horizon (far-future schedule, long retry backoff — writeback refreshes
`last_refill` only on pick traffic, and `ensure`'s DO NOTHING never does) woke
up ungrantable **forever**, and such rows accumulate at the head of the
`cand` window, eventually starving the whole queue. The item-6 premise
("recreation-full equals refilled") was right about tokens, wrong about there
being a recreator. **Fix:** a `heal` CTE in the pick recreates (full) any
winner's missing bucket; the insert is invisible to `locked` in the same
statement (CTE snapshot), so the row is granted on the NEXT pick. Common path
(no rate-limited winners) is a no-op. Tested: delete the bucket, first pick
returns `[]` and recreates it, second pick grants.

### 14. The documented accumulate-a-pack pattern spun in a busy-loop — FIXED

The park recheck (and the pre-fix park EXISTS before it — this predates the
item-1 fix) woke on ANY inbox signal matching `awaits`, unable to distinguish
a signal the step was just handed (`:await` consumes nothing) from a new one.
The guide's own Collector pattern: woken by "b" → re-await → recheck sees the
unconsumed "b" → immediate flip → completion-driven re-pick → re-await → … a
full-speed loop until the pack completes. **Fix:** `complete_await` takes the
`presented_ids` (the `ctx.awaited` the step received — the executor already
had them as `consumed`) and the recheck excludes them
(`id != ALL($presented)`). A set-difference, not a max-id watermark: ids
commit out of order, and a watermark could skip a signal inserted earlier but
committed later. Deliveries are unaffected (a delivered signal is a fresh
insert, never in the presented set). Tested: re-await with the presented
signal parks; a new matching signal wakes.

### 15. Deterministic user-data errors became infinite crash loops — FIXED

`Jason.encode!` of an unencodable `:done` result, `State.to_db` of a bad
state, a bad child spec — all raised AFTER `invoke`'s rescue window, so
`handle/2` never saw them: crash → lease wait → reap → same crash, forever,
no terminal state. **Fix:** outcome serialization moved inside `invoke`'s
guarded region (a new `serialize/2` producing the DB-ready tuple), so these
route to `handle/2` (default `{:stop, reason}`) like any step failure;
`apply_outcome` now consumes pre-serialized outcomes only. Deliberately NOT
guarded: `Registry.fetch!` — an unresolvable fsm is most likely a rolling
deploy (this node doesn't know the module yet; another does), so the crash
path (lease floor, re-pick elsewhere) is correct and a terminal `:stop` would
lose the instance. Tested: an unencodable result reaches `failed` promptly
under a 60s lease.

### 16. `vm_id` collision could reclaim a live VM's claims — FIXED

Containers commonly run BEAM as OS pid 1, and hostnames can collide
(compose-scaled fixed `hostname:`, cloned images) — two non-distributed VMs
then share a claim prefix, and startup reclaim would release each other's
live claims. **Fix:** reclaim additionally requires a stale lease —
`lease_expires_at < now() + (lease_ttl − 2 × heartbeat_interval)`. A live
owner beats every `heartbeat_interval` and always sits above the threshold
regardless of prefix collisions; a dead owner's rows qualify after ~2 missed
beats, still far ahead of full lease expiry. Rows claimed moments before the
predecessor died wait for the reaper (documented remainder).

### 17. Multi-row maintenance statements could deadlock each other — FIXED

A late `heartbeat` (id-order locking) overlapping the `reap` (lease-index
order) on ≥2 expired rows could cycle; same shape for `release` vs `reap` and
`gc_buckets` vs the pick's bucket locks. Self-healing (supervisor restart +
ownership guard) but crash-amplifying. **Fix:** every multi-row maintenance
statement (heartbeat, reap, release, reclaim_orphans, gc_buckets) now claims
its rows via `SELECT … ORDER BY … FOR UPDATE SKIP LOCKED` and then
updates/deletes the claimed set — never waits, deterministic order, no cycles
possible. SKIP LOCKED is also semantically better: a locked row is being
actively worked (outcome committing, beat extending) — exactly when
maintenance should skip it until the next tick. (Since 0.2.6 the
*instance-row* statements use `FOR NO KEY UPDATE SKIP LOCKED` — same ordering
and skip discipline, weaker strength; bucket locks stay `FOR UPDATE`.)

### 18. Window-partition collision between a numeric concurrency_key and an id — FIXED

`coalesce(concurrency_key, id::text)` collapsed a key `"42"` with an
unrelated row of id 42 into one dedup partition (transient underfill only).
Now `coalesce('k:' || concurrency_key, 'i:' || id::text)`.

### 19. `min_demand` above the claim ceiling silently disabled refill — FIXED

Clamped at scheduler init to `concurrency + prefetch`.

### 20. PERFORMANCE.md contained stale, now-false claims — FIXED

§1's cost table (per-step loads "always-on"), §3's `deliver_signal` EXPLAIN
showing the pre-fix status-guarded wake (the exact shape whose
skip-without-locking was the item-1 bug), and the `gen_durable_unique`
index naming — all refreshed to the shipped code.

### 21. Hygiene batch

Fixed: `complete_await` now resets `attempt` (asymmetry with `:next` looked
unintentional); the scheduler's buffer-order comment no longer overclaims
(RETURNING order is not SQL-guaranteed); the supervisor's queue-loop variable
no longer shadows the instance name; documented — `:prefix` requires the
schema on the repo's `search_path`, GC frees "reserved" correlation keys at
retention (identity guide), `ctx.childs` spans fan-out rounds (children
guide), the registry's dynamic fallback needs loaded modules (lazy-loading
caveat).

Noted, not fixed: insert-time `:rate_limit`/`:weight` are unvalidated (no
`[:rate_limit, :unknown]` telemetry on the insert path — only `:next` warns);
`upsert_rate_configs` never deletes removed config rows (harmless — orphaned
buckets ARE swept, config rows are a handful); two instances sharing a repo
with different `rate_limits` last-write-win on shared names.

### 22. The advisory-lock layer was redundant — REPLACED with a unique arbiter

**Status: fixed** (came out of the concurrency-cap design discussion). The
execution-time serialization of `concurrency_key` — a session advisory lock on
a checked-out connection held for the whole step — existed to close the
cross-node claim race the pick's snapshot guard cannot see. But the invariant
it protected ("at most one executing row per key") is exactly expressible as a
DB constraint: `gen_durable_concurrency_active` is now a UNIQUE partial index,
so the second claim of a racing pair is **uncommittable** — the claim itself
is the lock, held precisely for the step window (the executing status) and
released by any outcome or the reaper. Deleted: `advisory_try_lock/unlock`,
the `Repo.checkout` pinning (keyed steps no longer consume a pool connection
1:1 for their duration — former PERFORMANCE known-limit #2),
`reset_to_runnable`, the contended hand-back path. A violation aborts the
losing pick's whole claim statement (an UPDATE has no ON CONFLICT), which
costs one bounded retry of a rare race — observable via the repurposed
`[:gen_durable, :concurrency, :contended]` (`%{queue}` metadata).

Why hold-for-the-step could not simply be dropped without the constraint: a
momentary lock validates nothing (the second claimant acquires it a moment
later), and post-commit self-checks cannot safely elect a loser — the earlier
committer can be provably blind to the later one, and commit order is not
cheaply recoverable from row data. Mutual exclusion over a time window needs
something held for the window; the executing row itself, fenced by the unique
arbiter, is that something — for free.

Note for the future concurrency-cap feature (K > 1): a unique index does not
count to K; that design needs a slots table — tracked in the design
discussion, not here.

### 23. Gate over-admission race, caught by its own CHECK under bench load — FIXED

The concurrency-gate throughput bench (20k jobs, 8 pickers, one hot shard)
crashed on `CHECK (available >= 0)`: `available` went to −1/−2 — the pick
over-admitted past the fresh availability. Forensics: a trigger-ledger run
showed exact conservation (the race is timing-sensitive; plpgsql overhead
masks it), and the overshoot arithmetic matched exactly one mechanism — the
locked bucket scan (`c_locked`) emitting the SAME bucket row twice under a
concurrent write (old + EPQ-refreshed version), which duplicates its entry in
the cumulative ranges and doubles its admission window (available 1 → ranges
(0,1] and (1,2] → 2 admitted, debit base 1 → −1). A controlled two-session
experiment confirmed the simple no-join `FOR UPDATE` shape IS
EPQ-fresh — the duplication needs the join+window plan under contention.

**Fix, two layers:** `c_ranges` deduplicates by `(key, shard)` taking
`min(available)` (the conservative version wins — worst case a transient
under-admission, retried next pick), and the pick's constraint-retry rescue
now also covers `gen_durable_concurrency_buckets_check` — the same
constraint-equals-correctness discipline as the K=1 arbiter, so any residual
shape of this race aborts and retries instead of committing or crashing the
scheduler. Re-ran the bench: three consecutive runs of the crashing scenario
clean, post-drain slot conservation exact, and the measured numbers landed in
PERFORMANCE §2c (one hot shard 0.45× of lockless at pure gate traffic, 8
shards 0.80×).

The meta-lesson goes in the pick's comments: cross-CTE reads of concurrently
written rows are not trustworthy even under `FOR UPDATE` — accumulate through
row-resident RMW or fence with constraints, never through values carried
across CTE boundaries.

### 24. Cold-gate heal-lag broke drain's quiescence contract (external report) — FIXED

Reported from a host app: a step switched its `concurrency_key` mid-flight
onto a gate whose buckets did not exist; the pick's `c_heal` minted them
invisibly-to-itself ("admitted next pick"), returned an empty batch, and
`GenDurable.Testing.drain/1` — whose contract is "until quiescence, nothing
left that can run" — treated the empty pick as done and exited with a runnable
row stranded. Deterministic under the Ecto sandbox (minted buckets roll back
every test, so the cold path reproduces on the first transition to every
gate). Two root causes stacked: (1) the ensure riders covered insert /
insert_all / schedule_childs but NOT the `:next` key-switch — a documented
"acceptable one-pick lag" that wasn't; (2) the lag itself made "empty pick" an
untruthful signal for every caller — including the production scheduler, whose
idle backoff would stretch a cold gate's wake-up toward `max_poll_interval`.

**Fix — the mint became part of admission (the zero-lag design):** a cold
gate's capacity is known without reading anything (a fresh mint is full by
definition), so the pick computes virtual full shards from the config
(`c_cold`), admits against `locked ∪ cold`, and materializes the cold shards
**pre-debited** (`c_mint`: `available = cap − taken`). Two picks racing the
same cold key merge via `ON CONFLICT DO UPDATE` (the loser's debit lands on
the winner's row); a combined overdraft is uncommittable (CHECK) and retried —
the standing discipline. This DELETED the gate ensure riders from all three
insert paths and the pick's `c_heal`: fewer mechanisms, no special cold state,
"no buckets" is now indistinguishable from "buckets full". The reconciler
additionally backfills missing in-range shards (a `shards:` increase used to
silently shrink capacity — the all-or-nothing invariant `c_cold` relies on is
now actively restored each sweep).

Verified: full suite (incl. the reporter's exact scenario: key-switch onto a
cold gate → admitted by the very next pick; drain over a cold gate completes
in one call), EXPLAIN (cold CTEs `never executed` on unkeyed batches), the
throughput bench re-run within noise of the pre-change numbers (baseline
9 579 jobs/s, uncapped gate 1 shard 0.46×, 8 shards 0.88×), and three
8-worker/20k conservation runs with exact post-drain slot balance.

### 25. Rate limiter converted to the same zero-lag mint — the last ensure/heal user — FIXED

After #24 the rate limiter was the odd one out: buckets were still minted by
`ensure` riders on insert / insert_all / `:next` / schedule_childs, with the
pick's `heal` CTE as the backstop for keys whose idle bucket the GC swept —
and `heal` had the exact statement-invisibility #24 was about (its mint is
invisible to its own `locked`, "granted on the NEXT pick"). Reachable through
a GC-sweep + sleeping-row interleaving in production (one poll of extra
latency plus a false `:throttled` event with granted=0), and the same
drain-contract hole in principle. The riders also taxed every insert path for
a key most transitions never set.

**Fix — same design, one asymmetry:** `r_cold` computes the virtual full
bucket (a fresh mint is `burst` by definition) for winner keys with no bucket
row, `avail` admits over `locked ∪ cold`, and `r_mint` INSERTs the cold
buckets pre-debited (`burst − consumed`). The asymmetry to the gates: a
racing double-mint cannot MERGE debits — the merge arithmetic needs `burst`,
which is a config value, not a bucket column, and `ON CONFLICT DO UPDATE`
sees only the target row and EXCLUDED — so the collision is left to the
bucket's PRIMARY KEY: the losing claim aborts whole and the pick retries
against the winner's committed row (rescue extended;
`[:gen_durable, :rate_limit, :contended]`). DELETED: `ensure_buckets_cte`,
`bucket_keys`, the `:next` and schedule_childs ensure riders (schedule_childs
dropped a parameter: 18 → 17), and `heal`. Inserts are now rider-free
single statements.

Verified: full suite incl. new tests (a cold key admits on the very first
pick, mint lands pre-debited; `:next` onto a cold rate key granted by the
very next pick; a swept bucket re-mints with zero lag; drain over a cold
bucket completes in one call), EXPLAIN (r_cold anti-join and r_mint
`never executed` / rows=0 on unkeyed batches), throughput bench — every rate
scenario within the ±3 % noise band of baseline (9 150 jobs/s baseline; hot
bucket cold 1.02×, warm 1.03×, 100 partitions 0.99×, 2 000 cold keys 0.98×;
27 mint collisions retried across the run, all jobs drained), and three
conservation runs (rate 0, burst 5, 100 fresh keys × 10 jobs, 8 workers
racing the mints): exactly 5 grants per key, every bucket at exactly
0.0 tokens.

Still open from the #21 batch, deliberately not taken here: insert-time
`rate_limit` names are unvalidated (a typo'd name stalls the row, visible
only via throttled telemetry), and the legit-throttle flavor of the drain gap
— `promote_scheduled` collapses scheduled time but not token-refill time, so
a drain with more pending weight than burst exits before quiescence.

### 26. Rate writeback under the #23 EPQ lens — AUDITED, no violation reproduced

The #23 forensics left a suspicion: the rate `locked` CTE is the same
join+ORDER BY+FOR UPDATE shape whose gate sibling emitted a row twice under
contention, and rate buckets have NO CHECK — a duplicate/stale `avail` row
would not crash, it would silently commit token debt (writeback from the
fresh row, grants judged against the stale one) or token inflation (writeback
from the stale row, the concurrent debit lost).

Audited two ways. (1) Two-session shape experiment (the #23 protocol): a
session running the locked shape blocks behind a concurrent debit and, on
commit, returns the row ONCE with the FRESH value — same result as #23's
control. (2) An adversarial conservation harness on the WARM path: rate 0
(exact arithmetic), pre-warmed buckets, a permanently-throttled tail as the
over-admission tripwire (any stale grant would draw from it), neutral
mutator sessions (`SET tokens = tokens` — new row versions at maximum rate
with zero balance impact, standing in for the gates' credit riders), pickers
racing at both batch extremes (8×50 and 16×7, up to ~75k row-version churns
per scenario). Twelve scenario-runs: `done` per key EXACTLY equal to burst,
every bucket at exactly 0.0 tokens, nothing negative, nothing inflated.

Structurally consistent: the #23 duplication was only ever observed feeding
a WINDOW aggregate over the locked scan (the doubled cumulative range);
the rate path has no window over `locked` — its output goes through a 1:1
projection (`avail`), a per-candidate join (`granted`), and a by-key UPDATE
(`writeback`). Not proof — the mechanism was never fully pinned — which is
why the tripwires stay cheap to re-run (`/tmp/rate_epq_repro.exs` recipe
recorded here). Verdict: no fix warranted on current evidence.

Byproduct finding, documented as PERFORMANCE §6 pathology #4: rows denied
AFTER the candidate window — rate-throttled rows and rows of a saturated
configured gate alike (the `cand` guard only pre-filters K=1 keys; configured
names pass unconditionally) — OCCUPY the pick window's `LIMIT` slots while
staying runnable at the head of the sort order. A saturated key with a
backlog deeper than batch × concurrent picks therefore starves same-priority
work behind it until the head drains (refill rate for a rate key, completion
rate for a gate; never, for a head that cannot run — `weight > burst`, an
unconfigured rate name, which raises the stakes of the #21 no-insert-time-
validation gap). Surfaced by the first harness variant (key-grouped
insertion: 5 of 8 keys never entered the window). Mitigation documented in
both guides: own queue for heavily capped flows, or a less urgent priority.

### 27. Local poke storm × out-of-band write amplification — FIXED (debounce); adaptive backoff NOTED

**Status: fixed (debounce).** The local poke leg (`Poke.fanout(_, :local)`, run by every
transport for the caller's node) has no sender-side dedup — insert/insert_all, signal
delivery, and join/fan-out each fire one poke per event (`dispatch_rows` dedups only
per-queue within a single batch). The idle-gate (0.2.8) drops pokes only when the scheduler
is BUSY; a scheduler saturated on a configured limit is IDLE (`idle? = in_flight == [] and
buffer == []`) yet admits nothing, so every poke passed straight into a `fill`.

Benign before 0.2.9 — an empty pick was one read-only statement (the fused pick never
flipped denied rows to executing). The out-of-band split (#item: limiter behaviour) made an
empty pick on a saturated limit WRITE: `claim` (≤batch rows → executing) + `release_claims`
(≤batch → runnable). So a hot insert/signal stream to a saturated queue could drive
~poke-rate × 2·batch row-writes of pure churn — the picker-thrash regression.

**Fix:** an idle scheduler debounces the poke stream (`Scheduler`, `@poke_debounce_ms 10`):
the first poke arms a single timer, pokes within the window are dropped (timer already armed),
and the timer fires ONE `fill`. Poke-driven picks are bounded to ≤1 per window regardless of
poke rate; busy schedulers still drop pokes; freed capacity under saturation is found by
completion-refill (not pokes), so the debounce costs no real latency there. Verified: full
suite green — the 10 ms debounce is transparent (work is still discovered, ≤10 ms later).

**NOTED (not taken now):** under *sustained* saturation the debounce still allows ≤1 empty
claim+release per window (~100/s at 10 ms). An adaptive poke backoff — an empty poke-fill
stretches the next window like `adapt` stretches the idle poll, resetting on a filling one —
would collapse that to near-zero while keeping low latency for genuine idle→work transitions.
Deferred: the fixed debounce already turns unbounded churn into bounded; the adaptive version
trades simplicity for a win that only matters under a pathological hammer-a-full-gate load.
Revisit when a real workload shows material picker-write load from a persistently saturated
queue. (Related: pathology #4 above — denied rows occupying the pick window's `LIMIT` slots.)

**Coverage gap:** no deterministic test of the coalescing — schedulers are unnamed / not
directly addressable and timing assertions are flaky; the state-machine invariant (a second
poke while `poke_timer != nil` arms no second timer) is read-verified only.

## Design additions (feature review)

### 28. Inline execution (run-ahead) — the design and its deliberately-taken tradeoffs

**Status: NOTED (feature; the tradeoffs below are accepted and documented, not bugs).**
`inline_execution:` lets a `:next` run the next step in the same worker task (guarded
`continue_next` keeps the row `executing`) instead of requeuing through the picker. Three
non-obvious hazards were closed by design; two residual tradeoffs are accepted.

**Closed — orphan corrupts a live claimant via the unguarded admit stamp.** The PG limiter's
`admit` `stamp`s `concurrency_shard` by id **without** a `locked_by` guard (limiter/postgres.ex,
`stamp` CTE). If a mid-chain admit ran from an orphaned task (lease expired, row reclaimed by a
new worker), that stamp would overwrite the new claimant's shard — and for an unconfigured key,
a non-null shard drops the row out of the `gen_durable_concurrency_active` index, **breaking
K=1 exclusion**. Closed by ordering: the guarded `continue_next` commits FIRST and re-proves
ownership; an orphan fails the guard (`:stale`) and never reaches admit. Verified: engine +
queries tests (`:stale` path), and the ordering is asserted in the executor's structure.

**Closed — Redis slot pruned mid-chain on a key change.** The scheduler renews held slots from
its `in_flight` map on the heartbeat; a chained step that swaps the concurrency slot (a new
key) would leave the new slot unrenewed → a lease-native backend prunes it → the key
double-books. Closed by the executor sending `{:slot_swap, id, slot}`; the scheduler tracks the
current slot. (`:keep`, the common case, never swaps.)

**Closed — unconfigured-key contention mid-chain.** A new unconfigured key already held by
another executing row trips the K=1 unique index inside `continue_next`; caught as `:contended`,
the whole statement rolls back and the row requeues so the picker's arbiter serializes it —
identical to a first pick losing the K=1 race. Deterministically tested (`queries_test.exs`).

**NOTED — priority staleness across a chain.** An inline FSM holds one executor slot and runs
its steps back-to-back without the picker re-evaluating `(priority, eligible_at)` between them,
so a higher-priority row that arrives mid-chain waits until the chain yields (a terminal/await/
retry outcome, a denied token, or a step with `inline_execution: false`). Accepted: inline is
opt-in per FSM, and a boundary step (`inline_execution: false`) is the explicit yield lever.
Triggering condition to revisit: an inline FSM with long `:next` chains sharing a queue with
latency-sensitive higher-priority work — cap chain length (a `run_ahead: N` bound) if it bites.

**NOTED — rate-token leak on a mid-chain `:stale`.** The rate token is admitted after the
guarded `continue_next` but the token debit is not itself rolled back if a later step's commit
goes `:stale`; the token leaks in the safe direction (throughput slightly under budget) and
refills by time. This is the same leak-on-crash the out-of-band admit already has (0.2.9); a
mid-chain `:stale` requires the lease to expire *during* a step, which is already a degenerate
(scheduler-dead) case. Not worth a rollback round-trip.

## Verified sound (checked deliberately)

Single-statement outcomes with data-modifying CTEs instead of transactions;
the pick is exactly the canonical SKIP LOCKED shape with window dedup and no
second locking pass; the two-step GC with its Seq Scan rationale; the
concurrency_key layering (picker guard as optimization, advisory lock as
correctness — the cross-node pick race is covered); partial indexes matching
query predicates 1:1; the heartbeat decoupling buffer depth from lease_ttl.
