# Formal models

Quint models of gen_durable's concurrency protocol, checked with **TLC** (explicit-state:
every reachable state for a given scope) and **Apalache** (symbolic: bounded traces and an
inductive invariant, i.e. a proof for traces of any length).

| File | What it models |
|---|---|
| `gen_durable.qnt` | claim → out-of-band admission → group-commit flush → credit, plus reaper, reconciler, inline run-ahead (re-keying between steps), for both limiter backends (PG counters / Redis lease ZSET). Parameterised by the fixes below, so one model holds both the pre-0.2.16 protocol and the current one. |
| `gen_durable_proof.qnt` | The inductive invariant of the fixed protocol (PG backend, crashes, pick path). |
| `retries.qnt` | `attempt` and `max_attempts` across every way a run can end: an outcome, `{:error, _}`, a dying Task, a dying node. |
| `signals.qnt` | Signals and `:await`: delivery vs park + recheck, the `presented` exclusion, expiry, consumption. |
| `AGENTS.md` | **Instructions for agents**: when a change must touch the models, how to extend and run them, and the pitfalls. |
| `run.sh` | The whole matrix, each experiment with its **expected** outcome. Exits non-zero on any surprise, so it doubles as a regression check for both the bugs and the fixes. |

## Running

```bash
make formal-up   # build + start the formal container (Quint 0.34, Apalache 0.62.4, TLC, JDK 21)
make formal      # the whole matrix (TLC + Apalache)
```

A single job, inside the container (`docker compose … --profile formal exec formal bash`):

```bash
quint verify gen_durable.qnt --main=shipped --invariant=Safety --backend=tlc --tlc-config=tlc.json
quint verify gen_durable.qnt --main=shipped --invariant=Safety --max-steps=10            # Apalache, bounded
quint verify gen_durable_proof.qnt --main=gen_durable_proof --inductive-invariant=IndInv --invariant=Safety
quint run    gen_durable.qnt --main=fixed --invariant=PauseSafety --max-samples=20000    # random simulation
```

`scripts/check.sh <instance> <invariant> [tlc|apalache] [steps]` runs one job and prints a TLC
counterexample as one line per state (`scripts/trace.py`). Run jobs **one at a time**: quint drives
a single Apalache server on port 8822 for both backends, and TLC's heap is capped in `tlc.json`.

## How the model maps to the code

| Action | Code | Notes |
|---|---|---|
| `claim` | `Queries` `@claim_sql` | `runnable → executing`, `locked_by = worker`, provisional shard `0` for a gated key |
| `admitOk` / `admitDeny` | `Limiter.Postgres` `@admit_sql` / `release_claims` | debit + **unguarded** `stamp` / guarded release |
| `flush` | `Queries.flush/2` via `Flusher` | guard `locked_by = worker AND status = 'executing'`; `:done`, `:next` requeue, inline `:keep`, inline re-key |
| `credit` | `Limiter.credit/2` | run by the Flusher **after** the flush commits, and by `Executor.finish_inline` |
| `inlineAdmitOk` / `inlineAdmitDeny` / `requeue` | `Executor.admit_inline` / `finish_inline` / `apply_outcome` | the denied branch requeues the same outcome, crediting the old slot |
| `reap` | `Queries.reap/1` | does not clear `concurrency_shard` |
| `reconcile` | `Queries.reconcile_concurrency/1` | `available = cap − count(executing ∧ shard IS NOT NULL)` |
| `prune` | Redis `@admit_lua` `ZREMRANGEBYSCORE` | a member whose lease is no longer renewed |

Abstractions, and why they are sound for these properties:

- **One statement or transaction is one atomic action.** Row locks and `SKIP LOCKED` only
  serialise statements against each other; they never let one interleave *inside* another.
- **A worker id is a scheduler incarnation** (`Scheduler.init`: `claim_prefix <> unique_integer`),
  not a per-claim token. That is the code, and it is the root of finding #2.
- **One shard per key.** Shards spread contention; they add no new interleavings to these
  properties. Each key is its own bucket of `CAP`.
- **Time is abstract.** A lease expires when its task crashed, or, with `PAUSES`, at any moment:
  a BEAM pause, VM suspend or partition longer than `lease_ttl` while the scheduler incarnation
  survives.

Properties: `GateCap` (at most `CAP` admitted steps of a key run at once), `GateCapDb` (at most
`CAP` executing rows carry a stamped slot), `NoStaleCommit` (only the current claim's outcome
commits), `NoStepRegression` (never an outcome for a step the row has already moved past),
`AvailInRange`. `Safety` is all of them. `PauseSafety` drops `GateCap`: under pauses a zombie
keeps running by design, because a lease can stop a commit but not user code.

## Findings

All four are **fixed in 0.2.16**. The `*shipped*` instances keep modelling the protocol as it
was before, and must keep reproducing each bug; the `fixed*` instances model the current code.
`test/formal_findings_test.exs` pins each fix at the code level.

### #1 PG gate: the reconciler × a post-commit credit = double credit → cap exceeded

**No pauses or crashes needed.** Since 0.2.9 the slot credit is a separate statement that the
Flusher runs **after** the flush commits. If `reconcile_concurrency` runs in between, it no
longer sees the finished row and frees its slot; then the delayed credit frees it again.
`LEAST(capacity, …)` only stops the counter from going over `capacity`. It cannot stop it from
over-promising once another admit has taken the freed unit. TLC, 10 steps, cap 1:

```
 5 flush:next   avail[a=0] | r1 committed, its credit still pending (Flusher side effect)
 6 reconcile    avail[a=1] | no executing holder → available = cap
 9 admitOk      avail[a=0] | r2 takes the slot
10 credit       avail[a=1] | r1's late credit: LEAST(1, 0 + 1)
11 admitOk      avail[a=0] | r1's next step admitted → two steps running under cap 1
```

The inline path widens the window: on a re-key the continue commit moves the row off the old
key, and the old slot is credited only after the new key's admit round-trip. On the denied branch
it is credited only after a whole extra flush, which can wait up to `max_delay_ms`.

**Fix A: credit inside the flush transaction (PG backend).** This brings back the atomicity of
the pre-0.2.9 `credit_gate` rider, behind the behaviour. Add an optional
`Limiter.credit_in_txn/2` that `Queries.flush` calls inside its transaction for the committed
entries' slots. The Postgres backend implements it with the existing `@credit_sql`, taking its
bucket locks in `(key, shard)` order through a locking CTE, because two flushers could otherwise
deadlock on each other's buckets. Redis does not implement it; fix C makes its post-commit credit
safe. On the inline path, the continue entry credits the old slot in the same transaction
(`credit_old`), and the job's `slot` is cleared so a denied admit does not credit it a second time.
Lock order: the flush takes instance rows and then buckets. Admit takes buckets with `SKIP LOCKED`,
so it never waits on them, and then the rows of its own fresh claim. With fix B those two row
sets are disjoint, so no wait cycle can form.

### #2 ABA in the ownership guard: a zombie of the same incarnation commits a stale outcome

The guard is `locked_by = $worker AND status = 'executing'`, and `$worker` identifies the
**scheduler incarnation**, not the claim. Take a task whose lease expired: the BEAM was paused,
or the node was partitioned longer than `lease_ttl`. The reaper requeues its row. If the same
incarnation re-claims that row, the old task's outcome passes the guard. TLC finds the
stale commit in 6 steps. With an intermediate claim by another node, the zombie commits a step's
outcome on top of a row that has already advanced (`NoStepRegression`, 11 steps): a lost
transition and rewound state.
The same hole lets a zombie `:schedule_childs` insert duplicate children and overwrite the join
counter. It also defeats the "ownership proof" that the inline path relies on before its
unguarded admit stamp.

**Fix B: a per-claim fencing token.** No schema change is needed. The scheduler claims with
`locked_by = worker <> "#" <> pick_seq`, where `pick_seq` is a per-incarnation counter bumped on
every pick. The token is unique per claim, because ABA needs a *later* pick, and still prefixed
by the worker, so `reclaim_orphans`' prefix match and an incarnation-prefix heartbeat keep
working. Every ownership guard compares the job's token instead of the worker: the flush, the
continue entry, the children insert, `release_claims` and shutdown `release`. The PG admit joins
its input to `gen_durable ON id AND locked_by = token AND status = 'executing'`, so an admission
for a lost claim neither debits nor stamps. A `txid_current()` suffix written by the claim
statement works equally well; the model only needs uniqueness per claim.

TLC also shows that **fix A alone is not enough under pauses** (`fix_credit_inline_pauses`).
Without the fence, a zombie's commit now credits *inside* the transaction and over-admits again.
The two fixes go together.

### #3 Redis gate: a member is the row id, so a late `ZREM` evicts a newer holding

The Redis semaphore's member is the row id (`ZADD gd:conc:<key> lease <id>`). Suppose a row
commits, its `ZREM` is still pending, and the row is re-claimed and admitted. The admit's `ZADD`
of the same id is a no-op that takes no new slot. The late `ZREM` then removes the **new** holding,
and others are admitted over it. TLC, no pauses, cap 2: three steps run at once.

**Fix C: the member is the claim token** (fix B's `job.token`, or any per-claim nonce). `credit`
and `renew` address exactly that member, so a stale `ZREM` is a no-op.

### #4 Crash loops ignore `max_attempts`: a poison pill retries forever, with no backoff

`max_attempts` is consulted only when a step *returns* `{:error, _}` (`FSM.__retry__`). A run
that never returns is invisible to it: a Task that `exit`s, which ISSUES #10 deliberately keeps
on the crash path, or that is killed; a node that dies from OOM or a NIF crash; a step that
outlives its lease. The reaper bumps `attempt` and requeues with `eligible_at` unchanged, so the
row is re-picked at once and nothing ever compares `attempt` to the cap. A node-down is worse:
the successor's `reclaim_orphans` requeues **without** bumping `attempt`, so the counter stops
moving entirely. TLC (`retries_shipped`, cap 3):

```
claim+run → error:retry → claim+run → error:retry (attempt 2) → claim+run
          → node-down+reclaim (attempt stays 2) → claim+run   ← 4th run, cap is 3
```

A node-killing poison pill therefore rotates through the fleet without end, taking nodes down
as it goes, and never reaches `failed`. FSMs (no `max_attempts` at all) have no bound on crash
loops either.

**Fix D.** D1: before running user code, the executor fails a row whose `attempt` is at or above
its cap (`__gd_max_attempts__`), committing `{:stop, "max_attempts (N) exceeded …"}`. Jobs keep
their default cap of 20; FSMs opt in with `:max_attempts` and stay uncapped by default, so a
deliberate unbounded `:retry` still works. D2: `reclaim_orphans` bumps `attempt` like the reaper does. Both are needed: D1 alone
still loops through node-down reclaims (`retries_cap_only`). Not modelled but recommended: have
the reaper apply `backoff(attempt)` to `eligible_at`, so a crash loop is not a hot loop.

### Verified, not bugs

- **Inline run-ahead logic** is sound once #1 is out of the way: `shipped_inline_norec` (no
  reconciler) is exhaustive over 2.1M states.
- **Signals / `:await`**: no lost wakeup, a signal is consumed only by an outcome that saw it,
  and none vanishes except by the documented terminal-outcome drop (`signals_small`). The
  mutation check (`signals_no_recheck`) confirms that the invariant really depends on the park's
  recheck.
- **Leaks in the safe direction**, healed by the reconciler or by lease expiry: a crash between
  commit and credit, an admit failure after a debit, shutdown `release` of buffered gated rows,
  the reaper keeping `concurrency_shard`. The model includes all of them as `crash`/`reap`, and
  none violates a property.
- **Zombie execution under pauses** is inherent to leases. Fencing protects commits and
  accounting, not user code; that is why `GateCap` is only checked in the crash-only world.

## Results

`make formal` on Quint 0.34 / Apalache 0.62.4 / TLC (Apalache-bundled), an 8 GiB / 6-CPU colima
VM. Every TLC "ok" is a **complete** search: every reachable state of that scope, with 0 left on
the queue.

| Backend | Instance | Property | Result | States / time |
|---|---|---|---|---|
| TLC | `shipped` | Safety | **#1** violation | 24.7k, 12 s |
| TLC | `shipped_pauses` | PauseSafety | **#2** violation (stale commit) | 5.5k, 8 s |
| TLC | `shipped_pauses` | NoStepRegression | **#2** violation (step regression) | 683k, 29 s |
| TLC | `redis_shipped` | Safety | **#3** violation | 78k, 12 s |
| TLC | `retries_shipped` | Safety | **#4** violation | 38, 4 s |
| TLC | `fix_credit_inline_pauses` | PauseSafety | violation: A without B is not enough | 5.9k, 9 s |
| TLC | `retries_cap_only` | Safety | violation: D1 without D2 is not enough | 38, 4 s |
| TLC | `signals_no_recheck` | Safety | violation (mutation check) | 80, 5 s |
| TLC | `shipped_inline_norec` | Safety | ok | 856k, 43 s |
| TLC | `fix_credit` | Safety | ok | 159k, 14 s |
| TLC | `fixed` | PauseSafety | ok | 552k, 29 s |
| TLC | `fixed_inline` | Safety | ok | 31k, 10 s |
| TLC | `fixed_inline_pauses` | PauseSafety | ok | 607k, 33 s |
| TLC | `redis_fixed` | Safety | ok | 2.64M, 74 s |
| TLC | `redis_fixed_inline` | Safety | ok | 299k, 17 s |
| TLC | `redis_fixed_pauses` | PauseSafety | ok | 2.11M, 105 s |
| TLC | `retries_fixed` | Safety | ok | 20, 4 s |
| TLC | `signals_small` | Safety | ok | 2.8k, 4 s |
| Apalache, inductive | `gen_durable_proof` | IndInv ⇒ Safety | **ok: traces of any length** | 123 s |
| Apalache, inductive | `gen_durable_proof_shipped` | IndInv | breaks at `reconcile` (= #1, mutation check) | 91 s |
| Apalache, bounded 10 | `shipped` | Safety | #1 found symbolically | 402 s |
| Apalache, bounded 6 | `fixed_inline` | Safety | ok | 27 s |
| Apalache, bounded 10 | `retries_shipped` | Safety | #4 found | 5 s |
| Apalache, bounded 30 | `retries_fixed` | Safety | ok | 32 s |
| Apalache, bounded 10 | `signals_small` | Safety | ok | 36 s |

Practical notes, learned the hard way:

- Bounded Apalache on `gen_durable.qnt` grows about 5× per two steps (4 → 13 s, 6 → 25 s,
  8 → 134 s, 10 → 421 s). The inductive invariant is the way to go deep, not `--max-steps`.
- Apalache treats a state with no successor as a **deadlock** violation; TLC runs with
  `-deadlock`. Every model has an explicit `idle` stutter, and `run.sh` reports `got=deadlock`
  separately so it is never mistaken for an invariant violation.
- The inductive checks need `JVM_ARGS=-Xmx6g` (`run.sh` sets it). With the default 4G the
  server dies mid-check with `Connection dropped`.
- TLC defaults to `-Xmx8G`. `tlc.json` caps it at 5G so a concurrent Apalache server is not
  OOM-killed; better still, run one job at a time.
- `TRACE = false` on the instances expected to pass drops the `lastAction` label from the state,
  which otherwise multiplies the state count. The instances expected to fail keep it for
  readable traces.
