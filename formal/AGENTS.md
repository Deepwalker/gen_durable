# Formal models: instructions for agents

`formal/` holds Quint models of gen_durable's concurrency protocol, checked with TLC
(explicit-state, exhaustive for a scope) and Apalache (bounded, plus an inductive invariant =
a proof for traces of any length). They found four real bugs (ISSUES #31–#34, fixed in 0.2.16)
that the test suite never hit. Treat them as part of the test suite for anything concurrent.

Read `formal/README.md` first: it maps every model action to the code (`Queries`, `Limiter.*`,
`Flusher`, `Executor`, `Scheduler`, reaper/reconciler) and states every abstraction.

## When you MUST touch the models

Any change to the semantics of one of these is a protocol change:

- the claim (`@claim_sql`), the claim token / ownership guard (`locked_by`), `release*`, heartbeat;
- admission and credit (`Limiter.admit/credit/renew/reconcile`, `@admit_sql`, `@credit_sql`,
  the Redis Lua), including *when* a credit runs relative to a commit;
- the flush (`Queries.flush/3`): what commits atomically with what, the guard;
- inline run-ahead (`Executor.attempt_inline/finish_inline`) and its slot hand-off;
- reaper, `reclaim_orphans`, `attempt` / `max_attempts`;
- signals and await (deliver, park + recheck, consume, expire).

Then:

1. **Update the model first**: the action that mirrors the changed code (keep the
   one-statement-or-transaction = one-action rule; a statement that runs after a commit is a
   *separate* action with its own phase).
2. If the change is a new mechanism or a fix, add a constant that switches it (as
   `CREDIT_IN_TXN`, `FENCED`, `CAP_CRASHES`, `RECLAIM_BUMPS` do), so one model holds both the old
   and the new behaviour, and add an instance for each.
3. Add the instances to `run.sh` with their **expected** outcome. A bug reproduction is
   `violation`; the fixed protocol is `ok`. Add a mutation check whenever you add a property:
   an instance that disables the mechanism the property depends on and must fail. A property no
   mutant can break is not checking anything.
4. Run `make formal` (or `make formal ARGS=tlc` while iterating) and get every row to `PASS`.
5. Add a regression test in `test/` for the code-level property the fix relies on (see
   `test/formal_findings_test.exs`), record the finding in `ISSUES.md`, and update `README.md`'s
   results table.

When a model finds a violation, read the counterexample (`scripts/trace.py` prints TLC traces
one state per line) and decide whether it is a protocol bug or a modelling artefact: a missing
guard in the model, a state the code cannot reach, an Apalache deadlock on a terminal state.
Confirm it against the code before reporting it. Never weaken an invariant just to make a run
green.

## Instance naming

- `shipped*`, `redis_shipped`, `retries_shipped`: the protocol **before 0.2.16**. Kept on purpose
  as regression demonstrations; they must keep reproducing their bug (`expect=violation`).
- `fixed*`, `redis_fixed*`, `retries_fixed`, `fix_*`: the current code. They must stay `ok`.
- `*_norec`, `*_cap_only`, `fix_credit_inline_pauses`: isolation experiments, showing which fix
  is needed for what.
- `gen_durable_proof` / `gen_durable_proof_shipped`: the inductive invariant of the fixed
  protocol, and its mutation check.

## Running

```bash
make formal-up            # build/start the container (Quint 0.34, Apalache 0.62.4, TLC, JDK 21)
make formal               # whole matrix, ~20 min; non-zero exit on any unexpected result
make formal ARGS=tlc      # TLC rows only (~12 min)
make formal ARGS=apalache # Apalache rows: inductive proof + bounded runs (~10 min)
```

One job, inside the container (`docker compose -p gen_durable -f .devcontainer/docker-compose.yml
--profile formal exec formal bash`):

```bash
scripts/check.sh fixed PauseSafety tlc        # one TLC job + compact trace on violation
quint run gen_durable.qnt --main=fixed --invariant=PauseSafety --max-samples=20000  # quick smoke
```

## Hard-won rules

- **One job at a time.** Quint drives a single Apalache server on port 8822, used by *both*
  backends (TLC goes through it to get the TLA+), and TLC plus Apalache together exceed the VM's
  8 GiB.
- **Keep scopes small; TLC has no depth limit.** Exhaustive TLC explores the whole graph, so
  scope (`ROWS`, `WORKERS`, `KEYS`, `MAX_CLAIMS`, `MAX_STEP`) is the only knob. Every scope
  increment is roughly 5–10× the states. One scheduler incarnation is the worst case for the
  ABA (#32), so do not add a second one "for coverage" to the pause instances. `run.sh` kills a
  job after `JOB_TIMEOUT` (1200 s) and reports `got=timeout`: shrink the scope; do not raise
  the cap.
- **`TRACE = false` on instances expected to pass.** The `lastAction` label is ghost state that
  multiplies the state count. Instances expected to fail keep it for readable traces.
- **Apalache bounded runs get expensive fast** (about 5× per two steps on `gen_durable.qnt`).
  For depth, use the inductive invariant (`gen_durable_proof.qnt`), not `--max-steps`. The
  inductive checks need `JVM_ARGS=-Xmx6g`, which `run.sh` sets; with less, the server dies with
  `Connection dropped`.
- **Apalache reports a state with no successor as a deadlock.** Every model has an explicit
  `idle` stutter; keep it in any new model. `run.sh` reports `got=deadlock`, never `violation`.
- **Inductive invariant changes:** keep the `Shape` generator (`x.in(...)`) separate from the
  core (`IndInvCore`), and check the core with `quint run` first: the simulator cannot evaluate
  `powerset` over a big set. If phase 2 fails, read the pre-state. Usually the invariant is too
  weak (it admits an unreachable state) rather than the protocol being wrong; strengthen it, and
  confirm with `quint run --invariant=IndInvCore` that it still holds on reachable states.
- `pkill -f` inside the container matches its own `bash -c` command line; use a bracket
  pattern such as `pkill -f "[a]palache.jar server"`.
