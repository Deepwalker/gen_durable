# CLAUDE.md

Working notes for AI agents (and humans) on this repo. Keep it current — see
[Documentation](#documentation).

## What this is

`gen_durable` — a Postgres-backed durable-execution engine for Elixir. You declare an FSM;
the engine commits its state to Postgres **before each step proceeds**, so an instance
survives process and node death and resumes where it left off. There is **no GenServer per
instance**: an FSM is a row, each step runs as an ephemeral `Task`, and a small set of
GenServers (scheduler, reaper, GC) pick runnable rows and dispatch them.

**The one guarantee:** on step completion the new state is committed before execution
proceeds; a crash before commit re-runs the step (at-least-once). Idempotency of step
effects is the user's responsibility.

The goal is a **small, correct, fast** engine. Correctness first (the durability guarantee,
concurrency/rate invariants, crash recovery), then keep the hot path cheap. Prefer one
static SQL statement over a round-trip chain; prefer self-healing over bookkeeping.

## Running tests

**Everything runs in the devcontainer** (Elixir 1.18 / OTP 27 + Postgres 17 + Redis 7,
pinned in `.devcontainer/`). The host has no database — do **not** run `mix test` directly
on the host; it will fail to connect.

```bash
make up     # build + start the app/db/redis containers (once)
make test   # mix deps.get && mix test, inside the container
```

`make test` is `docker compose -p gen_durable -f .devcontainer/docker-compose.yml exec -T app
sh -lc "mix deps.get && mix test"`. To run a subset, invoke `mix test` in the container
directly:

```bash
docker compose -p gen_durable -f .devcontainer/docker-compose.yml exec -T app \
  sh -lc "mix test test/queries_test.exs"
```

- `test/test_helper.exs` drops and recreates the test DB each run, then migrates it — every
  run starts from a pristine schema.
- Redis is available in the compose (`REDIS_URL=redis://redis:6379`), used by the
  `{:redis, _}` poke transport tests and the Redis limiter backend.
- The `:bench` tag is **excluded by default**; run perf benchmarks with
  `mix test --only bench` (see below).

Get a green baseline before changing anything, and re-run the full suite after each
increment — the suite is fast (~13 s) and the feedback loop is the point. When behaviour
legitimately changes, update the test to assert the *new* contract (and say why in the
diff), don't weaken it.

## Performance

Performance is a tested, documented contract, not a vibe:

- `test/perf_test.exs` asserts **statement / round-trip counts** for the hot path (e.g. an
  empty pick is one statement; a batch pick is claim + two batched enrich `SELECT`s; each
  outcome is one guarded statement). These catch a round-trip regression the moment it lands.
- `PERFORMANCE.md` documents the cost model, the picker, and EXPLAIN plans.
- **Any change to the hot path** (the pick / admission / limiter / outcomes / signals) must:
  run the perf tests, re-check the statement and round-trip counts, run the `:bench` suite
  if the plan could shift, and **update `PERFORMANCE.md`** if the cost model moved. A change
  that adds a round-trip must justify it in the changelog (e.g. out-of-band admission trades
  the fused claim's lock for an extra statement — that's a documented, deliberate tradeoff).

Never regress the hot path silently. If a statement count changes, the perf test should
change with it, in the same commit, with a note.

## Migrations & schema versions

The DDL lives in the library (`GenDurable.Migration`), and the live schema version is the
`COMMENT ON TABLE gen_durable IS '<n>'`. `GenDurable.Migration.up/1` applies only the
increments an install is missing.

**There is a live install, so schema changes ship as versioned increments — never edit
shipped DDL in place.** Freeze the already-shipped `change/N`, add a new `change(N+1)` for
the delta, and bump the schema-version comment. Editing a shipped `change` would silently
diverge existing installs from new ones. (Reusing existing tables/columns without any DDL —
as the out-of-band limiter did — needs no migration at all; confirm with
`git diff lib/gen_durable/migration.ex` being empty.)

## Versioning & changelog

- **Pre-1.0: stay on the `0.2.x` line.** Every shippable change increments the `mix.exs`
  patch version (`0.2.9`, `0.2.10`, …). Do **not** jump to `0.3.x` — patch increments are
  the convention here, no backward-compat guarantees are made.
- Every version bump adds a `CHANGELOG.md` entry (Keep a Changelog format): what changed,
  and for a behavioural/hot-path change, the tradeoff and why.

## Documentation

Docs are part of the change, not a follow-up:

- **Source of truth is `guides/*.md`** (jobs, machines, signals, children, rate_limiting,
  concurrency, identity, scheduling, testing, operations, internals) plus `README.md`,
  `PERFORMANCE.md`, `CHANGELOG.md`, `ISSUES.md`. `doc/` is **generated** by ExDoc
  (`make docs`) — never hand-edit it.
- `guides/internals.md` describes the actual mechanics (the pick, outcomes, locking,
  self-heal). When you change how something works, update the guide **in the same change** —
  a guide that describes a removed mechanism (e.g. a fused pick, a rider that's now
  out-of-band) is worse than no guide.
- Grep the guides and `PERFORMANCE.md` for the thing you changed (`credit_gate`, `@pick_sql`,
  "one statement", CTE names) and fix every now-false claim.
- **Keep this file (`CLAUDE.md`) current too:** when the test command, toolchain, versioning
  rule, or these conventions change, update it here.

## Design review findings (`ISSUES.md`)

`ISSUES.md` is the running ledger of **design-review and adversarial-review findings** —
correctness, performance, and processing-organization issues, ordered by severity, each with
an explicit **status**: `FIXED` (with the fix and how it was verified), `DOCUMENTED` (a known
limitation written into `PERFORMANCE.md`/a guide, cure deferred until a real workload demands
it), `AUDITED` (investigated, no violation reproduced, with a re-runnable tripwire recipe), or
`NOTED` (real but deliberately not taken, with the reason). It is the memory of *why* the
engine is shaped the way it is — read the relevant entry before touching a subsystem, because
many non-obvious mechanisms (the K=1 unique arbiter, ordered `SKIP LOCKED`, zero-lag cold
mint, the ownership guard) exist to close a specific race documented here.

When an adversarial review (or your own analysis) turns up something:

- **Fixed it?** Add an entry: original finding, the fix, and how you verified (test, EXPLAIN,
  bench, conservation run).
- **Not fixing it now?** Still record it — `DOCUMENTED`/`NOTED` — with the tradeoff and the
  triggering condition, so it isn't rediscovered from scratch. A saturated-limit starvation
  or a churn regression belongs here even if the cure waits.
- Cross-link: a documented limitation goes into `PERFORMANCE.md` §6 (pathologies) or the
  relevant guide too, and a hot-path finding that changes behaviour rides the `CHANGELOG.md`
  entry.

Don't let a review's findings evaporate into a chat message — the ledger is where they
survive the session.
