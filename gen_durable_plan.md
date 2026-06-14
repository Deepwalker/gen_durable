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
| Signal consume | **Engine auto-deletes** the snapshotted signal ids on the outcome txn (**DECIDED**) | Outcomes (§3) carry no signal info; engine snapshots the ids it loaded into `ctx` and deletes exactly those. Signals arriving mid-step survive. |
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
The five "complete_*" run the outcome `UPDATE` **and** the consumed-signal `DELETE` in one txn.

### 5.2 Executor (`executor.ex`)
Given a picked row map, run the step to a committed outcome:
1. If `partition_key`, `pg_try_advisory_lock(hashtext(partition_key))` on the step connection; on
   `false`, reset row to `runnable` (drop lease) and stop — another worker owns the key.
2. Load pending signals for `id` (snapshot their ids — we delete *exactly these* on commit, so
   signals arriving mid-step aren't lost).
3. Resolve module via `Registry.fetch!({fsm, fsm_version})`; **load jsonb → state struct** (the FSM's
   `state:` embedded schema, via `Ecto.embedded_load`); build `Context` with the struct.
4. `try` → `module.step(step, ctx)`; on raised exception → `module.handle(reason, ctx)`; if
   `handle/2` itself raises → write `:stop`/`failed` (spec §4.2).
5. **Dump the returned state struct → jsonb** (`Ecto.embedded_dump`), then apply the outcome via the
   matching `complete_*` query, deleting the snapshotted consumed signals in the same txn.
6. `pg_advisory_unlock`, stop heartbeat.

A worker process crash before step 5 means *no outcome row* → reaper path. We do **not** trap and
convert crashes into `handle/2`; crash ≠ caught exception (spec §4 distinguishes the three sources).

### 5.3 Scheduler (`scheduler.ex`)
Per queue. Tracks free slots = `pool_size − in_flight`. On tick (interval, default ~`lease_ttl/10`,
**CONFIRM**) and when slots free up: run `pick(queues, free_slots)`, spawn one `Task` per returned row
under the queue's `Task.Supervisor`, decrement slots; on Task completion (any reason) increment slots.
Demand-driven so we never pick more than we can run.

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

### Open follow-ups (post-v1, not blocking)
- **`schedule_childs` (spec §11) — specified, not yet implemented.** Schema version 2: enum value
  `awaiting_children`, columns `parent_id` / `children_pending`, index `gen_durable_parent`. Work:
  `Migration.change(2, …)`; a `{:schedule_childs, next_step, children, state}` outcome (batch-insert
  children + park in one txn); a child→parent join decrement appended to the `:done`/`:stop`
  transactions; load `ctx.childs` on wake; tests for the all-children barrier (incl. a `failed` child
  still releasing its slot, and zero-children → immediate `next_step`).
- **D2:** spec §10 `:done` update lists `state = $state`, but the `{:done, result}` outcome carries no
  state. The engine writes `result` only and leaves `state` as-is. Worth reconciling in the spec.
- Signal-consumption edge: on a *progressing* outcome the engine deletes the whole inbox snapshot; a
  non-matching signal present at that moment is dropped. Fine for one-await-one-signal (the common
  case); revisit if multi-name inboxes become real (spec §5 wants non-matching signals to persist).
- Always-load-signals: every step does one `signals_target` SELECT. Cheap, but optimizable later.

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
6. **Signals:** engine auto-deletes the snapshotted consumed signal ids in the outcome txn.

## 9. Risks / watch-items

- **Connection pressure from partitioned steps** (§0): each in-flight partitioned step holds a
  connection for its whole duration. Pool sizing must account for it.
- **ON CONFLICT inference on the generated column** must match the partial index predicate exactly.
- **Heartbeat vs lease race:** if heartbeat starves (slow DB), a live step can be reaped and run
  twice. The TTL margin and the at-least-once contract make this safe, but tune the margin.
- **`handle/2` re-entrancy:** a caught exception calls `handle/2`, which may `:replay`; ensure the
  re-run path is identical to a fresh run (no leftover lease/lock state).
