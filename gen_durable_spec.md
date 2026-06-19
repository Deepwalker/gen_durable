# gen_durable — durable FSM engine: specification and schema

Status: normative draft v1, plus §11 `schedule_childs` (schema version 2). Postgres-backed durable-execution engine for long-running FSMs — an FSM is a row, not a process; the runtime backbone is a small set of GenServers.

## 1. Guarantee

The engine's only guarantee: **on step completion, the new state is committed to the database before execution proceeds.** On a crash before commit, the step re-executes from scratch (at-least-once). Idempotency of step effects is the user's responsibility.

The engine guarantees exactly-once of nothing: not delivery, not effects. `:retry`, retries, and failure policy are user decisions; the engine does not guess them.

The unit of re-execution is the **whole step**. Everything inside a step re-runs as one bundle on re-execution; the user makes the entire step idempotent, not individual effects. Hence the practice: keep steps small.

## 2. Three primitives

- **durable step** — user step code that returns an outcome on completion (see §3).
- **durable await** — a step parks the instance until a signal from a named **set** arrives, naming the step to run next; on arrival the engine runs that step with the matched signals as `ctx.awaited` (full inbox in `ctx.all`). It is sugar over a step that inspects the inbox itself — the engine pre-filters the awaited set and consumes it. The wake-up only changes state, with no side effects, so re-running it is harmless.
- **durable childs** — a step spawns a batch of child instances and parks until **all** of them reach a terminal status; the engine owns the fan-in barrier (see §11). The wake-up only reads child results — no side effects, so re-running it is harmless.

These compose into larger shapes in user code. The spawn-and-join shape is first-class: the engine tracks the `parent_id` link and the join barrier. A child is otherwise an ordinary independent instance — cancel and cascade are **not** implied (see §8, §11).

## 3. Step outcomes

A step, and the error handler `handle/2`, return one of:

| Outcome | Effect | `attempt` | `eligible_at` |
|---|---|---|---|
| `{:next, step, state}` | transition to `step`, `runnable` | `:= 0` | `now()` |
| `{:retry, state, delay_ms}` | same step again, `runnable` | `+1` | `now() + delay_ms` |
| `{:await, names, next_step, state}` | park, `awaiting_signal`, `awaits := names` (one name or a set), `step := next_step`; on any match, run `next_step` with `ctx.awaited` | — | — |
| `{:schedule_childs, step, children, state}` | spawn `children`, park `awaiting_children`; on barrier release advance to `step`, `runnable` (see §11) | `:= 0` | `now()` on release |
| `{:done, result}` | terminal, `done`, `result` recorded | — | — |
| `{:stop, reason}` | terminal, `failed`, `last_error := reason` | — | — |

Backoff for `:retry` is computed by the user from `attempt` (available in the step context and in `handle/2`). The engine does not compute the delay.

## 4. Three sources of re-execution

Not to be conflated:

1. **explicit `:retry`** from a step — controlled retry;
2. **caught exception** → the engine calls `handle(reason, ctx)`, where `ctx = %{id, fsm, fsm_version, step, attempt, state}`; the handler returns any outcome from §3, including `:retry` or `:stop`. If `handle/2` itself raises — `failed`;
3. **worker crash** (no outcome returned, lease expired) → the reaper moves the row back to `runnable`, `attempt + 1`, and the step runs from scratch. `handle/2` is **not** called — this is the at-least-once safety floor, not an `:error`.

## 5. await and signals

`await` is sugar over the raw signal model. The primitives: every instance has a durable **inbox** (`signals` rows), always fully visible to a step as `ctx.all`; and a step can **park** on a set of names (`awaits`, a `text[]`) until any of them arrives. `{:await, names, next_step, state}` writes `awaits := names` and `step := next_step`. Delivery moves the instance to `runnable` **only on a name match** (`$name = ANY(awaits)`); non-matching signals stay in the inbox. On wake-up the engine runs `next_step`, handing it the matched subset as `ctx.awaited` (and the whole inbox as `ctx.all`).

A signal into an await must be durable (a row in `signals`), at-least-once. Loss is not allowed — otherwise the instance hangs forever; duplicates are allowed (caught by dedup or the step itself). Delivery inserts the signal row **before** flipping to `runnable`, so on wake-up `next_step` already sees it in the inbox.

**Consumption is by received id, on progress.** `await` itself deletes nothing. When the woken step *progresses* (`:next`/`:schedule_childs`), its outcome transaction deletes exactly the signal ids it received in `ctx.awaited` — **not** by name. So a signal of an awaited name that arrives *after* the step's snapshot (which the step never saw) is not deleted, and never-awaited signals are never touched. This also lets a step **accumulate a pack**: re-await (which consumes nothing) until `ctx.awaited` holds the whole set, then progress once, consuming it all. `:retry` consumes nothing and keeps `awaits` (the redo must see the same inputs). A **terminal** outcome (`:done`/`:stop`) deletes the *whole* inbox — the instance is finished, so nothing will read its signals again (cleanup).

**No lost wake-up.** A signal can arrive before the step has parked (delivery sees the instance still `executing`, so the flip is a no-op but the row is inserted). To avoid a hang, parking is guarded: `:await` commits as `runnable` (at `next_step`) instead of `awaiting_signal` when a matching signal is already in the inbox, so `next_step` runs at once. Combined with the row lock, every interleaving of deliver vs. park ends with the instance either flipped or already runnable.

**Addressing.** A signal targets an instance by its internal `id`, or by an optional **`correlation_key`** — a business key (e.g. `"order:42"`) stamped at insert. The same key is the engine's uniqueness guard (§7): it resolves to the single instance currently *occupying* it (per the key's `:unique` policy), via the partial unique index on `correlation_guard` (`correlation_guard = $key`). Addressing a key that no instance occupies is a no-op error (`{:error, :no_target}`) — a freed/terminal key can no longer be woken, and the engine does **not** durably hold a signal for an instance that does not exist yet. The internal-id path trusts the FK. This removes the caller's need to map its own business key to the engine's `id`.

## 6. Scheduler

The picker is dumb: it selects `runnable` rows whose `eligible_at` has arrived, ordered by `priority, eligible_at`, via `FOR UPDATE SKIP LOCKED`, and flips them to `executing` with a lease — a short transaction. The step runs outside the transaction. Completion writes via the §3 outcomes.

- **lease + reaper** — the worker holds a lease `lease_expires_at` and extends it with a heartbeat; the reaper returns `executing` rows with an expired lease to `runnable` (crash recovery, see §4.3).
- **queue** — logical load separation; a worker pool subscribes to its queues, local concurrency = pool size.
- **partition_key** — per-key serialization: the worker holds a session-level `pg_try_advisory_lock(hashtext(partition_key))` for the duration of the step; the picker skips locked keys. Parallelism across keys + strict ordering within a key. Worker death → connection dropped → advisory released automatically.

Plain `SKIP LOCKED` already provides concurrent executors with no coordination. Picker sharding (`hashtext(partition_key) % N`) is only worth it once head-of-queue contention shows up in a profile; it requires membership/rebalancing and works against the dumb-picker principle.

## 7. Correlation key: identity = addressing + uniqueness

**One concept** serves both signal addressing (§5) and uniqueness: a **`correlation_key`** (the instance's business identity) + a **scope** (the statuses in which the key is "occupied"). This is the durable-execution model — Temporal's Workflow ID, DBOS's workflow ID: the business key you assign is *both* how you find/signal the instance *and* how the engine deduplicates it; the internal `id` is the per-execution handle (≈ Temporal's Run ID). (Job queues like Oban have the uniqueness half but no addressing — instances aren't signalable; we are a durable-execution engine, so the two are the same key.)

`correlation_guard` is a generated column: equal to the key while the current status is in the instance's `correlation_scope`, otherwise NULL (drops out of the index). A single partial unique index on `correlation_guard` does double duty — it enforces uniqueness **and** resolves the signal address. Per-key scope **and** single-statement batch insert, unlike Oban's imperative check.

The user-facing surface is the **`:unique` policy**, which expands to a scope:

- `:live` (default) — occupied in the non-terminal statuses; a terminal instance **frees** the key for reuse;
- `:global` — occupied in every status; the key is **never** reused (until GC removes the terminal row).

Properties:

- per-instance opt-in: `correlation_key IS NULL` → neither addressable nor deduplicated (NULLs don't conflict in btree);
- the guard is recomputed on every status transition (free — the row is rewritten anyway);
- leaving the occupied statuses (`:live` on termination) → the key is free for re-insertion;
- collision: a new instance is rejected (`{:error, :duplicate}`) if a row with the same key currently sits in a status the policy considers occupied;
- windowed uniqueness (Oban `period` style) — fold a coarse timestamp into the key, turning the window into key equality. No engine magic.

`correlation_scope` is stored as `durable_status[]` (not `text[]`) so the generated guard stays IMMUTABLE — the enum→text cast (`enum_out`) is only STABLE. The `:unique` policy is the public surface; the raw scope is an implementation detail.

## 8. Non-goals (v1)

- **limited trees** — `schedule_childs` (§11) provides spawn + an all-children fan-in barrier, and the engine tracks the `parent_id` link. Still out: **cancel/cascade** (failing or terminating a parent does not touch its children, nor the reverse), quorum/`k`-of-`n` barriers, and arbitrary DAG joins beyond "all my children";
- **no cancel**;
- **no transition history / event sourcing** — current-state snapshot only;
- **the FSM `step/2` path does not cap retries** — no `max_attempts`, only an `attempt` counter; `handle/2` decides when to stop by reading it (the job `perform` form *does* cap, via `:max_attempts`);
- **no global cross-node concurrency limit** — local pools only;
- **no auto-migration of in-flight instances on FSM change** — old ones finish on their `fsm_version`.

GC of terminal rows is **built in**: a periodic sweep deletes `done`/`failed` rows whose termination is older than `:gc_retention` (default 1 day), sparing a terminal child still needed for a parent's join. Disable it with `gc_interval: nil` to prune externally instead. The inbox is cleaned on terminal outcomes (§5) and cascades on row delete.

---

## 9. DDL

```sql
create type durable_status as enum
  ('runnable', 'executing', 'awaiting_signal', 'awaiting_children', 'done', 'failed');

create table gen_durable (
  id            bigint generated always as identity primary key,
  fsm           text     not null,                 -- machine definition name
  fsm_version   int      not null default 1,
  step          text     not null,                 -- current step
  status        durable_status not null default 'runnable',
  state         jsonb    not null default '{}',
  result        jsonb,                              -- set on :done
  awaits        text[],                             -- awaited signal name set when awaiting_signal

  -- scheduling
  queue         text     not null default 'default',
  priority      smallint not null default 0,        -- lower = earlier
  partition_key text,                               -- serialization key (advisory lock)
  eligible_at   timestamptz not null default now(),
  attempt       int      not null default 0,        -- attempts of the CURRENT step; reset on :next
  last_error    text,

  -- lease
  locked_by        text,
  lease_expires_at timestamptz,

  -- children (schedule_childs / fan-in barrier, §11)
  parent_id        bigint references gen_durable(id) on delete set null,
  children_pending int not null default 0,           -- non-terminal children left to join on

  -- correlation key: business identity = signal address (§5) + uniqueness guard (§7)
  correlation_key   text,                            -- the key the user assigns (e.g. "order:42")
  correlation_scope durable_status[] not null default '{}', -- statuses in which the key is "occupied"
  correlation_guard text generated always as (       -- = key while occupied, else NULL
    case when correlation_key is not null and status = any(correlation_scope)
         then correlation_key end
  ) stored,                                          -- NB: scope is durable_status[], not text[] —
                                                     -- a generated column must be IMMUTABLE, and the
                                                     -- enum->text cast (enum_out) is only STABLE.

  inserted_at   timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

-- picker hot path
create index gen_durable_pick on gen_durable (queue, priority, eligible_at)
  where status = 'runnable';

-- reaper over expired leases
create index gen_durable_lease on gen_durable (lease_expires_at)
  where status = 'executing';

-- one index, two jobs: uniqueness among "occupied" statuses (§7) AND the signal
-- address lookup `correlation_guard = $key` (§5)
create unique index gen_durable_correlation on gen_durable (correlation_guard)
  where correlation_guard is not null;

-- fan-in barrier: a parent's children, and the join decrement (§11)
create index gen_durable_parent on gen_durable (parent_id)
  where parent_id is not null;

create table signals (
  id          bigint generated always as identity primary key,
  target_id   bigint not null references gen_durable(id) on delete cascade,
  name        text   not null,
  payload     jsonb  not null default '{}',
  dedup_key   text,                                 -- null = no dedup; key supplied by user
  inserted_at timestamptz not null default now(),
  unique (target_id, dedup_key)
);

create index signals_target on signals (target_id, name);
```

---

## 10. Operations

### Pick (short transaction)

```sql
with picked as (
  select id from gen_durable
  where status = 'runnable' and eligible_at <= now()
    and queue = any($queues)
  order by priority, eligible_at
  for update skip locked
  limit $batch
)
update gen_durable g
set status = 'executing', locked_by = $worker,
    lease_expires_at = now() + $lease_ttl, updated_at = now()
from picked where g.id = picked.id
returning g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.partition_key;
```

Then, if `partition_key` is not NULL: `pg_try_advisory_lock(hashtext(partition_key))`; on failure, return the row to `runnable` (drop the lease) and skip it.

### Lease heartbeat

```sql
update gen_durable set lease_expires_at = now() + $lease_ttl
where id = $id and locked_by = $worker;
```

### Reaper

```sql
update gen_durable
set status = 'runnable', locked_by = null, lease_expires_at = null,
    attempt = attempt + 1, updated_at = now()
where status = 'executing' and lease_expires_at < now();
```

### Step outcomes (one transaction per outcome)

Consume rules (§5): `:next`/`:schedule_childs` delete exactly the awaited ids the step received
(`$consumed`, a `bigint[]`); `:done`/`:stop` delete the whole inbox (cleanup); `:retry`/`:await`
consume nothing. The consume rides each statement as a leading data-modifying CTE.

```sql
-- consume on a PROGRESSING outcome (:next / :schedule_childs): exactly the received ids
delete from signals where target_id = $id and id = any($consumed);  -- empty array ⇒ no-op

-- :next
update gen_durable
set step = $next, state = $state, status = 'runnable', eligible_at = now(),
    attempt = 0, awaits = null, locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :retry  (redo the same step — consumes nothing, KEEPS awaits so the redo sees the same inputs)
update gen_durable
set state = $state, status = 'runnable', eligible_at = now() + $delay_ms,
    attempt = attempt + 1, locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :await  (park at next_step on a name set; but go straight to runnable if a match already arrived)
update gen_durable
set step = $next_step, state = $state, awaits = $names::text[], eligible_at = now(),
    status = case when exists (select 1 from signals where target_id = $id and name = any($names::text[]))
                  then 'runnable' else 'awaiting_signal' end,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :schedule_childs  (one transaction; consume received ids, spawn the batch, then park on the join barrier)
with ins as (
  insert into gen_durable
    (fsm, step, state, queue, priority, partition_key,
     eligible_at, correlation_key, correlation_scope, parent_id)
  values
    (...), (...)                                      -- one row per child, parent_id = $id
  on conflict (correlation_guard) where correlation_guard is not null do nothing
  returning 1
)
update gen_durable
set step = $step, state = $state,
    children_pending = (select count(*) from ins),
    -- zero children actually inserted ⇒ barrier already satisfied ⇒ run next_step now
    status = case when (select count(*) from ins) = 0 then 'runnable' else 'awaiting_children' end,
    eligible_at = now(), attempt = 0, awaits = null,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- terminal cleanup: prelude to :done and :stop — the instance is finished, drop its whole inbox
delete from signals where target_id = $id;

-- :done  (state is not rewritten — the {:done, result} outcome carries no state)
update gen_durable
set result = $result, status = 'done', awaits = null,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :stop
update gen_durable
set status = 'failed', last_error = $reason, awaits = null,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- child → parent join: appended to :done and :stop, in the SAME transaction.
-- No-op when parent_id is null (the join yields no parent row). The decrement
-- that drives children_pending to zero releases the barrier. Concurrent siblings
-- serialize on the parent row lock.
update gen_durable p
set children_pending = p.children_pending - 1,
    status      = case when p.children_pending - 1 <= 0 and p.status = 'awaiting_children'
                       then 'runnable' else p.status end,
    eligible_at = case when p.children_pending - 1 <= 0 and p.status = 'awaiting_children'
                       then now() else p.eligible_at end,
    updated_at = now()
from gen_durable c
where c.id = $id and c.parent_id = p.id;
```

### Signal delivery (sender side, one transaction)

`$id` is the internal id, or resolved from a `correlation_key` first (§5): `select id from gen_durable where correlation_guard = $key` (the guard is NULL unless the key is occupied) — no row ⇒ `{:error, :no_target}`, no insert.

```sql
insert into signals (target_id, name, payload, dedup_key)
values ($id, $name, $payload, $dedup) on conflict (target_id, dedup_key) do nothing;

-- flip to runnable but KEEP awaits: the woken step consumes by received id on its outcome (§5)
update gen_durable
set status = 'runnable', eligible_at = now(), updated_at = now()
where id = $id and status = 'awaiting_signal' and $name = any(awaits);
```

### Batch insert with uniqueness

```sql
insert into gen_durable
  (fsm, step, state, queue, priority, partition_key, eligible_at, correlation_key, correlation_scope)
values
  (...), (...), (...)
on conflict (correlation_guard) where correlation_guard is not null
do nothing
returning id;
```

One index catches duplicates both against existing rows and within the batch itself. Concurrent inserts are handled by the constraint.

---

## 11. Children: `schedule_childs` (fan-out + fan-in)

`schedule_childs` is one outcome that does two things atomically: it spawns a batch of child instances and parks the parent on a **join barrier** that releases only when every child has reached a terminal status (`done` **or** `failed`). It is the engine's answer to "fan work out, then wait for all of it." Schema version 2 (`awaiting_children`, `parent_id`, `children_pending`, `gen_durable_parent`) backs it.

**Shape.** `{:schedule_childs, next_step, children, state}`. `children` is a list of child specs, each an ordinary insert: `fsm`, `version`, `step`, `state`, `queue`, `priority`, `partition_key`, `correlation_key`, `:unique`, `eligible_at`. The engine stamps every child's `parent_id` with the parent's id. It is shaped like `:next` — the parent advances to `next_step` — but the transition is gated behind the barrier, so the parent re-runs at `next_step` (never at the spawning step), and there is no "re-run re-spawns" ambiguity.

**Atomic spawn + park.** The whole thing is the parent's step-completion transaction (§10): the children are inserted **and** the parent flips to `awaiting_children` in one commit. Children become visible (`runnable`) only once that commits — a child can never finish and try to release a barrier that does not yet exist. `children_pending` is set to the number of children **actually inserted** (per-child uniqueness may drop some); if zero are inserted the barrier is pre-satisfied and the parent goes straight to `next_step`, `runnable`.

**The join.** When a child reaches a terminal status, its own outcome transaction — the same one that writes `done`/`failed` — decrements the parent's `children_pending` (§10). Because the decrement rides the child's terminal commit, it happens **exactly once per child** even under at-least-once re-execution: a child that crashes before committing simply re-runs and never decremented. The decrement that drives `children_pending` to zero, while the parent is still `awaiting_children`, flips the parent to `runnable` at `next_step`. Concurrent siblings serialize on the parent's row lock.

**Wake-up.** The parent re-runs `next_step` with its children available as `ctx.childs` — `[%{id, fsm, status, state, result, last_error}]`, one entry per child of this parent. The parent aggregates and returns a normal outcome. Children are **not** deleted on read (unlike consumed signals); they stay as terminal rows, GC'd externally like any other terminal row (§8). Re-execution of `next_step` (a crash) reads the same children and is idempotent if the aggregation is.

**Failures.** The barrier waits for *termination*, not success: a child that ends `failed` still releases its slot. The parent sees `status: "failed"` + `last_error` in `ctx.childs` and decides (proceed, `:stop`, compensate). The engine does not auto-propagate child failure.

**Not covered.** No cancel/cascade — terminating or failing a parent does not touch its children, and vice-versa. No quorum / `k`-of-`n` barrier — the join is all-or-nothing; quorum is the user's to build with `await` + per-child signals. No barrier deadline — a child that hangs forever hangs the parent; bound it inside the child (e.g. `:retry` with a timeout). Nesting is free: a child may itself `schedule_childs`, each level an independent barrier.
