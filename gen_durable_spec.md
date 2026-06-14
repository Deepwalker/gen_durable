# gen_durable — durable FSM engine: specification and schema

Status: normative draft v1, plus §11 `schedule_childs` (schema version 2). Postgres-backed engine for long-running FSMs on top of GenServer.

## 1. Guarantee

The engine's only guarantee: **on step completion, the new state is committed to the database before execution proceeds.** On a crash before commit, the step re-executes from scratch (at-least-once). Idempotency of step effects is the user's responsibility.

The engine guarantees exactly-once of nothing: not delivery, not effects. `:replay`, retries, and failure policy are user decisions; the engine does not guess them.

The unit of re-execution is the **whole step**. Everything inside a step re-runs as one bundle on re-execution; the user makes the entire step idempotent, not individual effects. Hence the practice: keep steps small.

## 2. Three primitives

- **durable step** — user step code that returns an outcome on completion (see §3).
- **durable await** — a step parks the instance until a named signal arrives; the wake-up only changes state, with no side effects, so re-running it is harmless.
- **durable childs** — a step spawns a batch of child instances and parks until **all** of them reach a terminal status; the engine owns the fan-in barrier (see §11). The wake-up only reads child results — no side effects, so re-running it is harmless.

These compose into larger shapes in user code. The spawn-and-join shape is first-class: the engine tracks the `parent_id` link and the join barrier. A child is otherwise an ordinary independent instance — cancel and cascade are **not** implied (see §8, §11).

## 3. Step outcomes

A step, and the error handler `handle/2`, return one of:

| Outcome | Effect | `attempt` | `eligible_at` |
|---|---|---|---|
| `{:next, step, state}` | transition to `step`, `runnable` | `:= 0` | `now()` |
| `{:replay, state, delay_ms}` | same step again, `runnable` | `+1` | `now() + delay_ms` |
| `{:await, signal_name, state}` | park, `awaiting_signal`, `awaits := signal_name` | — | — |
| `{:schedule_childs, step, children, state}` | spawn `children`, park `awaiting_children`; on barrier release advance to `step`, `runnable` (see §11) | `:= 0` | `now()` on release |
| `{:done, result}` | terminal, `done`, `result` recorded | — | — |
| `{:stop, reason}` | terminal, `failed`, `last_error := reason` | — | — |

Backoff for `:replay` is computed by the user from `attempt` (available in the step context and in `handle/2`). The engine does not compute the delay.

## 4. Three sources of re-execution

Not to be conflated:

1. **explicit `:replay`** from a step — controlled retry;
2. **caught exception** → the engine calls `handle(reason, ctx)`, where `ctx = %{id, fsm, fsm_version, step, attempt, state}`; the handler returns any outcome from §3, including `:replay` or `:stop`. If `handle/2` itself raises — `failed`;
3. **worker crash** (no outcome returned, lease expired) → the reaper moves the row back to `runnable`, `attempt + 1`, and the step runs from scratch. `handle/2` is **not** called — this is the at-least-once safety floor, not an `:error`.

## 5. await and signals

Parking writes `awaits := signal_name`. Signal delivery moves the instance to `runnable` **only on a name match**; non-matching signals stay in the inbox until their own await.

A signal into an await must be durable (a row in `signals`), at-least-once. Loss is not allowed — otherwise the instance hangs forever; duplicates are allowed (caught by dedup or the step itself). Delivery inserts the signal row **before** flipping to `runnable`, so on wake-up the same step re-executes and already sees the signal in the inbox. The step reads and deletes consumed signals in the same transaction as its outcome.

## 6. Scheduler

The picker is dumb: it selects `runnable` rows whose `eligible_at` has arrived, ordered by `priority, eligible_at`, via `FOR UPDATE SKIP LOCKED`, and flips them to `executing` with a lease — a short transaction. The step runs outside the transaction. Completion writes via the §3 outcomes.

- **lease + reaper** — the worker holds a lease `lease_expires_at` and extends it with a heartbeat; the reaper returns `executing` rows with an expired lease to `runnable` (crash recovery, see §4.3).
- **queue** — logical load separation; a worker pool subscribes to its queues, local concurrency = pool size.
- **partition_key** — per-key serialization: the worker holds a session-level `pg_try_advisory_lock(hashtext(partition_key))` for the duration of the step; the picker skips locked keys. Parallelism across keys + strict ordering within a key. Worker death → connection dropped → advisory released automatically.

Plain `SKIP LOCKED` already provides concurrent executors with no coordination. Picker sharding (`hashtext(partition_key) % N`) is only worth it once head-of-queue contention shows up in a profile; it requires membership/rebalancing and works against the dumb-picker principle.

## 7. Uniqueness

Uniqueness = **key** (user function, stored verbatim, no transformation) + **scope** (the statuses in which the key is "occupied," set by the user per job).

`unique_guard` is a generated column: equal to the key while the current status is in the instance's `unique_scope`, otherwise NULL (drops out of the index). A single partial unique index on `unique_guard`. This delivers per-job scope **and** single-statement batch insert, unlike Oban's imperative check.

- per-job opt-in: `unique_key IS NULL` → not deduplicated (NULLs don't conflict in btree);
- the guard is recomputed on every status transition (free — the row is rewritten anyway);
- leaving the "occupied" statuses → the key is free for re-insertion;
- collision: a new job is rejected if a row with the same key currently sits in a status that **it** considers occupied;
- windowed uniqueness (Oban `period` style) — the user folds a coarse timestamp into the key, turning the window into key equality. No engine magic.

## 8. Non-goals (v1)

- **limited trees** — `schedule_childs` (§11) provides spawn + an all-children fan-in barrier, and the engine tracks the `parent_id` link. Still out: **cancel/cascade** (failing or terminating a parent does not touch its children, nor the reverse), quorum/`k`-of-`n` barriers, and arbitrary DAG joins beyond "all my children";
- **no cancel**;
- **no transition history / event sourcing** — current-state snapshot only;
- **the engine does not cap retries** — no `max_attempts`, only an `attempt` counter; `handle/2` decides when to stop by reading it;
- **no global cross-node concurrency limit** — local pools only;
- **no auto-migration of in-flight instances on FSM change** — old ones finish on their `fsm_version`;
- **GC of terminal rows and the inbox** — not the engine's job, an external cron.

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
  awaits        text,                               -- awaited signal when awaiting_signal

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

  -- uniqueness
  unique_key    bytea,                              -- user function result, verbatim
  unique_scope  durable_status[] not null default '{}', -- statuses in which the key is "occupied"
  unique_guard  bytea generated always as (
    case when unique_key is not null and status = any(unique_scope)
         then unique_key end
  ) stored,                                         -- NB: scope is durable_status[], not text[] —
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

-- uniqueness among "occupied" statuses, per-job scope
create unique index gen_durable_unique on gen_durable (unique_guard)
  where unique_guard is not null;

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

### Step outcomes (one transaction per outcome; consumed signals are deleted here too)

```sql
-- :next
update gen_durable
set step = $next, state = $state, status = 'runnable', eligible_at = now(),
    attempt = 0, locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :replay
update gen_durable
set state = $state, status = 'runnable', eligible_at = now() + $delay_ms,
    attempt = attempt + 1, locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :await
update gen_durable
set state = $state, status = 'awaiting_signal', awaits = $signal_name,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :schedule_childs  (one transaction; spawn the batch, then park on the join barrier)
with ins as (
  insert into gen_durable
    (fsm, step, state, queue, priority, partition_key,
     unique_key, unique_scope, eligible_at, parent_id)
  values
    (...), (...)                                      -- one row per child, parent_id = $id
  on conflict (unique_guard) where unique_guard is not null do nothing
  returning 1
)
update gen_durable
set step = $step, state = $state,
    children_pending = (select count(*) from ins),
    -- zero children actually inserted ⇒ barrier already satisfied ⇒ run next_step now
    status = case when (select count(*) from ins) = 0 then 'runnable' else 'awaiting_children' end,
    eligible_at = now(), attempt = 0,
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :done
update gen_durable
set state = $state, result = $result, status = 'done',
    locked_by = null, lease_expires_at = null, updated_at = now()
where id = $id;

-- :stop
update gen_durable
set status = 'failed', last_error = $reason,
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

```sql
insert into signals (target_id, name, payload, dedup_key)
values ($id, $name, $payload, $dedup) on conflict (target_id, dedup_key) do nothing;

update gen_durable
set status = 'runnable', eligible_at = now(), awaits = null, updated_at = now()
where id = $id and status = 'awaiting_signal' and awaits = $name;
```

### Batch insert with uniqueness

```sql
insert into gen_durable
  (fsm, step, state, queue, priority, partition_key, unique_key, unique_scope, eligible_at)
values
  (...), (...), (...)
on conflict (unique_guard) where unique_guard is not null
do nothing
returning id;
```

One index catches duplicates both against existing rows and within the batch itself. Concurrent inserts are handled by the constraint.

---

## 11. Children: `schedule_childs` (fan-out + fan-in)

`schedule_childs` is one outcome that does two things atomically: it spawns a batch of child instances and parks the parent on a **join barrier** that releases only when every child has reached a terminal status (`done` **or** `failed`). It is the engine's answer to "fan work out, then wait for all of it." Schema version 2 (`awaiting_children`, `parent_id`, `children_pending`, `gen_durable_parent`) backs it.

**Shape.** `{:schedule_childs, next_step, children, state}`. `children` is a list of child specs, each an ordinary insert: `fsm`, `version`, `step`, `state`, `queue`, `priority`, `partition_key`, `unique_key`, `unique_scope`, `eligible_at`. The engine stamps every child's `parent_id` with the parent's id. It is shaped like `:next` — the parent advances to `next_step` — but the transition is gated behind the barrier, so the parent re-runs at `next_step` (never at the spawning step), and there is no "re-run re-spawns" ambiguity.

**Atomic spawn + park.** The whole thing is the parent's step-completion transaction (§10): the children are inserted **and** the parent flips to `awaiting_children` in one commit. Children become visible (`runnable`) only once that commits — a child can never finish and try to release a barrier that does not yet exist. `children_pending` is set to the number of children **actually inserted** (per-child uniqueness may drop some); if zero are inserted the barrier is pre-satisfied and the parent goes straight to `next_step`, `runnable`.

**The join.** When a child reaches a terminal status, its own outcome transaction — the same one that writes `done`/`failed` — decrements the parent's `children_pending` (§10). Because the decrement rides the child's terminal commit, it happens **exactly once per child** even under at-least-once re-execution: a child that crashes before committing simply re-runs and never decremented. The decrement that drives `children_pending` to zero, while the parent is still `awaiting_children`, flips the parent to `runnable` at `next_step`. Concurrent siblings serialize on the parent's row lock.

**Wake-up.** The parent re-runs `next_step` with its children available as `ctx.childs` — `[%{id, fsm, status, state, result, last_error}]`, one entry per child of this parent. The parent aggregates and returns a normal outcome. Children are **not** deleted on read (unlike consumed signals); they stay as terminal rows, GC'd externally like any other terminal row (§8). Re-execution of `next_step` (a crash) reads the same children and is idempotent if the aggregation is.

**Failures.** The barrier waits for *termination*, not success: a child that ends `failed` still releases its slot. The parent sees `status: "failed"` + `last_error` in `ctx.childs` and decides (proceed, `:stop`, compensate). The engine does not auto-propagate child failure.

**Not covered.** No cancel/cascade — terminating or failing a parent does not touch its children, and vice-versa. No quorum / `k`-of-`n` barrier — the join is all-or-nothing; quorum is the user's to build with `await` + per-child signals. No barrier deadline — a child that hangs forever hangs the parent; bound it inside the child (e.g. `:replay` with a timeout). Nesting is free: a child may itself `schedule_childs`, each level an independent barrier.
