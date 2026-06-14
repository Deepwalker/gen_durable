# gen_durable — durable FSM engine: specification and schema

Status: normative draft v1. Postgres-backed engine for long-running FSMs on top of GenServer.

## 1. Guarantee

The engine's only guarantee: **on step completion, the new state is committed to the database before execution proceeds.** On a crash before commit, the step re-executes from scratch (at-least-once). Idempotency of step effects is the user's responsibility.

The engine guarantees exactly-once of nothing: not delivery, not effects. `:replay`, retries, and failure policy are user decisions; the engine does not guess them.

The unit of re-execution is the **whole step**. Everything inside a step re-runs as one bundle on re-execution; the user makes the entire step idempotent, not individual effects. Hence the practice: keep steps small.

## 2. Two primitives

- **durable step** — user step code that returns an outcome on completion (see §3).
- **durable await** — a step parks the instance until a named signal arrives; the wake-up only changes state, with no side effects, so re-running it is harmless.

Everything else (fanning work out, waiting on a group of tasks) is expressed with these two primitives in user code. The engine knows nothing about trees / parent-child — "children" are ordinary independent instances.

## 3. Step outcomes

A step, and the error handler `handle/2`, return one of:

| Outcome | Effect | `attempt` | `eligible_at` |
|---|---|---|---|
| `{:next, step, state}` | transition to `step`, `runnable` | `:= 0` | `now()` |
| `{:replay, state, delay_ms}` | same step again, `runnable` | `+1` | `now() + delay_ms` |
| `{:await, signal_name, state}` | park, `awaiting_signal`, `awaits := signal_name` | — | — |
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

- **no trees** — parent/child, fan-in barrier, cascade. "Children" = ordinary instances; the user coordinates via `await` + signal;
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
  ('runnable', 'executing', 'awaiting_signal', 'done', 'failed');

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

  -- uniqueness
  unique_key    bytea,                              -- user function result, verbatim
  unique_scope  text[]  not null default '{}',      -- statuses in which the key is "occupied"
  unique_guard  bytea generated always as (
    case when unique_key is not null and status::text = any(unique_scope)
         then unique_key end
  ) stored,

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
