# gen_durable — performance

Performance is a first-class constraint for this engine, not an afterthought. This
document is the cost model: what one step costs, the EXPLAIN plans of every hot-path
statement, the indexes that back them, throughput estimates, and the known limits.

All `EXPLAIN (ANALYZE, BUFFERS)` output below is **real**, captured on **Postgres 17**
(the devcontainer image) against a seeded dataset of **1,000,000 `gen_durable` rows**
(295k `runnable`, 5k `executing`, 700k terminal) and **50k `signals`**, over a local
Unix socket. Reproduction steps are at the end. Numbers are warm-cache; absolute ms
will differ on your hardware, but the *plan shapes* and *row counts* are what matter.

---

## 1. The cost model of one step

A step runs **between two short database transactions**, with the user's code in the
middle, outside any transaction (see the architecture notes in `gen_durable_plan.md`):

```
 pick (claim+lease)         user step/2 (no DB, no txn)        outcome (commit)
 ───────────────────►  ····························  ───────────────────►
 TX1: short, batched        side effects live here            TX2: short
 amortized over the batch    (idempotency is yours)           consume + update
```

The database work per step, and where it goes:

| Phase | Statements | Round-trips | Notes |
|---|---|---|---|
| **pick** | 1 (`UPDATE … RETURNING` over a dedup CTE) | ~`1/B` per step | one pick claims a batch of `B`; the feeder amortizes it |
| **load signals** | 1 `SELECT` | 1 | always-on today; gated by an FSM flag is the F4 optimization |
| **load childs** | 1 `SELECT` | 1 | same |
| **user `step/2`** | 0 | 0 | runs outside any transaction |
| **outcome** | `BEGIN` · consume `DELETE` · outcome `UPDATE` · `COMMIT` | ~4 | `:done`/`:stop` add `notify_parent` (+1) |

So a plain `:next` step today is **~6 round-trips** (2 loads + 4 for the outcome txn),
with the pick amortized to near-zero by batching. The two loads and the multi-statement
transaction are the F4 optimization target (gate the loads, collapse the outcome into a
single autocommit CTE) — that takes it to **~1–2 round-trips/step**. See §7.

Round-trips, not query execution time, dominate: every statement below executes in
**well under 1 ms** at the database. The cost is the count of client↔Postgres hops.

---

## 2. The picker (the one query that must scale)

The picker runs on every poll and every completion-driven refill, so it is the query
that most has to stay cheap as the table grows. It does three things in one statement:
claim a batch atomically, serialize partitions, and dedup partition keys (spec §6).

### 2.1 The decisive detail: `queue = $1`, never `ANY`

Each scheduler owns exactly one queue, so the picker filters `queue = $1` (equality).
This is **not cosmetic** — it is what lets the `gen_durable_pick (queue, priority,
eligible_at) WHERE status='runnable'` index supply rows already ordered, so the `LIMIT`
stops after a handful. With `queue = ANY($1)` the planner cannot trust the index order
and falls back to **scanning the entire runnable set and top-N sorting it**:

```
-- queue = ANY('{default}')  ❌
->  Index Scan using gen_durable_pick on gen_durable g_2 (rows=280000)   ← whole runnable set
      Filter: ((partition_key IS NULL) OR (NOT ...))
->  Sort  Sort Method: top-N heapsort
Execution Time: 613.578 ms
```

```
-- queue = 'default'  ✅
->  Limit (rows=50)
      ->  Index Scan using gen_durable_pick on gen_durable g_2 (rows=50)  ← stops at the batch
            Index Cond: ((queue = 'default') AND (eligible_at <= now()))
Execution Time: 0.725 ms
```

**~850× faster, and O(batch) instead of O(runnable backlog).** Same data, one operator.

### 2.2 Full plan of the current picker (`batch = 50`)

```
Update on gen_durable g (actual time=0.279..0.682 rows=50 loops=1)
  Buffers: shared hit=1155 dirtied=1 written=1
  CTE picked
    ->  LockRows (rows=50)
          ->  Sort  Sort Key: g_1.priority, g_1.eligible_at         (50 rows, quicksort 28kB)
                ->  Nested Loop (rows=50)
                      ->  Subquery Scan on d (rows=50)
                            ->  Unique (rows=50)                      ← DISTINCT ON (dedup)
                                  ->  Sort  Sort Key: COALESCE(partition_key, id::text), …  (27kB)
                                        ->  Limit (rows=50)
                                              ->  Index Scan using gen_durable_pick (rows=50)
                                                    Index Cond: ((queue = 'default') AND (eligible_at <= now()))
                                                    Filter: ((partition_key IS NULL) OR (NOT (… hashed SubPlan 2)))
                                                    SubPlan 2
                                                      ->  Index Only Scan using gen_durable_partition_active (never executed)
                      ->  Index Scan using gen_durable_pkey on gen_durable g_1 (rows=1 loops=50)
Planning Time: 0.369 ms
Execution Time: 0.725 ms
```

What to read here:

- **`gen_durable_pick` index scan stops at the `LIMIT`** — the scan touches ~`batch`
  rows, not the 295k runnable rows. This is the whole game.
- **`SubPlan 2` (the partition `NOT EXISTS`) was `never executed`** here because the top
  50 rows were non-partitioned (`partition_key IS NULL` short-circuits the guard). A
  non-partitioned queue pays **nothing** for the partition machinery.
- When partitioned rows *are* in the window, the guard is an **Index Only Scan on
  `gen_durable_partition_active`** (a partial index over only the ~5k executing rows,
  `Heap Fetches: 0`) — a cheap probe, not a join against the big table.
- The two small `Sort`s are over `≤ batch` rows (50): ~27 kB, microseconds. The DISTINCT
  ON dedup is bounded to the batch window, never the backlog.

### 2.3 The bounded-window trade-off

The scan is `LIMIT $2` (the batch size). A cluster of same-key rows filling that window
dedups down to one, so a single pick can return fewer than `batch` — completion-driven
refill closes the gap on the next pick. This keeps the hot path index-cheap at the cost
of a little fill latency under heavy single-key load. See §6 for the degenerate case.

---

## 3. The outcome and point queries (all O(1) by primary key)

Every outcome statement and every signal/insert touches rows **by primary key or a
covering index** — constant work regardless of table size.

**`complete_next` — outcome `UPDATE` by id:**
```
Update on gen_durable
  ->  Index Scan using gen_durable_pkey (rows=1)   Index Cond: (id = 800000)
Execution Time: 0.855 ms
```

**`consume_awaited` — delete the awaited signals (name = `awaits`):**
```
Delete on signals s
  ->  Nested Loop  Join Filter: (s.name = g.awaits)
        ->  Index Scan using signals_target on signals s (rows=1)   Index Cond: (target_id = 700001)
        ->  Index Scan using gen_durable_pkey on gen_durable g       Index Cond: (id = 700001)
Execution Time: 0.093 ms
```

**`deliver_signal` — wake by id+status+awaits:**
```
Update on gen_durable
  ->  Index Scan using gen_durable_pkey (rows=0)
        Filter: ((status = 'awaiting_signal') AND (awaits = 'go'))
Execution Time: 0.029 ms
```

**`insert` — single row, dedup via the partial unique index:**
```
Insert on gen_durable
  Conflict Arbiter Indexes: gen_durable_unique
  Tuples Inserted: 1
Execution Time: 0.468 ms
```

**`load_signals` — the inbox by target:**
```
Sort  Sort Key: id
  ->  Index Scan using signals_target on signals (rows=1)   Index Cond: (target_id = 700001)
Execution Time: 0.072 ms
```

All sub-millisecond, all index-driven. None of them grow with the table.

---

## 4. The reaper (proportional to *expired* rows only)

The reaper sweeps expired leases via the `gen_durable_lease (lease_expires_at) WHERE
status='executing'` partial index — it never scans live or terminal rows:

```
Update on gen_durable (rows=5000)
  ->  Index Scan using gen_durable_lease (rows=5000)
        Index Cond: (lease_expires_at < now())
        Filter: (status = 'executing')
Execution Time: 42.002 ms   ← 5000 rows reaped at once (mass-crash scenario)
```

The index scan itself is ~2 ms; the 42 ms is the heap update of 5000 rows. **In steady
state almost nothing is expired**, so a sweep finds 0 rows and costs an index probe. Cost
scales with the size of the *expired* set (a mass worker death), not the table — exactly
what you want.

---

## 5. Index coverage

Every hot statement is served by a partial or primary-key index; the big terminal-row
bulk (700k `done`/`failed`) sits in none of the partial indexes.

| Query | Index | Kind |
|---|---|---|
| `pick` scan | `gen_durable_pick (queue, priority, eligible_at) WHERE status='runnable'` | partial, ordered |
| `pick` partition guard | `gen_durable_partition_active (partition_key) WHERE status='executing'` | partial, index-only probe |
| `pick` lock / outcomes / signal | `gen_durable_pkey (id)` | primary key |
| `reap` | `gen_durable_lease (lease_expires_at) WHERE status='executing'` | partial |
| `insert` dedup | `gen_durable_unique (unique_guard) WHERE unique_guard IS NOT NULL` | partial unique |
| child join | `gen_durable_parent (parent_id) WHERE parent_id IS NOT NULL` | partial |
| `load_signals` | `signals_target (target_id, name)` | covering |

The partial predicates matter: `gen_durable_pick` indexes only the ~295k runnable rows,
not the 700k terminal ones, so the working set stays small as completed work accumulates.

---

## 6. Throughput model and known limits

### Round-trip-bound throughput

Query *execution* is sub-millisecond, so steady-state throughput is set by **round-trips
per step × round-trip latency**, divided across concurrency.

Let `R` = round-trips/step, `L` = client↔Postgres round-trip latency, `U` = user
`step/2` time. One serial chain does `1 / (R·L + U)` steps/s; with `C` concurrent workers
(pool and DB permitting), throughput ≈ `C / (R·L + U)`.

| | round-trips `R` | per-step DB latency at `L`=0.2 ms (local) | at `L`=1 ms (cloud) |
|---|---|---|---|
| today (2 loads + 4-stmt outcome txn) | ~6 | ~1.2 ms | ~6 ms |
| F4 (gate loads, single-CTE outcome) | ~1–2 | ~0.2–0.4 ms | ~1–2 ms |

So a trivial-step workload at `C = 50`, local, today: `50 / 1.2 ms ≈ 40k steps/s`,
DB permitting; F4 would roughly triple-to-quintuple that. These are model figures from
the measured per-query costs, not an end-to-end benchmark — treat them as order-of-
magnitude. The pick is amortized out by the feeder batch (`0.7 ms / 50 rows ≈ 14 µs/row`).

### Tuning levers (feeder knobs — see `GenDurable.Scheduler`)

- **`prefetch`** — claims a batch ahead into memory; turns many small picks into few fat
  ones (amortizes the pick round-trip). Buffered rows are heartbeated, so depth is safe
  w.r.t. `lease_ttl`. Cost of depth: cross-node fairness, crash blip (bounded by TTL).
- **`min_demand`** — batch gate: fetch fat, not one row per freed slot.
- **`poll_interval` / `max_poll_interval`** — idle backoff cuts the polling load on an
  empty queue to near-zero without hurting busy-queue latency.

### Known limits / pathologies (honest list)

1. **A single hot partition key with a large runnable backlog.** While that key is
   `executing`, the picker's `NOT EXISTS` excludes each of its runnable siblings — one
   cheap index probe each — but if those siblings dominate the top of the queue by
   priority, the scan walks past many of them per pick. The window `LIMIT` bounds the
   *output*, not how far it skips. Mitigation (future): picker sharding by `hashtext(key)`
   so one key maps to one scheduler, or a per-key "next eligible" side structure. A single
   key monopolizing a queue is itself a modeling smell.
2. **Partitioned steps pin a connection.** `partition_key` serialization holds a session
   advisory lock on a checked-out connection for the *whole* step (user code included), so
   in-flight partitioned steps consume pool connections 1:1. Size the pool for peak
   partitioned concurrency. Non-partitioned steps grab/release per statement.
3. **Outcome is a multi-statement transaction.** `BEGIN`/consume/`UPDATE`/`COMMIT` is
   ~4 round-trips; collapsible to one autocommit CTE (`DELETE … ; UPDATE …` as a single
   statement). F4.
4. **`load_signals` + `load_childs` run on every step**, even for FSMs that never await or
   spawn. Two wasted round-trips for the common machine; gate them behind
   `use GenDurable.FSM, awaits: true, childs: true`. F4.

---

## 7. Optimization backlog (F4) — ordered by payoff

1. **Collapse the outcome transaction into one CTE statement** — ~4 round-trips → 1. The
   biggest single win on the hot path.
2. **Gate `load_signals` / `load_childs` on FSM capability flags** — ~2 round-trips → 0
   for plain machines.
3. **Picker sharding by key hash** — removes the hot-key skip cost (limit #1).

Together (1)+(2) take a plain `:next` step from ~6 round-trips to ~1–2.

---

## 8. Reproducing these numbers

The dataset and every `EXPLAIN` above were produced in the devcontainer Postgres. To
regenerate: bring up the stack (`make up`), then in `docker compose … exec db psql -U
postgres`, create a scratch database, apply the v1 DDL from
`lib/gen_durable/migration.ex` verbatim, seed with `generate_series` (700k terminal /
200k runnable non-partitioned / 80k distinct-key / 15k hot-key / 5k executing / 50k
signals), `ANALYZE`, and run `EXPLAIN (ANALYZE, BUFFERS)` on each statement from
`lib/gen_durable/queries.ex`. Wrap mutating statements in `BEGIN; … ; ROLLBACK;` so the
plan executes without changing the dataset. Run each twice and read the second (warm).
