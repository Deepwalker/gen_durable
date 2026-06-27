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
| **pick** | 1 (`UPDATE … RETURNING` over a window-dedup claim) | ~`1/B` per step | one pick claims a batch of `B`; the feeder amortizes it |
| **load signals** | 1 `SELECT` | 1 | always-on today; gated by an FSM flag is the remaining F4 step |
| **load childs** | 1 `SELECT` | 1 | same |
| **user `step/2`** | 0 | 0 | runs outside any transaction |
| **outcome** | 1 CTE (`consumed` DELETE + outcome UPDATE [+ parent-join]) | **1** | folded from a 4–5-stmt transaction into one statement (F4) |

So a plain `:next` step is now **~3 round-trips** (2 loads + 1 outcome), with the pick
amortized to near-zero by batching. The outcome used to be a 4-statement
`BEGIN`/consume/`UPDATE`/`COMMIT` transaction (+1 for `:done`/`:stop`'s parent-join);
folding it into a single data-modifying CTE made it **1** — measured at exactly one
statement (`test/perf_test.exs`), and consistently faster than the transaction form on
wall-clock (the round-trips are the cost). Gating the two loads behind an FSM capability
flag is the last F4 step → **~1 round-trip/step**. See §7.

Round-trips, not query execution time, dominate: every statement below executes in
**well under 1 ms** at the database, but each client↔Postgres hop is a network round-trip
(~0.3–1 ms across hosts). The cost is the count of hops, which is why collapsing the
outcome from 4 hops to 1 matters more than any single statement's plan.

---

## 2. The picker (the one query that must scale)

The picker runs on every poll and every completion-driven refill, so it is the query
that most has to stay cheap as the table grows. It does three things in one statement:
claim a batch atomically, serialize concurrency keys, and dedup concurrency keys (spec §6).

### 2.1 The decisive detail: `queue = $1`, never `ANY`

Each scheduler owns exactly one queue, so the picker filters `queue = $1` (equality).
This is **not cosmetic** — it is what lets the `gen_durable_pick (queue, priority,
eligible_at) WHERE status='runnable'` index supply rows already ordered, so the `LIMIT`
stops after a handful. With `queue = ANY($1)` the planner cannot trust the index order
and falls back to **scanning the entire runnable set and top-N sorting it**:

```
-- queue = ANY('{default}')  ❌
->  Index Scan using gen_durable_pick on gen_durable g_2 (rows=280000)   ← whole runnable set
      Filter: ((concurrency_key IS NULL) OR (NOT ...))
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

### 2.2 Full plan of the picker (`batch = 50`)

The picker is the canonical Postgres claim — one `SELECT … FOR UPDATE SKIP LOCKED LIMIT`,
then one `UPDATE` — with the concurrency_key dedup folded in as a window function over the
locked set, so there is **exactly one nested loop** (the `UPDATE` join):

```
Update on gen_durable g (actual time=0.400..0.983 rows=50 loops=1)
  Buffers: shared hit=868
  CTE locked
    ->  WindowAgg (rows=50)                                     ← dedup: row_number() per key
          ->  Sort  Sort Key: COALESCE(concurrency_key, id::text), priority, eligible_at  (27kB)
                ->  Limit (rows=50)
                      ->  LockRows (rows=50)                    ← FOR UPDATE SKIP LOCKED, in-scan
                            ->  Index Scan using gen_durable_pick (rows=50)
                                  Index Cond: ((queue = 'default') AND (eligible_at <= now()))
                                  Filter: ((concurrency_key IS NULL) OR (NOT (… hashed SubPlan 2)))
                                  SubPlan 2
                                    ->  Index Scan using gen_durable_lease (never executed)
  ->  Nested Loop (rows=50)                                     ← the one, optimal join
        ->  CTE Scan on locked l (rows=50)  Filter: (rn = 1)
        ->  Index Scan using gen_durable_pkey on gen_durable g (rows=1 loops=50)
Planning Time: 0.582 ms
Execution Time: 1.122 ms
```

What to read here:

- **`gen_durable_pick` index scan stops at the `LIMIT`** — the scan touches ~`batch`
  rows, not the 295k runnable rows. This is the whole game.
- **The lock happens in that one scan** (`LockRows`), and `row_number()` dedups the
  *already-locked* set — so there is no separate re-lock pass. The earlier design
  (`DISTINCT ON` → re-lock → update) had a second nested loop that scaled per-row with
  the batch; folding the dedup into a window function removed it (measured −19% at
  batch 5000, §2.4).
- **`SubPlan 2` (the concurrency_key `NOT EXISTS`) was `never executed`** here because the top
  50 rows were non-keyed (`concurrency_key IS NULL` short-circuits the guard). A
  non-keyed queue pays **nothing** for the concurrency machinery. When concurrency-keyed
  rows *are* in the window, the guard probes `gen_durable_concurrency_active` — a partial
  index over only the executing rows with a **non-null** `concurrency_key`, so a
  non-keyed claim never even writes to it.
- **The single `Nested Loop` is the `UPDATE` join, and it is optimal** — outer = `batch`
  rows, inner = one primary-key point lookup each (`loops=50, rows=1`). That is O(batch)
  point updates, the textbook-best way to update N rows by id; see §2.5 for the proof
  that forcing it off is ~10× slower. The per-key losers (`rn > 1`) were locked but not
  updated, so they stay `runnable` and their lock releases at commit — no advisory bounce.

### 2.3 The bounded-window trade-off

The scan is `LIMIT $2` (the batch size). A cluster of same-key rows filling that window
dedups down to one, so a single pick can return fewer than `batch` — completion-driven
refill closes the gap on the next pick. This keeps the hot path index-cheap at the cost
of a little fill latency under heavy single-key load. See §6 for the degenerate case.

### 2.4 Large batches and the per-row floor

Crank `prefetch` and the pick claims a big batch in one statement. The cost is **linear
in the batch** — every component (index scan, dedup sort, lock, the `UPDATE`) is per-row:

| batch | pick (median of 5, warm) | per row |
|---|---|---|
| 50 | ~1.1 ms | ~22 µs |
| 100 | ~1.9 ms | ~19 µs |
| 1000 | ~16 ms | ~16 µs |
| 5000 | ~57 ms | ~11 µs |

The fixed cost (planning + the `NOT EXISTS` hash build over the executing set) amortizes
away with bigger batches; the **~11–16 µs/row marginal cost does not**. That floor is the
actual work of *claiming*: flipping each row `runnable → executing` is a heap write plus
moving the row out of the `runnable` partial index and into the two `executing` partial
indexes (`lease`, `concurrency_active`) — plus WAL for all of it. **You cannot claim a row
without writing it**, so ~11–16 µs/row is close to the floor for this schema.

Two consequences worth designing around:

- A pick of 5000 is a **70 ms synchronous statement** in the scheduler GenServer and holds
  `FOR UPDATE` over 5000 rows. Past ~a few hundred, batch size buys no amortization (the
  fixed cost is already gone) and only adds blocking. If you push `prefetch` very high,
  prefer many medium picks over one giant one.
- Per-row cost is **aggregate DB CPU**: at 10k steps/s that floor is ~0.15 s/s of CPU on
  picking alone. The way to cut it is fewer, larger logical steps — not a faster pick.

### 2.5 The one nested loop is optimal (proof)

The `UPDATE` join is a `Nested Loop` and that is exactly right. "Update these N rows by id"
wants N primary-key point lookups — a nested loop with the PK index on the inner side,
O(batch). It only looks scary when the inner side is *unindexed* (then it is O(N·M)); here
it is `loops=batch, rows=1` against `gen_durable_pkey`. Forcing the planner off it
(`SET enable_nestloop = off`) makes it fall back to a hash join that **Seq Scans the whole
million-row table** to find the batch:

```
->  Hash Join  (Hash Cond: g.id = l.id)
      ->  Seq Scan on gen_durable g  (rows=1000000)     ← scans everything to find `batch` rows
```

| batch | nested loop (default) | forced off → hash join + seq scan |
|---|---|---|
| 1000 | 17 ms | 160 ms |
| 5000 | 56 ms | 195 ms |

~10× slower. The nested loop is the canonical Postgres-queue claim, and it is the floor:
`UPDATE` cannot `LIMIT`, so the claim must be `SELECT … FOR UPDATE … LIMIT` then `UPDATE`
joined by id; and updating N rows means writing N rows regardless of how they are reached.

### 2.6 What did *not* help (measured, rejected)

Restructurings prototyped against the seeded dataset and **rejected by measurement** —
recorded so they are not re-attempted blind:

- **Single-scan dedup** (correlated `NOT EXISTS` "I am the most-urgent runnable of my key"
  + `FOR UPDATE` in the scan, backed by a new `(concurrency_key, priority, eligible_at)`
  partial index). A wash on wall-clock and **worse at batch 5000** — the new partial index
  adds write amplification to every claim *and* every return-to-`runnable`. The window
  function over the locked set (the shipped picker) gets the same dedup with no new index.
- **`ctid`-join instead of `id`-join** for the `UPDATE` re-touch (TID scan instead of a PK
  descent). Cut **logical buffer hits ~28%** but **warm-cache wall-clock did not move**
  (17.0 vs 17.0 ms at 1000) and was **slower at 5000** (79 vs 70). Those buffers are
  nanosecond cache hits; time is dominated by heap writes + WAL, which `ctid` does not
  touch — consistent with §2.5 (the cost is the write, not the lookup).

**Lesson:** on a warm queue the picker is at its floor; fewer logical page touches do not
translate to time when they are all cache hits. The real lever is **round-trips per step**
(§7), not the pick query. Beware benchmarking on a bloated table — rolled-back
`EXPLAIN ANALYZE` runs accumulate dead tuples and inflate timings by ~20%; `VACUUM` and
take the median of several runs before believing a delta.

---

## 3. The outcome and point queries (all O(1) by primary key)

Every outcome and every signal/insert touches rows **by primary key or a covering index** —
constant work regardless of table size.

### The collapsed outcome (F4) — the plan, not the statement count

Each `complete_*` is one statement (§1): the signal consume rides as a leading `consumed`
CTE, and `:done`/`:stop` carry the parent-join as the main `UPDATE` after a `terminal` CTE.
Counting "one statement" only proves the *round-trip* — the plan has to prove the single
statement is *cheap*. Here is the heaviest outcome, `complete_done` on a **child** (all
three parts: consume + done + parent decrement), measured with **50,000 children present**
so the parent path is real, not a one-row fixture:

```
Update on gen_durable p                                          ← the parent decrement
  CTE consumed
    ->  Delete on signals s   (Index Scan signals_target → gen_durable_pkey)
  CTE terminal
    ->  Update on gen_durable  ->  Index Scan using gen_durable_pkey  (id = $1)
  ->  Nested Loop
        ->  Index Scan using gen_durable_pkey on c   (id = $1)            ← child by PK
        ->  Index Scan using gen_durable_pkey on p   (id = c.parent_id)   ← parent by PK
Execution Time: 0.268 ms
```

**Every node is a primary-key index scan — no `Seq Scan` on `gen_durable`**, even with 50k
rows in the parent index. The `c.id = $1` equality is the selective path so the planner
stays on the PK (a *one-row* fixture can fool it onto the partial parent index with an
`id` filter — harmless there, and it does not happen once stats are real). For a non-child
`:done` the parent-join is a clean PK no-op (`p` matches 0 rows).

And it is not only fewer round-trips: the single statement is **less DB execution time**
than the old three separate statements — median **~0.25 ms vs ~0.42 ms** (EXPLAIN ANALYZE,
300k-row table), before even counting the `BEGIN`/`COMMIT` round-trips the transaction form
also paid. Consume is by received id (spec §5): a progressing outcome deletes exactly the
`ctx.awaited` ids the step saw (`id = ANY($consumed)`, a PK lookup), so latecomers and
never-awaited signals survive; a terminal outcome drops the whole inbox (`target_id = $id`).

(The statement-count assertions in `test/perf_test.exs` guard the *round-trip* count; this
EXPLAIN is the *execution-cost* evidence. Different claims, both checked.)

### Point queries

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

## 4b. The GC sweep (must scale with the batch, not the table)

GC (`GenDurable.GC`) deletes terminal rows older than `:gc_retention` in bounded
batches. The naïve form is a trap — measured on a **1,000,000-row** table (800k old
terminal, 100k terminal children of active parents, 100k active), retention 1 day,
batch 10k:

```
-- DELETE FROM gen_durable WHERE id IN (SELECT … LIMIT 10000)
Hash Semi Join                       (actual 7.8..352 ms)
  ->  Seq Scan on gen_durable        (rows=1000000, 190 ms)   ← scans the WHOLE table
Execution Time: 410 ms
```

`WHERE id IN (subquery)` (and `… USING doomed`) makes the planner **Seq Scan the entire
table** to match the small doomed set: cost is **O(table)**, ~190 ms here and *seconds*
on a 100M-row table — to delete only 10k rows. The shipped form splits it: select the
≤ `batch` ids, then delete by `id = ANY($ids)`:

```
-- DELETE FROM gen_durable WHERE id = ANY($ids)   (10k ids)
Delete on gen_durable                              (actual 5.9 ms)
  ->  Index Scan using gen_durable_pkey            (rows=10000, 1.9 ms)   ← PK, O(batch)
Trigger gen_durable_parent_id_fkey: 22 ms          (ON DELETE SET NULL, 10k)
Trigger signals_target_id_fkey:     23 ms          (cascade, 10k)
Execution Time: 52 ms
```

**52 ms vs 410 ms**, and — the point — the delete is a **primary-key index scan**, so
cost tracks the *batch* (10k), not the table. The remaining ~45 ms is the two FK
triggers, inherent to deleting 10k rows that may have children/signals, also O(batch).
Terminal rows are immutable, so the ids stay valid between the two round-trips.

The id-select uses `gen_durable_gc (updated_at) WHERE status IN ('done','failed')` when
old terminal rows are sparse; when they dominate the table the planner prefers a
Seq Scan that the `LIMIT` short-circuits. One honest caveat: the `NOT EXISTS` parent
guard means a sweep can wade past terminal **children of still-active parents** before
filling the batch — bounded by how many such children cluster ahead of collectibles
(transient in practice, since parents join quickly).

---

## 5. Index coverage

Every hot statement is served by a partial or primary-key index; the big terminal-row
bulk (700k `done`/`failed`) sits in none of the partial indexes.

| Query | Index | Kind |
|---|---|---|
| `pick` scan | `gen_durable_pick (queue, priority, eligible_at) WHERE status='runnable'` | partial, ordered |
| `pick` concurrency guard | `gen_durable_concurrency_active (concurrency_key) WHERE status='executing' AND concurrency_key IS NOT NULL` | partial, index-only probe |
| `pick` lock / outcomes / signal | `gen_durable_pkey (id)` | primary key |
| `reap` | `gen_durable_lease (lease_expires_at) WHERE status='executing'` | partial |
| `insert` dedup | `gen_durable_unique (unique_guard) WHERE unique_guard IS NOT NULL` | partial unique |
| child join | `gen_durable_parent (parent_id) WHERE parent_id IS NOT NULL` | partial |
| `gc` id-select | `gen_durable_gc (updated_at) WHERE status IN ('done','failed')` | partial (situational) |
| `gc` delete | `gen_durable_pkey (id)` | primary key |
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

1. **A single hot concurrency key with a large runnable backlog.** While that key is
   `executing`, the picker's `NOT EXISTS` excludes each of its runnable siblings — one
   cheap index probe each — but if those siblings dominate the top of the queue by
   priority, the scan walks past many of them per pick. The window `LIMIT` bounds the
   *output*, not how far it skips. Mitigation (future): picker sharding by `hashtext(key)`
   so one key maps to one scheduler, or a per-key "next eligible" side structure. A single
   key monopolizing a queue is itself a modeling smell.
2. **Concurrency-keyed steps pin a connection.** `concurrency_key` serialization holds a session
   advisory lock on a checked-out connection for the *whole* step (user code included), so
   in-flight concurrency-keyed steps consume pool connections 1:1. Size the pool for peak
   concurrency-keyed concurrency. Non-concurrency-keyed steps grab/release per statement.
3. **`load_signals` + `load_childs` run on every step**, even for FSMs that never await or
   spawn. Two wasted round-trips for the common machine; gate them behind
   `use GenDurable.FSM, awaits: true, childs: true`. The last remaining F4 step.

---

## 7. Optimization backlog — ordered by payoff

1. ✅ **Collapse the outcome transaction into one CTE statement** — done (F4): a 4–5-stmt
   `BEGIN … COMMIT` became one data-modifying CTE, ~4 round-trips → 1, the biggest single
   win on the hot path. Asserted single-statement in `test/perf_test.exs`.
2. **Gate `load_signals` / `load_childs` on FSM capability flags** — ~2 round-trips → 0
   for plain machines. Takes a plain `:next` step from ~3 round-trips to ~1.
3. **Picker sharding by key hash** — removes the hot-key skip cost (limit #1).

Together (1)+(2) take a plain `:next` step from ~6 round-trips to ~1–2.

---

## 8. Reproducing these numbers

The dataset and every `EXPLAIN` above were produced in the devcontainer Postgres. To
regenerate: bring up the stack (`make up`), then in `docker compose … exec db psql -U
postgres`, create a scratch database, apply the v1 DDL from
`lib/gen_durable/migration.ex` verbatim, seed with `generate_series` (700k terminal /
200k runnable non-keyed / 80k distinct-key / 15k hot-key / 5k executing / 50k
signals), `ANALYZE`, and run `EXPLAIN (ANALYZE, BUFFERS)` on each statement from
`lib/gen_durable/queries.ex`. Wrap mutating statements in `BEGIN; … ; ROLLBACK;` so the
plan executes without changing the dataset. Run each twice and read the second (warm).
