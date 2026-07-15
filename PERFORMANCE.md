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
middle, outside any transaction:

```
 pick (claim+lease)         user step/2 (no DB, no txn)        outcome (commit)
 ───────────────────►  ····························  ───────────────────►
 TX1: short, batched        side effects live here            TX2: short
 amortized over the batch    (idempotency is yours)           consume + update
```

The database work per step, and where it goes:

| Phase | Statements | Round-trips | Notes |
|---|---|---|---|
| **pick** | 3 (window-dedup claim + batched signal load + batched children load) | ~`3/B` per step | one pick claims and enriches a batch of `B`; the feeder amortizes it |
| **user `step/2`** | 0 | 0 | runs outside any transaction |
| **outcome** | batched flush: `UPDATE … FROM unnest` [+ consume / parent-join / recheck / children] | ~`3/F` per step | one flush transaction commits `F` outcomes across instances (group commit) |

So a plain `:next` step is **~1 round-trip's worth of DB work** — its outcome — with both the
pick (including inbox/children enrichment) and the commit amortized across a batch. The commit
goes through `GenDurable.Flusher`: the worker Task blocks until durable, and the flusher
coalesces every outcome waiting on it into **one transaction** — a single
`UPDATE gen_durable … FROM unnest(...)` for the whole batch, plus the consume / parent-join /
await-recheck / children-insert riders. A flush of `F` outcomes is a **bounded** number of
statements (not `F`) — `test/perf_test.exs` asserts three for a plain-`:next` batch regardless
of size — so the per-instance commit storm (one transaction per instance per step, each its own
lock footprint and WAL flush) is gone.

The tradeoff is latency: an outcome waits up to `max_delay_ms` (default 100 ms) under light load
(no batch to amortize). Under load the batch **auto-grows** — waiters pile up while a flush runs
— so throughput is decoupled from that delay, and the single coordinator is not a linear
bottleneck (step *execution* stays fully parallel; only the commit is coalesced). Tune
`flushers: [%{queues:, max_batch:, max_delay_ms:}]` per queue.

Round-trips, not query execution time, dominate: every statement below executes in
**well under 1 ms** at the database, but each client↔Postgres hop is a network round-trip
(~0.3–1 ms across hosts). The cost is the count of hops — which is why coalescing N per-instance
commits into one batched transaction matters more than any single statement's plan.

### 1.1 Inline execution (run-ahead) — trading the re-pick for a held claim

With `use GenDurable.FSM, inline_execution: true`, a `:next` step does not go back through
the picker. The step transition and the next step run **in the same worker task**, on the
claim already held:

| Phase (chained step) | Statements | Notes |
|---|---|---|
| **continue commit** | batched flush | keep-executing continue entry, coalesced with other outcomes (durability unchanged) |
| **enrich** | 2 | the same batched inbox + children loads a re-pick would run — a fresh snapshot |
| **`Limiter.admit`** | 0 or 1 | only when the next step is rate-limited or adopts a *configured* concurrency key |

So a chained step is **~3 round-trips (+1 when admitting)** — but it **skips the pick's claim
scan, the requeue write, and the task respawn** entirely. The claim scan (§2) is the
expensive statement on the hot path (index scan + `SKIP LOCKED` + K=1 window), so avoiding it
per step is the win, even though the enrich loads are still paid (they keep the run-ahead
step's `ctx.all`/`ctx.childs` identical to what a re-pick would hand it). `concurrency_key:
:keep` — the default — needs no admit round-trip at all: the instance holds its one slot
across the whole chain. This is **opt-in and default-off**, so the §1 statement counts and
the `test/perf_test.exs` pick/outcome assertions are unchanged; the chained-step counts are
guarded separately there. The tradeoff — a chain doesn't re-check priority until it yields —
is in `ISSUES.md`.

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

The picker is the canonical Postgres claim — one `SELECT … FOR NO KEY UPDATE SKIP LOCKED
LIMIT` (NO KEY: claims coexist with the `FOR KEY SHARE` that signal-insert FK checks take),
then one `UPDATE` — with the concurrency_key dedup folded in as a window function over the
locked set, so there is **exactly one nested loop** (the `UPDATE` join):

```
Update on gen_durable g (actual time=0.400..0.983 rows=50 loops=1)
  Buffers: shared hit=868
  CTE cand
    ->  WindowAgg (rows=50)                                     ← dedup: row_number() per key
          ->  Sort  Sort Key: concurrency_key, priority, eligible_at  (27kB)  ← raw key, no synth
                ->  Limit (rows=50)
                      ->  LockRows (rows=50)                    ← FOR NO KEY UPDATE SKIP LOCKED, in-scan
                            ->  Index Scan using gen_durable_pick (rows=50)
                                  Index Cond: ((queue = 'default') AND (eligible_at <= now()))
                                  Filter: ((concurrency_key IS NULL) OR (concurrency_name = ANY($5)) OR (NOT (… hashed SubPlan 2)))
                                  SubPlan 2
                                    ->  Index Scan using gen_durable_lease (never executed)
  ->  Nested Loop (rows=50)                                     ← the one, optimal join
        ->  CTE Scan on cand  Filter: ((concurrency_key IS NULL) OR (rn = 1) OR gated)
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
- **Gate membership is a column comparison, not a per-row parse.** `concurrency_name = ANY($5)`
  reads the STORED generated split of `concurrency_key` (materialized once per write, see
  `Migration` change(3)) against the configured-gate array threaded from the caller. The pick
  does **no `split_part(...)` and no join to `gen_durable_bucket_configs`** — see §2.7 for the
  measured cost this removed. The window `PARTITION BY concurrency_key` is over the **raw** key;
  all-NULL keys land in one partition but are kept wholesale by the `concurrency_key IS NULL`
  branch of `winners`, so there is no synthetic per-row partition string either.
- **The single `Nested Loop` is the `UPDATE` join, and it is optimal** — outer = `batch`
  rows, inner = one primary-key point lookup each (`loops=50, rows=1`). That is O(batch)
  point updates, the textbook-best way to update N rows by id; see §2.5 for the proof
  that forcing it off is ~10× slower. The per-key losers (`rn > 1`) were locked but not
  updated, so they stay `runnable` and their lock releases at commit.

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
  row locks over 5000 rows. Past ~a few hundred, batch size buys no amortization (the
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
`UPDATE` cannot `LIMIT`, so the claim must be a locking `SELECT … LIMIT` then `UPDATE`
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

### 2.7 Gate membership without a per-row string parse (measured)

Before: the claim decided "is this a configured concurrency gate?" per candidate row with
`split_part(concurrency_key, ':', 1)` (a substring allocation) tested via a correlated
`EXISTS` **and** a `LEFT JOIN` to `gen_durable_bucket_configs` — plus a synthetic partition
string `coalesce('k:' || concurrency_key, 'i:' || id::text)` so NULL keys stayed distinct.
All of it per row, in the hottest query.

Now: `concurrency_key` carries a STORED generated column `concurrency_name`
(`split_part(concurrency_key, ':', 1)`, computed **once per write** by Postgres). The claim
tests `concurrency_name = ANY($5)` against the configured-gate array the caller already holds
(`config.concurrency_limit_names` — the set the executor trusts too), and partitions by the
**raw** `concurrency_key`. The configs table leaves the hot path entirely (it stays the source
of truth for the limiter and GC, not the claim).

Measured on Postgres 17, 50k runnable rows (80 % plain / 10 % gated / 10 % unconfigured, 500
executing to exercise the arbiter probe):

| | claim, batch 200 | isolated window+filter over all 50k (no LIMIT) | per candidate row |
|---|---|---|---|
| before (`split_part` ×2 + configs join + `‖` partition) | 2.25 ms | 285 ms | ~5.7 µs |
| after (`concurrency_name = ANY` + partition by key) | **1.98 ms** | **173 ms** | **~3.5 µs** |

The per-pick win is modest (~12 %) because `LIMIT $2` caps the parsed rows and the claim's
`UPDATE` dominates a single pick (§2.4). The real saving is **~1.5–2 µs of CPU per candidate
row**, and it scales with `batch × pick-frequency`: at the §2.4 reference of ~10k picked
rows/s that is a measurable slice of the ~0.15 s/s picking floor, burned on string work the
column removed. Statement counts are unchanged (claim is still one statement; `test/perf_test.exs`
holds at 1 / 3). The cost paid once: `change(3)`'s `ADD COLUMN … STORED` rewrites the table
under `ACCESS EXCLUSIVE` (~125 ms / 50k rows here; scales with row count — a maintenance-window
migration on a large install).

---

## 2b. Rate limiting folds into the picker for free on the common path

Token-bucket rate limiting (spec §12) is a set of CTEs appended to the §2 pick. The design
goal was **zero cost when unused** and **bounded cost when used**. Measured.

**Common path (no rate-limited rows).** EXPLAIN of the pick on a 5000-row runnable queue,
all `rate_limit IS NULL`:

```
Update on gen_durable g (rows=50)
  CTE cand     -> Index Scan using gen_durable_pick (rows=50)   ← unchanged candidate select
  CTE winners  -> WindowAgg -> Sort (rows=50, 27kB quicksort)   ← the only added work
  CTE r_cold   -> Anti Join (rows=0); gen_durable_buckets index "never executed"
  CTE r_locked -> LockRows (rows=0); gen_durable_buckets index "never executed"
  CTE r_shards/r_key_avail/granted/r_new/writeback/r_mint -> rows=0 / never executed
  final        -> Nested Loop -> Index Scan gen_durable_pkey (the PK flip, as before)
Execution Time: 1.9 ms   (the full production pick, §2c gate CTEs included)
```

The candidate scan still rides `gen_durable_pick`, the flip is still the PK update — the
rate-bucket joins do **nothing** (`never executed`, zero rows). The only addition is a window
sort to compute the cumulative weight, over the **≤batch** winners (50 rows, ~27 kB,
sub-millisecond) — bounded by batch, not table size.

**Buckets are minted by the pick itself, pre-debited.** No transition creates bucket rows; the
first pick that grants from a key admits against the virtual full bucket (a fresh mint is
`burst` by definition) and INSERTs it already debited — cold keys, including keys whose idle
bucket the GC swept, admit with **zero lag**. Two picks racing the same cold key collide on the
bucket's primary key; the loser's claim aborts whole and retries against the winner's committed
row (`[:gen_durable, :rate_limit, :contended]`, same bounded-retry discipline as the §2c gates).

**Under contention (sharded).** A key's budget is split across `shards` counter rows
(default 1). Each pick locks the shards it needs with `FOR UPDATE OF b SKIP LOCKED`, so
concurrent (cross-node) pickers grab **disjoint** shards and admit in parallel instead of
serializing — or blocking a whole pick — on one row held across the claim. A skip-locked lock
never waits, so bucket deadlock is structurally impossible; a lone picker grabs every shard
and sees the full `burst` (`weight ≤ burst` still holds), and the consumed weight is debited
**proportionally** across the grabbed shards (rate carries no per-row shard — it has no
credit-back). Size `shards ≥ the nodes that contend the hottest key`; leave it at 1 for a
single-node or cold key. The cold-mint conservation property is unchanged: a run of rate 0,
burst 5, 100 fresh keys × 10 jobs, 8 workers racing the mints granted **exactly** 5 per key
with every bucket at exactly 0.0 tokens — the PK-collision path loses and leaks nothing.

> ⚠ **The table below was measured on the pre-sharding 0.2.6 design** — a single counter row
> locked with blocking `FOR UPDATE`. It showed that even one hot bucket did not register at
> batch 50 (the lock was taken once per pick, held for the statement only, amortized over the
> batch — all within a ±3% noise band). The sharded `SKIP LOCKED` design changes the
> contention mechanism (disjoint shards, skip-instead-of-wait), and reliable multi-picker
> contention numbers need production-like idle-backoff and real multi-node hardware — not a
> single-VM raw-loop drain, which would penalize skip-and-retry for a spin the scheduler
> backs off. These numbers are **pending re-measurement**; the correctness properties above
> still hold. (This also reverses the earlier note that `SKIP LOCKED` was *slower*: that was
> true for one hot **unsharded** row with no alternative work — sharding removes that row.)

| path (pre-sharding 0.2.6) | throughput | vs baseline |
|---|---|---|
| no rate limit | 9 150 jobs/s | 1.00× |
| one hot bucket, cold start | 9 320 jobs/s | 1.02× |
| one hot bucket, pre-warmed | 9 400 jobs/s | 1.03× |
| 100 buckets (partitioned), cold | 9 060 jobs/s | 0.99× |
| 2 000 cold keys, 10 jobs each | 8 960 jobs/s | 0.98× |

### 2c. Concurrency gates: batched grants, per-shard release chains

A configured `concurrency_key` (a gate: at most `limit` in flight) adds its own CTE family
to the pick, shaped like the rate limiter's: lock this picker's share of the gate's
slot-counter shards (`FOR UPDATE OF b SKIP LOCKED`, ordered), admit the winners' prefix
against the grabbed `available`, debit in one writeback. Like the rate CTEs, all of it is
`never executed` when no gated rows are in the window — a non-gated queue pays nothing (same
EXPLAIN discipline as §2b).

Sharding helps **both** sides. On the **grant** side, each pick locks only the shards it
touches with `SKIP LOCKED`, so concurrent pickers take disjoint shards and admit in parallel —
a hot gate no longer serializes (or blocks a whole pick on) one row. On the **release** side,
the outcome's `credit` rider locks the specific shard it addresses until commit, so
completions of one key form a commit-latency chain per shard — a per-shard ceiling of roughly
`1 / commit_latency` (≈1–3k completions/s on local disks, ~300–1000/s on cloud storage;
chained transactions cannot group-commit). `S` shards ⇒ up to `S` parallel grants and `S`
independent release chains. Size `shards` ≥ the larger of the contending nodes (grant
parallelism) and `limit × commit_latency / step_duration` (release throughput). The
self-limiting argument of §2b applies on both sides: a gate is only hot if its key is hot, and
the cap itself throttles the key — a config that saturates its own gate (huge limit, sub-10ms
steps) is capping something that did not need capping.

Crash paths deliberately under-credit (a leaked slot means *stricter*-than-limit, never
looser); the GC reconciler repairs the counters from the executing-rows truth each sweep,
and the `CHECK (0 ≤ available ≤ cap)` makes both over-admission and double-credit
uncommittable at the schema level. The CHECK is not decorative: the drain bench below
caught a real over-admission race through it (a locked bucket scan emitting a row twice
under concurrent writes — fixed by deduplication in the pick, with the CHECK + retry as
the remaining backstop).

> ⚠ **Measured on the pre-sharding 0.2.6 design** (drain of 20k zero-length jobs, 8 pickers,
> batch 50): one shard serialized both grants and credits on a single row (0.46× of lockless,
> the worst case by construction — zero-length steps = pure gate traffic), 8 shards recovered
> to ~0.9×. Under the current `SKIP LOCKED` grants the 1-shard grant side no longer *blocks*
> (it skips), so these numbers are **pending re-measurement** on production-like backoff /
> multi-node hardware (see the §2b note).

| scenario (pre-sharding 0.2.6) | throughput | vs baseline |
|---|---|---|
| no key (baseline) | 9 579 jobs/s | 1.00× |
| gate, never throttling, 1 shard | 4 389 jobs/s | 0.46× |
| gate, never throttling, 8 shards | 8 428 jobs/s | 0.88× |

Cold gates cost nothing extra: the first claim mints the counters pre-debited in the
same statement (a racing double-mint merges via ON CONFLICT, overdraft aborted by the
CHECK and retried), so "no buckets yet" is indistinguishable from "buckets full".
Real workloads sit far from this ceiling: with steps of any real duration, the cap
itself throttles the key long before the gate's machinery does (the §2b self-limiting
argument). Capped-scenario numbers are omitted — with zero-length steps they measure
the drain loop's backoff, not the engine.

---

## 3. The outcome and point queries (all O(1) by primary key)

Every outcome and every signal/insert touches rows **by primary key or a covering index** —
constant work regardless of table size.

### The batched flush — the plan, not the statement count

The outcome flush is one transaction (§1): the guarded `UPDATE gen_durable … FROM unnest(...)`
joins the parameter arrays to `gen_durable` **by primary key** (a nested loop over the batch,
one PK probe per row — the same shape `insert_all` relies on), and the riders (signal consume,
parent-join, await-recheck, children-insert) are each index scans over the committed set.
Counting statements only proves the *round-trip* — the plan has to prove each is *cheap*, and
each is **O(1) per row** (PK/covering-index bound), so a batch of `F` is `O(F)` index probes in
one transaction, not `O(table)`. Below is the per-row shape of the heaviest case, a terminal on
a **child** (consume + terminal update + parent decrement), measured with **50,000 children
present** so the parent path is real, not a one-row fixture — the batched flush runs this shape
once for the whole batch, every node a PK/index scan:

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

**`deliver_signal` — one statement (target resolve → inbox insert → wake), all
PK/partial-index driven.** Note the wake deliberately has NO status filter in its WHERE —
it matches the target row unconditionally and flips via CASE, because a status-guarded
WHERE skips a not-yet-parked row *without locking*, which was the lost-wakeup race. The
row lock it always takes on a live row is the correctness mechanism, and it costs one
PK-indexed row.

**`insert` — single row, dedup via the partial unique index
(`gen_durable_correlation` on `correlation_guard`):**
```
Insert on gen_durable
  Conflict Arbiter Indexes: gen_durable_correlation
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

GC (`GenDurable.GC`) deletes terminal rows older than the configured `retention` in bounded
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
| `pick` concurrency guard | `gen_durable_concurrency_active` / `gen_durable_lease` (planner's choice: per-candidate probes or one hashed scan of the executing set) | partial; bounded by in-flight count, not table size |
| `pick` lock / outcomes / signal | `gen_durable_pkey (id)` | primary key |
| `reap` | `gen_durable_lease (lease_expires_at) WHERE status='executing'` | partial |
| `insert` dedup / key addressing | `gen_durable_correlation (correlation_guard) WHERE correlation_guard IS NOT NULL` | partial unique |
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
| pre-F4 (2 per-step loads + 4-stmt outcome txn) | ~6 | ~1.2 ms | ~6 ms |
| current (batch-enriched pick, single-CTE outcome) | ~1 | ~0.2 ms | ~1 ms |

The per-step loads are gone entirely: the pick batch-loads signal inboxes and children
for its whole claim set (3 statements per *batch*, asserted in `test/perf_test.exs`), so a
plain `:next` step pays one round-trip — its outcome. The exception is `:await`, a
deliberate 4-round-trip transaction (park + recheck; it buys the lost-wakeup fix, and
parking is externally-paced anyway). A trivial-step workload at `C = 50`, local:
`50 / 0.2 ms ≈ 250k steps/s` as a model ceiling — these are model figures from the
measured per-query costs, not an end-to-end benchmark; treat them as order-of-magnitude.
The pick is amortized out by the feeder batch (`0.7 ms / 50 rows ≈ 14 µs/row`).

### Tuning levers (feeder knobs — see `GenDurable.Scheduler`)

- **`prefetch`** — claims a batch ahead into memory; turns many small picks into few fat
  ones (amortizes the pick round-trip). Buffered rows are heartbeated, so depth is safe
  w.r.t. `lease_ttl`. Cost of depth: cross-node fairness, crash blip (bounded by TTL).
- **`min_demand`** — batch gate: fetch fat, not one row per freed slot.
- **`poll_interval` / `max_poll_interval`** — idle backoff cuts the polling load on an
  empty queue to near-zero without hurting busy-queue latency. Locally-inserted work
  doesn't wait on either: the insert pokes the queue's scheduler on its own node, so the
  poll only bounds discovery of *remote* inserts, wakes, and retry backoffs.

### Known limits / pathologies (honest list)

1. **A single hot concurrency key with a large runnable backlog.** While that key is
   `executing`, the picker's `NOT EXISTS` excludes each of its runnable siblings — one
   cheap index probe each — but if those siblings dominate the top of the queue by
   priority, the scan walks past many of them per pick. The window `LIMIT` bounds the
   *output*, not how far it skips. Mitigation (future): picker sharding by `hashtext(key)`
   so one key maps to one scheduler, or a per-key "next eligible" side structure. A single
   key monopolizing a queue is itself a modeling smell.
2. **A cross-node concurrency_key claim race aborts the whole pick batch.**
   Serialization is a UNIQUE partial index over executing keys, so two picks racing the
   same key resolve by a unique violation on one of them — which aborts that pick's entire
   claim statement, not just the conflicting row (an UPDATE has no ON CONFLICT). The pick
   retries (bounded), the winner is visible by then, and the loser's batch is re-claimed —
   one wasted round-trip on a rare race, observable via `[:concurrency, :contended]`. In
   exchange, no per-step locks or pinned connections exist at all: every step, keyed or
   not, touches the pool per statement only.
3. **A far-future scheduled backlog on a more-urgent priority.** The pick index is
   `(queue, priority, eligible_at)`: within one priority group, eligible rows sort before
   future ones, so delayed work on the *same* priority costs nothing. But to reach
   priority `p+1`, the scan must walk through (and filter out) every future-eligible row
   of priority `p`. Pathology: a large delayed backlog on an urgent priority plus live
   work on a less urgent one — every pick re-scans that backlog. If you schedule delayed
   work at scale, keep it on one priority (ideally the least urgent). The structural cure
   — a separate `scheduled` status promoted to `runnable` by a sweeper (Oban's Stager
   shape) — adds a background process and buys nothing until a real workload hits this,
   so it stays unbuilt.
4. **A denied-after-the-window backlog at the head of a queue caps its visibility.**
   The K=1 concurrency guard is a `WHERE` filter — skipped rows don't consume the
   `LIMIT`. But both capacity admissions happen *after* the candidate window: a
   rate-throttled row and a row of a saturated **configured gate** are picked as
   candidates, denied (no tokens / no free slots), and left runnable — still occupying
   their `LIMIT` slots next pick. A saturated key whose backlog is older (earlier
   `eligible_at`, same priority) than the live work behind it caps the queue's
   effective visibility at batch × concurrent picks; behind a deep enough denied head,
   unrelated work starves until the head drains — at the refill rate for a rate key, at
   the completion rate (`limit / step_duration`) for a gate, and never for a head that
   cannot run (`weight > burst`, an unconfigured rate name). Mitigations: give heavily
   capped flows their own queue (the clean cure — queues isolate windows), or a less
   urgent priority than latency-sensitive work (ordering is priority-first). Observed
   while adversarially testing the limiter (ISSUES #26): 5 of 8 keys behind a throttled
   head never entered the window at all.

---

## 7. Optimization backlog — ordered by payoff

1. ✅ **Collapse the outcome transaction into one statement** — done (F4), later superseded by the batched group-commit flush (§1): a 4–5-stmt
   `BEGIN … COMMIT` became one data-modifying CTE, ~4 round-trips → 1, the biggest single
   win on the hot path. Asserted single-statement in `test/perf_test.exs`.
2. ✅ **Kill the per-step `load_signals` / `load_childs`** — done, and better than the
   capability-flag gating originally planned: the pick batch-enriches its whole claim set
   (2 statements per batch, zero per step), so even awaiting/spawning machines pay
   nothing per step. A plain `:next` step is now ~1 round-trip. Asserted in
   `test/perf_test.exs`.
3. ✅ **Statement caching** — done: every static statement goes through the
   connection-level prepared-statement cache (`cache_statement:`), so Postgres parses and
   plans each query once per connection. `deliver_signal` also collapsed from a
   5-round-trip transaction to one statement.
4. **Picker sharding by key hash** — removes the hot-key skip cost (limit #1). Unbuilt:
   waiting for a workload where one concurrency key dominates a queue.

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

Re-verified after the 0.2.0 hardening on a fresh 1M-row seed (same recipe), warm plans:
the common-path pick still rides `gen_durable_pick` with the rate CTEs **and the
cold-mint CTE** (`heal` at the time; since replaced by `r_cold`/`r_mint`, re-verified —
see §2b) at zero rows / `never executed` (~4 ms with a 6k-row executing set — the
concurrency guard's hashed scan of the in-flight set is the biggest component; on a
keyless queue with the full rate machinery active the pick is ~2 ms, the cold-key
exists-check costing ~0.01 ms); the reworked maintenance statements keep their
proportionality — reap ≈ 8 µs per expired row including the new ordered SKIP LOCKED
claim, a 50-row heartbeat ≈ 0.6 ms, both PK/partial-index driven; the one-statement
`deliver_signal` ≈ 0.27 ms, all PK scans; batch enrichment ≈ 0.1–0.14 ms per 50-row
batch; the collapsed outcome holds its bench win (~1.75× vs the transaction form,
`mix test --only bench`).
