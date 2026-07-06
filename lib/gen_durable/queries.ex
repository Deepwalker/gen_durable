defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement, one function each, as raw SQL.

  All functions take the `repo` explicitly. The `complete_*` functions run the
  outcome `UPDATE` and the consumed-signal `DELETE` in one transaction.
  `concurrency_key` serialization is enforced by the database — the UNIQUE
  partial index `gen_durable_concurrency_active` makes a second executing row
  per key uncommittable, so the pick's claim IS the lock (held exactly for the
  step window, released by any outcome or the reaper); no advisory locks, no
  pinned connections.

  Every statement has a static SQL text and goes through the connection-level
  prepared-statement cache (`cache_statement:`), so Postgres parses and plans it
  once per connection instead of on every call — bulk inserts pass rows as
  parallel arrays via `unnest` to keep the text static (and the parameter count
  fixed, clear of the 65535-parameter protocol cap). The one exception is
  `upsert_rate_configs` (dynamic VALUES, boot-time only). Hosts behind a
  transaction-pooling proxy set `prepare: :unnamed` on the repo to bypass the
  cache.

  JSON values (state, result, signal payloads) arrive here as encoded JSON text
  and are bound as TEXT parameters cast server-side (`$n::text::jsonb`). A bare
  `$n::jsonb` parameter would make the driver JSON-encode the already-encoded
  string, storing a double-encoded jsonb *scalar* instead of an object —
  invisible to `->>`/jsonb indexes. Rows written by versions that did exactly
  that still decode fine: the read paths accept both formats.
  """

  # --- shared insert shape -----------------------------------------------------
  # The 12 insert columns, used by insert / insert_all / complete_schedule_childs.

  @insert_cols "fsm, fsm_version, step, state, queue, priority, concurrency_key, eligible_at, correlation_key, correlation_scope, rate_limit, weight"

  # SELECT list decoding one unnest-ed row into the 12 columns above.
  # correlation_scope travels comma-joined and is split back server-side: unnest
  # zips scalar arrays element-wise, but an array-per-row column would need a
  # rectangular multidim array (enum labels contain no commas, so the join is safe).
  @unnest_row_select "SELECT t.fsm, t.fsm_version, t.step, t.state::jsonb, t.queue, t.priority, " <>
                       "t.concurrency_key, COALESCE(t.eligible_at, now()), t.correlation_key, " <>
                       "string_to_array(t.scope, ',')::durable_status[], t.rate_limit, t.weight"

  # The unnest source for @unnest_row_select: 12 parallel arrays at $base+1..$base+12.
  defp unnest_from(base) do
    "FROM unnest($#{base + 1}::text[], $#{base + 2}::int[], $#{base + 3}::text[], " <>
      "$#{base + 4}::text[], $#{base + 5}::text[], $#{base + 6}::int[], $#{base + 7}::text[], " <>
      "$#{base + 8}::timestamptz[], $#{base + 9}::text[], $#{base + 10}::text[], " <>
      "$#{base + 11}::text[], $#{base + 12}::float8[]) " <>
      "AS t(fsm, fsm_version, step, state, queue, priority, concurrency_key, eligible_at, " <>
      "correlation_key, scope, rate_limit, weight)"
  end

  # One list per insert column (parallel arrays for unnest), from a list of params maps.
  defp column_arrays(rows) do
    [
      Enum.map(rows, & &1.fsm),
      Enum.map(rows, & &1.fsm_version),
      Enum.map(rows, & &1.step),
      Enum.map(rows, & &1.state_json),
      Enum.map(rows, & &1.queue),
      Enum.map(rows, & &1.priority),
      Enum.map(rows, & &1.concurrency_key),
      Enum.map(rows, & &1.eligible_at),
      Enum.map(rows, & &1.correlation_key),
      Enum.map(rows, &Enum.join(&1.correlation_scope, ",")),
      Enum.map(rows, & &1.rate_limit),
      Enum.map(rows, & &1.weight)
    ]
  end

  # Every static statement goes through the connection-level prepared-statement
  # cache: parse + plan happen once per connection, not on every call. A host
  # behind a transaction-pooling proxy sets `prepare: :unnamed` on the repo,
  # which bypasses the cache gracefully. Dynamically-shaped SQL
  # (upsert_rate_configs) stays uncached.
  defp q!(repo, name, sql, params),
    do: repo.query!(sql, params, cache_statement: "gen_durable/" <> name)

  # An outcome's ownership guard matched (1, committed) or not (0, the worker no
  # longer owns the row — the outcome was dropped).
  defp committed?(1), do: :ok
  defp committed?(0), do: :stale

  # --- picker ----------------------------------------------------------------

  # concurrency_key dedup: never claim more than one row per concurrency_key
  # in a batch, and never claim a key that is already being processed — without
  # the wasted claim→violation→retry churn that prefetch would amplify on hot keys.
  #
  # Shape: the canonical Postgres claim (one `SELECT … FOR UPDATE SKIP LOCKED
  # LIMIT`, then one `UPDATE`) plus an in-batch dedup that needs no extra pass.
  #
  #   locked — the top `$2` runnable rows by (priority, eligible_at) via the
  #            `gen_durable_pick` index, locked in that one scan. Single-queue
  #            equality (`queue = $1`, not `ANY`) is what lets the index supply
  #            the order so the LIMIT stops early instead of scanning + sorting
  #            the whole runnable set (see PERFORMANCE.md). Rows whose key is
  #            already `executing` are excluded (NOT EXISTS); NULL keys never
  #            serialize, so they short-circuit the guard and a non-keyed
  #            queue pays nothing for it. `row_number()` over the *locked* set
  #            marks the most-urgent row per key (NULL keys fall back to id, so
  #            each is its own group and is never collapsed; the 'k:'/'i:'
  #            prefixes keep a numeric key from colliding with an id).
  #   UPDATE — flip only the per-key winners (`rn = 1`). The losers (`rn > 1`)
  #            were locked but not touched, so they stay `runnable` and their
  #            lock is released at commit. The next pick skips them via the
  #            NOT EXISTS guard while the winner executes. The guard is the
  #            optimization; the correctness is the UNIQUE arbiter on
  #            (concurrency_key) WHERE executing — a cross-node claim race
  #            cannot commit two rows of one key (see pick/6's retry).
  #
  # Dedup is a window function *after* locking, so there is no separate re-lock
  # pass: exactly ONE nested loop — the `UPDATE` join by id, which is the optimal
  # and unavoidable way to update N rows by primary key (forcing the planner off
  # it falls back to a full-table Seq Scan, ~10× slower; see PERFORMANCE.md).
  #
  # A same-key cluster filling the window can underfill the batch; completion-
  # driven refill closes the gap on the next pick.
  #
  # The pick combines three admissions in one statement — concurrency_key K=1
  # dedup (unconfigured keys), concurrency GATES (configured keys: a semaphore of
  # size `cap`, sharded), and token-bucket rate limiting:
  #
  #   winners  — unconfigured keys keep only their rn=1 row (the K=1 window
  #              dedup); GATED keys (name in concurrency_configs) keep ALL their
  #              candidate rows — their admission is capacity-based, below.
  #   c_heal   — recreate (full) a gated key's missing bucket shards (swept by
  #              GC while the key slept); invisible to c_locked this statement,
  #              admitted next pick. No-op on the common path.
  #   c_locked — lock ALL bucket shards of the gated winner keys, ordered
  #              (key, shard): grants are BATCHED — one lock pass per pick over
  #              the aggregate capacity — while releases are addressed per shard
  #              (see the credit riders in complete_*), which is the whole point
  #              of sharding: completions of one key serialize per shard row.
  #   c_ranges — cumulative admission ranges over shards, most-available first,
  #              so claims spread across shards and their release chains stay
  #              balanced. Deduplicated by (key, shard) with min(available):
  #              under concurrent bucket writes the locked scan was OBSERVED
  #              (bench, 8 pickers, one hot shard) to emit a bucket row twice —
  #              old and EPQ-refreshed version — doubling its range and
  #              over-admitting by up to its availability; min() keeps the
  #              conservative value, and the CHECK + pick retry backstop the
  #              rest.
  #   c_admit  — a gated row is admitted iff its per-key rank rn falls inside
  #              some shard's range; the shard is remembered on the row
  #              (concurrency_shard) so the release can credit it back addressed.
  #              Admission is computed from the LOCKED shard rows (fresh values
  #              after any lock wait), so concurrent picks cannot over-admit;
  #              the bucket CHECK (0 ≤ available ≤ cap) is the uncommittable
  #              backstop on top.
  #   pool     — the concurrency-admitted set (unconfigured rn=1 rows pass
  #              through), which the rate CTEs below then filter further; both
  #              writebacks are computed from the FINAL claimed set, so a row
  #              admitted by the gate but denied by its rate limit debits
  #              neither.
  #   cand     — top-$2 runnable rows, locked once (FOR UPDATE SKIP LOCKED via gen_durable_pick),
  #              with the per-concurrency_key window rank `rn`.
  #   winners  — the concurrency winners (rn = 1); add the cumulative weight `cw` of the urgency
  #              prefix per rate_limit bucket (ROWS, deterministic by id).
  #   heal     — recreate (full) any winner's bucket that is missing: buckets are minted by the
  #              ensure CTEs at transition time and swept by gc_buckets when idle, so a row that
  #              slept past its bucket's refill horizon (far-future schedule, long retry backoff)
  #              would otherwise be ungrantable forever — the pick self-heals instead. The insert
  #              is invisible to `locked` in this same statement (CTE snapshot), so the row is
  #              granted on the NEXT pick; the common path (no rate-limited winners) is a no-op.
  #   locked   — lock the rate-bucket rows the winners draw from (the cross-node serialization
  #              point), in key order: with ORDER BY the sort happens before LockRows, so every
  #              concurrent pick acquires bucket locks in the same order — no deadlock. Then
  #              refill them to `avail` (clock_timestamp, real elapsed).
  #   granted  — the prefix whose cumulative weight fits (`cw <= avail`); cw monotonic ⇒ a head
  #              that doesn't fit grants nothing (reservation, no skip-ahead).
  #   writeback — debit each bucket by the weight actually taken (max cw among its granted rows).
  # Final flip: a winner runs iff it has no rate_limit (NULL short-circuits everything above) or
  # it made the fitting prefix. Without any rate-limited rows, locked/avail/granted are empty and
  # this reduces to the plain concurrency pick.
  @pick_sql """
  WITH cand AS (
    SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at,
           row_number() OVER (PARTITION BY coalesce('k:' || concurrency_key, 'i:' || id::text)
                              ORDER BY priority, eligible_at) AS rn
    FROM (
      SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at
      FROM gen_durable g
      WHERE g.status = 'runnable' AND g.eligible_at <= now() AND g.queue = $1
        AND (g.concurrency_key IS NULL
             OR EXISTS (SELECT 1 FROM gen_durable_concurrency_configs cc
                        WHERE cc.name = split_part(g.concurrency_key, ':', 1))
             OR NOT EXISTS (
               SELECT 1 FROM gen_durable e
               WHERE e.concurrency_key = g.concurrency_key AND e.status = 'executing'))
      ORDER BY g.priority, g.eligible_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    ) s
  ),
  winners AS (
    SELECT c.id, c.concurrency_key, c.rate_limit, c.weight, c.priority, c.eligible_at, c.rn,
           (cc.name IS NOT NULL) AS gated
    FROM cand c
    LEFT JOIN gen_durable_concurrency_configs cc
           ON cc.name = split_part(c.concurrency_key, ':', 1)
    WHERE c.rn = 1 OR cc.name IS NOT NULL
  ),
  c_heal AS (
    INSERT INTO gen_durable_concurrency_buckets (key, shard, cap, available)
    SELECT k.concurrency_key, s.shard,
           (cc.cap / cc.shards) + CASE WHEN s.shard < (cc.cap % cc.shards) THEN 1 ELSE 0 END,
           (cc.cap / cc.shards) + CASE WHEN s.shard < (cc.cap % cc.shards) THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT concurrency_key FROM winners WHERE gated) k
    JOIN gen_durable_concurrency_configs cc ON cc.name = split_part(k.concurrency_key, ':', 1)
    CROSS JOIN LATERAL generate_series(0, cc.shards - 1) AS s(shard)
    WHERE NOT EXISTS (SELECT 1 FROM gen_durable_concurrency_buckets b
                      WHERE b.key = k.concurrency_key)
    ORDER BY k.concurrency_key, s.shard
    ON CONFLICT (key, shard) DO NOTHING
  ),
  c_locked AS (
    SELECT b.key, b.shard, b.available
    FROM gen_durable_concurrency_buckets b
    JOIN (SELECT DISTINCT concurrency_key FROM winners WHERE gated) k
      ON k.concurrency_key = b.key
    ORDER BY b.key, b.shard
    FOR UPDATE OF b
  ),
  c_ranges AS (
    SELECT key, shard, available,
           sum(available) OVER (PARTITION BY key ORDER BY available DESC, shard
                                ROWS UNBOUNDED PRECEDING) AS hi
    FROM (
      SELECT key, shard, min(available) AS available
      FROM c_locked
      GROUP BY key, shard
    ) d
  ),
  c_admit AS (
    SELECT w.id, r.shard
    FROM winners w
    JOIN c_ranges r ON r.key = w.concurrency_key
                   AND w.rn > r.hi - r.available AND w.rn <= r.hi
    WHERE w.gated
  ),
  pool AS (
    SELECT w.id, w.concurrency_key, w.rate_limit AS rkey, w.weight,
           w.priority, w.eligible_at, w.gated, a.shard AS c_shard
    FROM winners w
    LEFT JOIN c_admit a ON a.id = w.id
    WHERE (NOT w.gated) OR a.id IS NOT NULL
  ),
  rw AS (
    SELECT id, rkey, c_shard,
           sum(weight) OVER (PARTITION BY rkey ORDER BY priority, eligible_at, id
                             ROWS UNBOUNDED PRECEDING) AS cw
    FROM pool
  ),
  heal AS (
    INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill)
    SELECT k.rkey, cfg.burst, clock_timestamp()
    FROM (SELECT DISTINCT rkey FROM rw WHERE rkey IS NOT NULL) k
    JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k.rkey, ':', 1)
    WHERE NOT EXISTS (SELECT 1 FROM gen_durable_rate_buckets b WHERE b.key = k.rkey)
    ORDER BY k.rkey
    ON CONFLICT (key) DO NOTHING
  ),
  locked AS (
    SELECT b.key, b.tokens, b.last_refill, cfg.burst, cfg.rate
    FROM gen_durable_rate_buckets b
    JOIN (SELECT DISTINCT rkey FROM rw WHERE rkey IS NOT NULL) k ON k.rkey = b.key
    JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(b.key, ':', 1)
    ORDER BY b.key
    FOR UPDATE OF b
  ),
  avail AS (
    SELECT key, LEAST(burst, tokens + extract(epoch from clock_timestamp() - last_refill) * rate) AS avail
    FROM locked
  ),
  granted AS (
    SELECT w.id, w.rkey, w.cw FROM rw w JOIN avail a ON a.key = w.rkey WHERE w.cw <= a.avail
  ),
  consumed AS (
    SELECT rkey AS key, max(cw) AS consumed FROM granted GROUP BY rkey
  ),
  writeback AS (
    UPDATE gen_durable_rate_buckets b
    SET tokens = a.avail - coalesce(c.consumed, 0), last_refill = clock_timestamp()
    FROM avail a LEFT JOIN consumed c ON c.key = a.key
    WHERE b.key = a.key
  ),
  claimed AS (
    UPDATE gen_durable g
    SET status = 'executing', locked_by = $3, concurrency_shard = p.c_shard,
        lease_expires_at = now() + $4::int * interval '1 millisecond', updated_at = now()
    FROM rw p LEFT JOIN granted gr ON gr.id = p.id
    WHERE g.id = p.id AND (p.rkey IS NULL OR gr.id IS NOT NULL)
    RETURNING g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.concurrency_key,
              g.awaits, g.concurrency_shard
  ),
  c_writeback AS (
    UPDATE gen_durable_concurrency_buckets b
    SET available = b.available - d.cnt
    FROM (SELECT c.concurrency_key AS key, c.concurrency_shard AS shard, count(*) AS cnt
          FROM claimed c
          WHERE c.concurrency_shard IS NOT NULL
          GROUP BY 1, 2) d
    WHERE b.key = d.key AND b.shard = d.shard
  ),
  throttled AS (
    SELECT w.rkey AS key, count(*) AS wanted, count(gr.id) AS granted
    FROM rw w LEFT JOIN granted gr ON gr.id = w.id
    WHERE w.rkey IS NOT NULL
    GROUP BY w.rkey
    HAVING count(*) > count(gr.id)
  ),
  c_throttled AS (
    SELECT w.concurrency_key AS key, count(*) AS wanted, count(a.id) AS admitted
    FROM winners w LEFT JOIN c_admit a ON a.id = w.id
    WHERE w.gated
    GROUP BY 1
    HAVING count(*) > count(a.id)
  )
  SELECT 0 AS tag, id, fsm, fsm_version, step, state, attempt, concurrency_key, awaits,
         NULL::text AS rkey, NULL::bigint AS wanted, NULL::bigint AS granted
  FROM claimed
  UNION ALL
  SELECT 1, NULL::bigint, NULL::text, NULL::int, NULL::text, NULL::jsonb, NULL::int, NULL::text,
         NULL::text[], key, wanted, granted
  FROM throttled
  UNION ALL
  SELECT 2, NULL::bigint, NULL::text, NULL::int, NULL::text, NULL::jsonb, NULL::int, NULL::text,
         NULL::text[], key, wanted, admitted
  FROM c_throttled
  """

  def pick(repo, queue, batch, worker, lease_ttl_ms),
    do: pick(repo, queue, batch, worker, lease_ttl_ms, 3)

  # A cross-node claim race on a concurrency_key (two picks, different rows, one
  # key, both invisible to each other's snapshot) ends in a unique violation on
  # the gen_durable_concurrency_active arbiter — the DB-enforced serialization.
  # The violation aborts the whole claim statement, so the losing pick simply
  # retries: the winner is committed by then and the NOT EXISTS guard routes
  # around its key. Rare (the guard filters everything visible), observable via
  # [:gen_durable, :concurrency, :contended]; after the attempts run out this
  # round claims nothing and the next poll tries again.
  defp pick(repo, queue, batch, worker, lease_ttl_ms, attempts) do
    %{rows: rows} = q!(repo, "pick", @pick_sql, [queue, batch, worker, lease_ttl_ms])
    jobs = Enum.filter(rows, fn [tag | _] -> tag == 0 end)

    # a limit that wanted more than it admitted is biting — observable.
    for [tag, _, _, _, _, _, _, _, _, key, wanted, granted] <- rows, tag in [1, 2] do
      {event, measurements} =
        case tag do
          1 -> {[:gen_durable, :rate_limit, :throttled], %{wanted: wanted, granted: granted}}
          2 -> {[:gen_durable, :concurrency, :throttled], %{wanted: wanted, admitted: granted}}
        end

      :telemetry.execute(event, measurements, %{key: key, queue: queue})
    end

    enrich(repo, Enum.map(jobs, &to_job(&1, worker)))
  rescue
    e in Postgrex.Error ->
      # Two constraint-resolved races, one discipline (constraint = correctness,
      # retry = resolution): the K=1 unique arbiter, and the gate buckets' CHECK
      # (a residual over-admission race — see c_ranges' dedup — aborts the whole
      # claim instead of committing it).
      constraint = is_map(e.postgres) && e.postgres[:constraint]

      if constraint in [
           "gen_durable_concurrency_active",
           "gen_durable_concurrency_buckets_check"
         ] do
        :telemetry.execute([:gen_durable, :concurrency, :contended], %{count: 1}, %{queue: queue})

        if attempts > 1,
          do: pick(repo, queue, batch, worker, lease_ttl_ms, attempts - 1),
          else: []
      else
        reraise e, __STACKTRACE__
      end
  end

  # `worker` rides in the job: it is the claim's identity, and the outcome
  # queries require it (ownership guard).
  defp to_job(
         [_tag, id, fsm, fsm_version, step, state, attempt, concurrency_key, awaits | _],
         worker
       ) do
    %{
      id: id,
      fsm: fsm,
      fsm_version: fsm_version,
      step: step,
      state: state,
      attempt: attempt,
      concurrency_key: concurrency_key,
      awaits: awaits,
      worker: worker
    }
  end

  # Attach each claimed job's signal inbox and children — two statements per
  # BATCH, not two per job (the per-step loads were pure round-trip tax: the
  # probes usually return nothing, but each cost a network hop and a pool slot).
  # The inbox snapshot is therefore taken at pick time, not execution start;
  # consumption stays exact either way (a progressing outcome deletes the
  # `ctx.awaited` ids the step actually saw — a signal landing after the pick
  # just stays in the inbox for the next wake, same as one landing mid-step).
  # With `prefetch > 0`, buffered jobs hold their snapshot until they run.
  defp enrich(_repo, []), do: []

  defp enrich(repo, jobs) do
    ids = Enum.map(jobs, & &1.id)

    %{rows: sig_rows} =
      q!(
        repo,
        "batch_signals",
        "SELECT target_id, id, name, payload FROM signals WHERE target_id = ANY($1) ORDER BY id",
        [ids]
      )

    signals =
      Enum.group_by(sig_rows, &hd/1, fn [_, id, name, payload] ->
        %{id: id, name: name, payload: decode_json(payload)}
      end)

    %{rows: child_rows} =
      q!(
        repo,
        "batch_childs",
        """
        SELECT parent_id, id, fsm, status::text, state, result, last_error
        FROM gen_durable WHERE parent_id = ANY($1) ORDER BY id
        """,
        [ids]
      )

    childs =
      Enum.group_by(child_rows, &hd/1, fn [_, id, fsm, status, state, result, last_error] ->
        %{
          id: id,
          fsm: fsm,
          status: status,
          state: decode_json(state),
          result: decode_json_or_nil(result),
          last_error: last_error
        }
      end)

    Enum.map(jobs, fn job ->
      job
      |> Map.put(:signals, Map.get(signals, job.id, []))
      |> Map.put(:childs, Map.get(childs, job.id, []))
    end)
  end

  # --- lease / reaper --------------------------------------------------------
  #
  # Every multi-row maintenance statement (heartbeat, reap, release, startup
  # reclaim, bucket GC) claims its rows first via
  # `SELECT … ORDER BY … FOR UPDATE SKIP LOCKED`, then updates/deletes the
  # claimed set. Never waiting (SKIP LOCKED) plus deterministic order means no
  # two maintenance statements can deadlock against each other — an unordered
  # multi-row UPDATE locks rows in plan order, and e.g. a late heartbeat
  # (id order) overlapping the reaper (lease-index order) on two expired rows
  # would cycle. A row that is locked right now is being actively worked (its
  # outcome committing, a beat extending it) — exactly when maintenance should
  # leave it alone until the next tick.

  def heartbeat(_repo, [], _worker, _ttl), do: :ok

  def heartbeat(repo, ids, worker, lease_ttl_ms) when is_list(ids) do
    q!(
      repo,
      "heartbeat",
      """
      WITH mine AS (
        SELECT id FROM gen_durable
        WHERE id = ANY($1) AND locked_by = $2
        ORDER BY id
        FOR UPDATE SKIP LOCKED
      )
      UPDATE gen_durable g
      SET lease_expires_at = now() + $3::int * interval '1 millisecond', updated_at = now()
      FROM mine m
      WHERE g.id = m.id
      """,
      [ids, worker, lease_ttl_ms]
    )

    :ok
  end

  def reap(repo) do
    %{rows: rows} =
      q!(
        repo,
        "reap",
        """
        WITH doomed AS (
          SELECT id FROM gen_durable
          WHERE status = 'executing' AND lease_expires_at < now()
          ORDER BY id
          FOR UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', locked_by = null, lease_expires_at = null,
            attempt = attempt + 1, updated_at = now()
        FROM doomed d
        WHERE g.id = d.id
        RETURNING g.id
        """,
        []
      )

    List.flatten(rows)
  end

  # Wake parks whose await timeout fired: expired `await_deadline` ⇒ runnable,
  # deadline cleared, `awaits`/`attempt` KEPT (the woken step reads the inbox
  # through `awaits` as usual — an empty ctx.awaited is the timeout signal; this
  # is a wake, not a failure). Ordered SKIP LOCKED per the maintenance note above.
  def expire_awaits(repo) do
    %{num_rows: n} =
      q!(
        repo,
        "expire_awaits",
        """
        WITH expired AS (
          SELECT id FROM gen_durable
          WHERE status = 'awaiting_signal' AND await_deadline < now()
          ORDER BY id
          FOR UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', eligible_at = now(), await_deadline = null, updated_at = now()
        FROM expired e
        WHERE g.id = e.id
        """,
        []
      )

    n
  end

  # Garbage-collect terminal instances. Deletes up to `batch` `done`/`failed`
  # rows whose `updated_at` (their termination instant — terminal rows are immutable)
  # is older than `retention_ms`. The `NOT EXISTS` guard spares a terminal child whose
  # parent is still active (`awaiting_children`/runnable/executing): the parent may yet
  # read it via `ctx.childs` on the join. A deleted parent SET-NULLs its
  # children's `parent_id` (FK), and `signals` cascade-delete. Returns the count deleted.
  # Two round-trips on purpose (GC is a background sweep, not latency-critical):
  # collect ≤ `batch` doomed ids, then delete them by `id = ANY($ids)`. A single
  # `DELETE … WHERE id IN (subquery)` / `USING` makes the planner Seq Scan the whole
  # table to match the small id set — O(table) per sweep, seconds on a 100M-row table.
  # `id = ANY($ids)` is a PK Index Scan instead — O(batch) (~50ms for 10k incl. FK
  # cascades; see PERFORMANCE.md §4b). Terminal rows are immutable, so the ids stay
  # valid between the two statements.
  def gc(repo, retention_ms, batch) do
    %{rows: rows} =
      q!(
        repo,
        "gc_doomed",
        """
        SELECT g.id FROM gen_durable g
        WHERE g.status IN ('done', 'failed')
          AND g.updated_at < now() - $1::int * interval '1 millisecond'
          AND NOT EXISTS (
            SELECT 1 FROM gen_durable p
            WHERE p.id = g.parent_id AND p.status NOT IN ('done', 'failed')
          )
        LIMIT $2
        """,
        [retention_ms, batch]
      )

    case List.flatten(rows) do
      [] ->
        0

      ids ->
        %{num_rows: n} =
          q!(repo, "gc_delete", "DELETE FROM gen_durable WHERE id = ANY($1::bigint[])", [ids])

        n
    end
  end

  # Sweep stale rate buckets — the GC side of partitioned limits
  # (`{name, partition}` mints a bucket row per partition ever seen). A bucket
  # is deletable when recreating it equals its natural state: buckets are
  # recreated FULL (by the ensure CTEs at transition time, and by the pick's
  # `heal` CTE for rows that slept past the refill horizon), so one idle longer
  # than burst/rate seconds (fully refilled by now anyway) loses nothing.
  # rate = 0 never qualifies (it never refills, so delete-and-recreate would
  # grant a fresh burst), and a bucket whose config was removed is unusable
  # (both the pick and `ensure` join configs) — swept unconditionally. The
  # ordered SKIP LOCKED claim (see the lease/reaper note) also means a bucket a
  # concurrent pick holds is simply skipped — it is active, not stale.
  def gc_buckets(repo) do
    %{num_rows: n} =
      q!(
        repo,
        "gc_buckets",
        """
        WITH doomed AS (
          SELECT key FROM gen_durable_rate_buckets b
          WHERE NOT EXISTS (SELECT 1 FROM gen_durable_rate_configs cfg
                            WHERE cfg.name = split_part(b.key, ':', 1))
             OR EXISTS (SELECT 1 FROM gen_durable_rate_configs cfg
                        WHERE cfg.name = split_part(b.key, ':', 1) AND cfg.rate > 0
                          AND b.last_refill < now() - make_interval(secs => cfg.burst / cfg.rate))
          ORDER BY key
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM gen_durable_rate_buckets b
        USING doomed d
        WHERE b.key = d.key
        """,
        []
      )

    n
  end

  # --- step outcomes -----------------------------------------
  #
  # Each outcome is a SINGLE statement, not a transaction (one round-trip instead
  # of BEGIN + … + COMMIT) — except `:await`, which is deliberately a two-statement
  # transaction (park + recheck): the extra round trips close the lost-wakeup race
  # with deliver_signal (see complete_await). The signal consume rides along as a
  # data-modifying CTE, `consumed`, atomic with the outcome UPDATE because one
  # statement is its own implicit transaction; it runs to completion even though
  # the main query never reads it — Postgres guarantees data-modifying CTEs always
  # execute fully. What `consumed` deletes depends on the outcome:
  #   * progressing (:next / :schedule_childs) — exactly the awaited-signal ids the
  #     step received (`id = ANY($consumed)`); latecomers and non-awaited signals
  #     survive. Empty list ⇒ no-op.
  #   * terminal (:done / :stop) — the whole inbox (`target_id = $id`): the instance
  #     is finished, so nothing will read its signals again (cleanup).
  #   * :retry / :await consume nothing (the step is redone / still waiting).
  #
  # OWNERSHIP GUARD: every outcome commits only while the worker still owns the
  # claim — `locked_by = $worker AND status = 'executing'`. An orphaned task (its
  # scheduler crashed, so nobody heartbeats its rows) can outlive the lease; the
  # reaper then hands the row to a new claimant, and the orphan's late commit
  # must NOT land on top — unguarded it would rewind step/state mid-flight, null
  # the new claimant's locked_by (silencing its heartbeat), or, terminally,
  # delete the inbox and decrement the parent join barrier. Guarded, the stale
  # outcome affects zero rows and every side effect is gated on the guarded
  # UPDATE via CTE references (reading the update's RETURNING, never the table —
  # a table re-read would see the pre-update snapshot and fire the side effects
  # even when the guard EPQ-failed). Each complete_* returns :ok | :stale; the
  # executor emits [:gen_durable, :outcome, :stale] telemetry on the drop, and
  # the step's work is redone by the current claimant (at-least-once).

  # The join-barrier decrement, a CTE of the terminal outcomes. Reads the child's
  # `parent_id` from the guarded `terminal` CTE's RETURNING — empty when the
  # ownership guard failed, so a stale worker never touches the parent. No-op
  # when the row has no parent (the join yields nothing). The decrement that hits
  # zero releases the barrier; concurrent siblings serialize on the parent row
  # lock. `terminal` updates the child; this updates the parent (a different
  # row), so the two table-modifications never touch the same row. The final
  # SELECT reports whether the outcome committed (1) or was stale (0).
  @notify_parent """
  notify AS (
    UPDATE gen_durable p
    SET children_pending = p.children_pending - 1,
        status = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                      THEN 'runnable' ELSE p.status END,
        eligible_at = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                           THEN now() ELSE p.eligible_at END,
        updated_at = now()
    FROM terminal c
    WHERE c.parent_id = p.id
  )
  SELECT count(*) FROM terminal
  """

  # The concurrency-gate release, a rider CTE of every outcome: the row is
  # leaving `executing`, so its slot is credited back to the shard it was drawn
  # from. The OLD key/shard are read from the TABLE — sibling CTEs see the
  # statement snapshot, never the outcome UPDATE's writes, so this is the
  # pre-transition value even when the outcome rewrites the key — and the join
  # on the guarded CTE (`src`) gates it: a stale outcome credits nothing. A
  # missing bucket (swept) drops the credit in the conservative direction
  # (under-admission), healed by the GC reconciler.
  defp credit_gate(src) do
    "credit AS (UPDATE gen_durable_concurrency_buckets b " <>
      "SET available = available + 1 " <>
      "FROM gen_durable old JOIN #{src} gc ON gc.id = old.id " <>
      "WHERE old.concurrency_shard IS NOT NULL " <>
      "AND b.key = old.concurrency_key AND b.shard = old.concurrency_shard)"
  end

  # :next sets the row's rate_limit key ($5, NULL ⇒ not limited) and weight ($6) for the
  # next step, and ensures the bucket exists (full) so the picker's locked reserve
  # never races a missing row. The `ensure` CTE no-ops when $5 is NULL (split_part(NULL,…) is
  # NULL ⇒ matches no config), when the bucket already exists (ON CONFLICT DO
  # NOTHING), or when the ownership guard failed (committed empty).
  #
  # `concurrency_key` is :keep (the default — identity keys persist across
  # steps), nil (release the key for the next steps), or a new key. The shard is
  # always cleared (the row leaves executing; a re-claim assigns a fresh one)
  # and the old slot is credited via the `credit` rider.
  def complete_next(repo, id, worker, step, state_json, consumed_ids, rate_limit, weight, ck) do
    {ck_change, ck_value} = if ck == :keep, do: {false, nil}, else: {true, ck}

    %{rows: [[n]]} =
      q!(
        repo,
        "complete_next",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, status = 'runnable', eligible_at = now(),
              attempt = 0, awaits = null, rate_limit = $5, weight = $6,
              concurrency_key = CASE WHEN $8::bool THEN $9 ELSE concurrency_key END,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $7 AND status = 'executing'
          RETURNING id
        ),
        #{credit_gate("committed")},
        ensure AS (
          INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill)
          SELECT $5, cfg.burst, clock_timestamp()
          FROM gen_durable_rate_configs cfg
          WHERE cfg.name = split_part($5, ':', 1) AND EXISTS (SELECT 1 FROM committed)
          ON CONFLICT (key) DO NOTHING
        ),
        consumed AS (
          DELETE FROM signals
          WHERE target_id IN (SELECT id FROM committed) AND id = ANY($4::bigint[])
        )
        SELECT count(*) FROM committed
        """,
        [id, step, state_json, consumed_ids, rate_limit, weight, worker, ck_change, ck_value]
      )

    committed?(n)
  end

  # :retry redoes the same step, so it consumes nothing and KEEPS `awaits` — the
  # redo must see the same awaited signals it was handed. The gate slot is
  # released (the row leaves executing); the redo re-claims through admission.
  def complete_retry(repo, id, worker, state_json, delay_ms) do
    %{rows: [[n]]} =
      q!(
        repo,
        "complete_retry",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET state = $2::text::jsonb, status = 'runnable',
              eligible_at = now() + $3::int * interval '1 millisecond',
              attempt = attempt + 1, concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $4 AND status = 'executing'
          RETURNING id
        ),
        #{credit_gate("committed")}
        SELECT count(*) FROM committed
        """,
        [id, state_json, delay_ms, worker]
      )

    committed?(n)
  end

  # Park on a name set ($3, text[]), transitioning to `next_step` ($4): when any named
  # signal arrives the row becomes runnable at `next_step`, which reads the matching
  # subset as `ctx.awaited`.
  #
  # Two statements in ONE transaction — the park side of the lost-wakeup fix:
  #   1. park — flips to awaiting_signal and takes the row lock (held to commit).
  #   2. recheck — a fresh snapshot: a matching signal already in the inbox (its
  #      delivery committed before this statement) flips straight to runnable.
  # A delivery the recheck cannot see must commit after it — but its wake UPDATE
  # matches the row unconditionally (see deliver_signal), so it queues on our row
  # lock and performs the flip itself once we commit. Either the recheck or the
  # delivery wakes the row; no interleaving leaves a matching signal with a
  # parked instance. A single statement cannot do this: under READ COMMITTED its
  # EXISTS runs on the statement snapshot, blind to a concurrently-committing
  # delivery, while that delivery's status-guarded wake skips the not-yet-parked
  # row without locking. The extra round trips buy the race away.
  #
  # `presented_ids` — the awaited-signal ids the parking step was HANDED
  # (ctx.awaited; :await consumes nothing, so they are still in the inbox). The
  # recheck excludes them: waking on a signal the step already saw and chose to
  # re-await would spin the accumulate-a-pack pattern (park → recheck flips →
  # immediate re-pick → re-await → …) at full speed until the pack completes.
  # A set-difference, not a max-id watermark: signal ids commit out of order, so
  # "id greater than the newest presented" could skip a signal that was inserted
  # earlier but committed later (invisible to the step's enrichment snapshot).
  # Deliveries are unaffected — a delivered signal is a fresh insert, never in
  # the presented set, and the wake checks only the name.
  # `timeout_ms` (nil ⇒ no deadline) arms `await_deadline`: the reaper's sweep
  # returns an expired park to runnable, and the woken step sees whatever is in
  # the inbox — for a fresh await an empty ctx.awaited means timeout. The park is
  # the only writer of the column (NULL propagation: a nil timeout stores NULL),
  # so a stale deadline left on a woken row is always overwritten by the next park
  # and never matches the sweep's status filter in between.
  def complete_await(repo, id, worker, state_json, names, next_step, presented_ids, timeout_ms) do
    {:ok, result} =
      repo.transaction(fn ->
        %{rows: [[parked]]} =
          q!(
            repo,
            "await_park",
            """
            WITH park AS (
              UPDATE gen_durable
              SET step = $4, state = $2::text::jsonb, awaits = $3::text[], eligible_at = now(),
                  status = 'awaiting_signal', attempt = 0, rate_limit = null, weight = 1,
                  concurrency_shard = null,
                  await_deadline = now() + $6::int * interval '1 millisecond',
                  locked_by = null, lease_expires_at = null, updated_at = now()
              WHERE id = $1 AND locked_by = $5 AND status = 'executing'
              RETURNING id
            ),
            #{credit_gate("park")}
            SELECT count(*) FROM park
            """,
            [id, state_json, names, next_step, worker, timeout_ms]
          )

        # Guard failed ⇒ the row is someone else's now — skip the recheck (its
        # own claimant parks and rechecks for itself).
        if parked == 1 do
          q!(
            repo,
            "await_recheck",
            """
            UPDATE gen_durable
            SET status = 'runnable', updated_at = now()
            WHERE id = $1 AND status = 'awaiting_signal'
              AND EXISTS (SELECT 1 FROM signals
                          WHERE target_id = $1 AND name = ANY(gen_durable.awaits)
                            AND id != ALL($2::bigint[]))
            """,
            [id, presented_ids]
          )

          :ok
        else
          :stale
        end
      end)

    result
  end

  def complete_done(repo, id, worker, result_json) do
    %{rows: [[n]]} =
      q!(
        repo,
        "complete_done",
        """
        WITH terminal AS (
          UPDATE gen_durable
          SET result = $2::text::jsonb, status = 'done', awaits = null,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
        #{credit_gate("terminal")},
        consumed AS (
          DELETE FROM signals WHERE target_id IN (SELECT id FROM terminal)
        ),
        """ <> @notify_parent,
        [id, result_json, worker]
      )

    committed?(n)
  end

  def complete_stop(repo, id, worker, reason_text) do
    %{rows: [[n]]} =
      q!(
        repo,
        "complete_stop",
        """
        WITH terminal AS (
          UPDATE gen_durable
          SET status = 'failed', last_error = $2, awaits = null,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
        #{credit_gate("terminal")},
        consumed AS (
          DELETE FROM signals WHERE target_id IN (SELECT id FROM terminal)
        ),
        """ <> @notify_parent,
        [id, reason_text, worker]
      )

    committed?(n)
  end

  # :schedule_childs — spawn the batch and park the parent on the join
  # barrier, in one statement (consume + insert children + park). children_pending
  # is set to the number of children actually inserted; zero inserted ⇒ barrier
  # pre-satisfied ⇒ runnable.
  def complete_schedule_childs(repo, parent_id, worker, next_step, state_json, [], consumed_ids) do
    %{rows: [[n]]} =
      q!(
        repo,
        "schedule_childs_empty",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, children_pending = 0, status = 'runnable',
              eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $5 AND status = 'executing'
          RETURNING id
        ),
        #{credit_gate("committed")},
        consumed AS (
          DELETE FROM signals
          WHERE target_id IN (SELECT id FROM committed) AND id = ANY($4::bigint[])
        )
        SELECT count(*) FROM committed
        """,
        [parent_id, next_step, state_json, consumed_ids, worker]
      )

    committed?(n)
  end

  # Children ride in as 12 parallel arrays via `unnest` (base 5: $6..$17), so the
  # parameter count is fixed — 18 for any batch size (the wire protocol caps a
  # statement at 65535 parameters, ~5400 rows in per-row-placeholder form) — and
  # the SQL text is static (statement-cacheable). $1 doubles as the parent_id column.
  # The ownership guard lives in a leading `claim` CTE (SELECT … FOR UPDATE): the
  # children insert, the consume, and the parent park are all gated on it, because
  # the park needs the inserted-children count and so cannot itself be the guard.
  # Children insert ORDER BY correlation_key — same arbiter-deadlock discipline
  # as insert_all (see there).
  def complete_schedule_childs(
        repo,
        parent_id,
        worker,
        next_step,
        state_json,
        children,
        consumed_ids
      ) do
    sql =
      "WITH claim AS (SELECT id FROM gen_durable " <>
        "WHERE id = $1 AND locked_by = $18 AND status = 'executing' FOR UPDATE), " <>
        credit_gate("claim") <>
        ", " <>
        "ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
        "SELECT k, cfg.burst, clock_timestamp() FROM unnest($5::text[]) k " <>
        "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
        "WHERE EXISTS (SELECT 1 FROM claim) " <>
        "ORDER BY k ON CONFLICT (key) DO NOTHING), " <>
        ensure_gates_cte(19) <>
        ", " <>
        "consumed AS (DELETE FROM signals WHERE target_id IN (SELECT id FROM claim) " <>
        "AND id = ANY($4::bigint[])), " <>
        "ins AS (INSERT INTO gen_durable (#{@insert_cols}, parent_id) " <>
        @unnest_row_select <>
        ", $1 " <>
        unnest_from(5) <>
        " WHERE EXISTS (SELECT 1 FROM claim)" <>
        " ORDER BY t.correlation_key" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING 1), " <>
        "cnt AS (SELECT count(*) AS n FROM ins) " <>
        "UPDATE gen_durable SET step = $2, state = $3::text::jsonb, " <>
        "children_pending = (SELECT n FROM cnt), " <>
        "status = (CASE WHEN (SELECT n FROM cnt) = 0 THEN 'runnable' " <>
        "ELSE 'awaiting_children' END)::durable_status, " <>
        "eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1, " <>
        "concurrency_shard = null, " <>
        "locked_by = null, lease_expires_at = null, updated_at = now() " <>
        "WHERE id IN (SELECT id FROM claim)"

    args =
      [parent_id, next_step, state_json, consumed_ids, bucket_keys(children)] ++
        column_arrays(children) ++ [worker, gate_keys(children)]

    %{num_rows: n} = q!(repo, "schedule_childs", sql, args)
    committed?(n)
  end

  # --- signals ---------------------------------------------------------------

  def load_signals(repo, target_id) do
    %{rows: rows} =
      q!(
        repo,
        "load_signals",
        "SELECT id, name, payload FROM signals WHERE target_id = $1 ORDER BY id",
        [target_id]
      )

    Enum.map(rows, fn [id, name, payload] ->
      %{id: id, name: name, payload: decode_json(payload)}
    end)
  end

  # Deliver a signal in ONE statement (was BEGIN + resolve + INSERT + wake +
  # COMMIT = up to 5 round trips on the user-facing hot path). `target` is either
  # an internal id (integer) or a correlation_key (string, resolved via
  # `correlation_guard` — the partial unique index — to the single occupied
  # instance carrying it). Shape:
  #   target — the addressed LIVE instance (terminal and missing ids resolve to
  #            nothing: nobody will ever read their inbox, so the signal is
  #            refused as :no_target instead of durably storing garbage; we also
  #            never hold a signal for an instance that does not exist yet).
  #   ins    — the inbox row; ON CONFLICT makes dedup_key redelivery idempotent.
  #   wake   — the delivery side of the lost-wakeup fix. Joins target by id with
  #            the flip condition in CASE, not WHERE, so it always locks the
  #            row: racing a park (complete_await) it queues behind the park's
  #            row lock and re-evaluates the CASE against the committed row
  #            (READ COMMITTED follows the update chain), flipping the freshly-
  #            parked row. A status-guarded WHERE would skip the not-yet-parked
  #            row without locking or waiting — the lost wakeup.
  # ins and wake commit atomically (one statement), so the park's recheck sees
  # the signal exactly when the wake also happened. Returns :ok, or
  # {:error, :no_target} when the target does not resolve to a live instance.
  @signal_sql_body """
  ),
  ins AS (
    INSERT INTO signals (target_id, name, payload, dedup_key)
    SELECT id, $2, $3::text::jsonb, $4 FROM target
    ON CONFLICT (target_id, dedup_key) DO NOTHING
  ),
  wake AS (
    UPDATE gen_durable g
    SET status = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                      THEN 'runnable'::durable_status ELSE g.status END,
        eligible_at = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                           THEN now() ELSE g.eligible_at END,
        updated_at = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                          THEN now() ELSE g.updated_at END
    FROM target t
    WHERE g.id = t.id
  )
  SELECT count(*) FROM target
  """

  @signal_by_id """
                WITH target AS (
                  SELECT id FROM gen_durable WHERE id = $1 AND status NOT IN ('done', 'failed')
                """ <> @signal_sql_body

  @signal_by_key """
                 WITH target AS (
                   SELECT id FROM gen_durable
                   WHERE correlation_guard = $1 AND status NOT IN ('done', 'failed')
                 """ <> @signal_sql_body

  def deliver_signal(repo, target, name, payload_json, dedup_key) do
    {stmt, sql} =
      if is_integer(target),
        do: {"signal_by_id", @signal_by_id},
        else: {"signal_by_key", @signal_by_key}

    %{rows: [[n]]} = q!(repo, stmt, sql, [target, name, payload_json, dedup_key])
    if n == 1, do: :ok, else: {:error, :no_target}
  end

  # --- insert / batch insert -------------------------------------------------

  # Ensures a token bucket exists (full) for each given rate_limit key, as a leading CTE so
  # the insert stays one statement. `$n` is a text[] of distinct keys; empty ⇒ no-op.
  # ORDER BY k: deterministic insertion order — two statements creating the same new
  # keys via the arbiter index in opposite orders would deadlock.
  defp ensure_buckets_cte(n) do
    "ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
      "SELECT k, cfg.burst, clock_timestamp() FROM unnest($#{n}::text[]) k " <>
      "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
      "ORDER BY k ON CONFLICT (key) DO NOTHING)"
  end

  # Ensure a gated concurrency_key's bucket shards exist (full) at insert time,
  # so its first pick can grant without the heal-lag. `$n` is a text[] of the
  # rows' distinct concurrency keys; unconfigured names simply don't join the
  # configs and cost nothing. Per-shard cap distributes the remainder to the
  # low shards. Ordered insert (arbiter-deadlock discipline).
  defp ensure_gates_cte(n) do
    "c_ensure AS (INSERT INTO gen_durable_concurrency_buckets (key, shard, cap, available) " <>
      "SELECT k, s.shard, " <>
      "cc.cap / cc.shards + CASE WHEN s.shard < (cc.cap % cc.shards) THEN 1 ELSE 0 END, " <>
      "cc.cap / cc.shards + CASE WHEN s.shard < (cc.cap % cc.shards) THEN 1 ELSE 0 END " <>
      "FROM unnest($#{n}::text[]) k " <>
      "JOIN gen_durable_concurrency_configs cc ON cc.name = split_part(k, ':', 1) " <>
      "CROSS JOIN LATERAL generate_series(0, cc.shards - 1) AS s(shard) " <>
      "WHERE NOT EXISTS (SELECT 1 FROM gen_durable_concurrency_buckets b WHERE b.key = k) " <>
      "ORDER BY k, s.shard ON CONFLICT (key, shard) DO NOTHING)"
  end

  defp bucket_keys(rows),
    do: rows |> Enum.map(& &1.rate_limit) |> Enum.reject(&is_nil/1) |> Enum.uniq() |> Enum.sort()

  defp gate_keys(rows),
    do:
      rows
      |> Enum.map(& &1.concurrency_key)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.sort()

  # Seed/refresh the rate-limit policy table at engine start. `configs` is a list of
  # `%{name, rate, burst}`. Idempotent: re-running with changed numbers updates them.
  # Sorted by name: DO UPDATE locks existing rows in VALUES order, and two nodes
  # booting with differently-ordered configs would deadlock.
  def upsert_rate_configs(_repo, []), do: :ok

  def upsert_rate_configs(repo, configs) when is_list(configs) do
    configs = Enum.sort_by(configs, & &1.name)

    values =
      configs
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_c, i} -> "($#{i * 3 + 1}, $#{i * 3 + 2}, $#{i * 3 + 3})" end)

    args = Enum.flat_map(configs, &[&1.name, &1.rate, &1.burst])

    repo.query!(
      "INSERT INTO gen_durable_rate_configs (name, rate, burst) VALUES " <>
        values <>
        " ON CONFLICT (name) DO UPDATE SET rate = EXCLUDED.rate, burst = EXCLUDED.burst",
      args
    )

    :ok
  end

  # Seed/refresh the concurrency-gate policy table at engine start. `configs` is
  # a list of `%{name, cap, shards}`. Sorted by name (DO UPDATE locks rows in
  # VALUES order — arbiter-deadlock discipline). Bucket rows pick up a changed
  # cap/shards lazily via the GC reconciler.
  def upsert_concurrency_configs(_repo, []), do: :ok

  def upsert_concurrency_configs(repo, configs) when is_list(configs) do
    configs = Enum.sort_by(configs, & &1.name)

    values =
      configs
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_c, i} -> "($#{i * 3 + 1}, $#{i * 3 + 2}, $#{i * 3 + 3})" end)

    args = Enum.flat_map(configs, &[&1.name, &1.cap, &1.shards])

    repo.query!(
      "INSERT INTO gen_durable_concurrency_configs (name, cap, shards) VALUES " <>
        values <>
        " ON CONFLICT (name) DO UPDATE SET cap = EXCLUDED.cap, shards = EXCLUDED.shards",
      args
    )

    :ok
  end

  # The GC-riding healer for concurrency gates — the counters' source of truth
  # is the executing rows themselves, so any drift (crash leaks — conservative,
  # available too low; config cap/shards changes; bugs) is periodically repaired
  # from it. Exactness needs the park+recheck trick: lock the bucket rows first
  # (ordered SKIP LOCKED — a busy bucket is being actively debited/credited, skip
  # it until the next sweep), then, in FRESH-snapshot statements while the locks
  # are held (claims and credits of the locked keys wait on them), recompute and
  # repair. The final statement sweeps orphaned buckets (config removed, shard
  # out of range) and whole idle-full keys — deletable because the ensure/heal
  # CTEs recreate buckets FULL, which is exactly their idle state; deletion is
  # all-shards-or-nothing per key, because the pick's heal only recreates keys
  # with NO bucket rows at all. Returns the number of repaired + deleted rows.
  def reconcile_concurrency(repo) do
    {:ok, n} =
      repo.transaction(fn ->
        %{rows: locked} =
          q!(
            repo,
            "conc_lock",
            """
            SELECT key, shard FROM gen_durable_concurrency_buckets
            ORDER BY key, shard
            FOR UPDATE SKIP LOCKED
            """,
            []
          )

        case locked do
          [] ->
            0

          pairs ->
            keys = Enum.map(pairs, &hd/1)
            shards = Enum.map(pairs, &Enum.at(&1, 1))

            %{num_rows: healed} =
              q!(
                repo,
                "conc_heal",
                """
                WITH tgt AS (SELECT k, s FROM unnest($1::text[], $2::int[]) AS t(k, s)),
                held AS (
                  SELECT g.concurrency_key AS k, g.concurrency_shard AS s, count(*) AS n
                  FROM gen_durable g
                  WHERE g.status = 'executing' AND g.concurrency_shard IS NOT NULL
                  GROUP BY 1, 2
                )
                UPDATE gen_durable_concurrency_buckets b
                SET cap = calc.cap, available = calc.available
                FROM (
                  SELECT t.k, t.s,
                         (cc.cap / cc.shards +
                          CASE WHEN t.s < (cc.cap % cc.shards) THEN 1 ELSE 0 END) AS cap,
                         GREATEST(0, cc.cap / cc.shards +
                          CASE WHEN t.s < (cc.cap % cc.shards) THEN 1 ELSE 0 END
                          - coalesce(h.n, 0)) AS available
                  FROM tgt t
                  JOIN gen_durable_concurrency_configs cc ON cc.name = split_part(t.k, ':', 1)
                  LEFT JOIN held h ON h.k = t.k AND h.s = t.s
                  WHERE t.s < cc.shards
                ) calc
                WHERE b.key = calc.k AND b.shard = calc.s
                  AND (b.cap <> calc.cap OR b.available <> calc.available)
                """,
                [keys, shards]
              )

            %{num_rows: deleted} =
              q!(
                repo,
                "conc_sweep",
                """
                WITH tgt AS (SELECT k, s FROM unnest($1::text[], $2::int[]) AS t(k, s)),
                orphan AS (
                  SELECT t.k, t.s
                  FROM tgt t
                  LEFT JOIN gen_durable_concurrency_configs cc
                         ON cc.name = split_part(t.k, ':', 1)
                  WHERE (cc.name IS NULL OR t.s >= cc.shards)
                    AND NOT EXISTS (SELECT 1 FROM gen_durable g
                                    WHERE g.status = 'executing'
                                      AND g.concurrency_key = t.k
                                      AND g.concurrency_shard = t.s)
                ),
                idle AS (
                  SELECT b.key
                  FROM gen_durable_concurrency_buckets b
                  JOIN tgt t ON t.k = b.key AND t.s = b.shard
                  GROUP BY b.key
                  HAVING bool_and(b.available = b.cap)
                     AND count(*) = (SELECT count(*) FROM gen_durable_concurrency_buckets x
                                     WHERE x.key = b.key)
                )
                DELETE FROM gen_durable_concurrency_buckets b
                USING tgt t
                WHERE b.key = t.k AND b.shard = t.s
                  AND ((t.k, t.s) IN (SELECT k, s FROM orphan)
                       OR t.k IN (SELECT key FROM idle))
                """,
                [keys, shards]
              )

            healed + deleted
        end
      end)

    n
  end

  def insert(repo, p) do
    sql =
      "WITH " <>
        ensure_buckets_cte(13) <>
        ", " <>
        ensure_gates_cte(14) <>
        " INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        "($1, $2, $3, $4::text::jsonb, $5, $6, $7, COALESCE($8::timestamptz, now()), " <>
        "$9, $10::text[]::durable_status[], $11, $12)" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    case q!(repo, "insert", sql, row_args(p) ++ [bucket_keys([p]), gate_keys([p])]) do
      %{rows: [[id]]} -> {:ok, id}
      %{rows: []} -> {:error, :duplicate}
    end
  end

  # Rows ride in as 12 parallel arrays via `unnest` — 13 parameters for any batch
  # size (the wire protocol caps a statement at 65535 parameters, which the old
  # per-row-placeholder form hit at ~5400 rows) and a static, cacheable SQL text.
  # ORDER BY correlation_key: deterministic insertion order across nodes — two
  # concurrent batches inserting the same NEW keys in opposite orders would
  # deadlock on the correlation_guard arbiter index (each waits on the other's
  # uncommitted entry). Ids are therefore assigned in key order, not entry order
  # — indistinguishable to callers, since dropped duplicates already break any
  # positional mapping. Keyless rows never touch the arbiter; their order is
  # irrelevant.
  def insert_all(repo, rows) when is_list(rows) do
    sql =
      "WITH " <>
        ensure_buckets_cte(13) <>
        ", " <>
        ensure_gates_cte(14) <>
        " INSERT INTO gen_durable (#{@insert_cols}) " <>
        @unnest_row_select <>
        " " <>
        unnest_from(0) <>
        " ORDER BY t.correlation_key" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    %{rows: out} =
      q!(repo, "insert_all", sql, column_arrays(rows) ++ [bucket_keys(rows), gate_keys(rows)])

    List.flatten(out)
  end

  defp row_args(p) do
    [
      p.fsm,
      p.fsm_version,
      p.step,
      p.state_json,
      p.queue,
      p.priority,
      p.concurrency_key,
      p.eligible_at,
      p.correlation_key,
      p.correlation_scope,
      p.rate_limit,
      p.weight
    ]
  end

  # Startup reclaim: release rows still claimed by a dead predecessor of this
  # scheduler — same claim prefix (instance+queue+VM, see Scheduler.claim_prefix/2),
  # any incarnation suffix. Prefix-compared with `left(...)` rather than LIKE, so
  # queue/instance names containing `%`/`_` need no escaping.
  #
  # The lease-staleness condition ($3, `lease_ttl - 2 × heartbeat_interval` ms of
  # remaining lease at most) is the safety net for claim-prefix collisions: a
  # LIVE owner beats every heartbeat_interval, keeping its remaining lease above
  # the threshold, so its claims are never touched even if two VMs end up with
  # the same prefix (e.g. containers with identical hostnames and BEAM as OS
  # pid 1). A dead owner's lease decays below it after ~2 missed beats — the
  # reclaim fires then, still far ahead of full lease expiry (the reaper's
  # floor). Rows claimed moments before the predecessor died are the remainder;
  # they wait for the reaper. Scans the (small) executing set via the partial
  # lease index; runs once per scheduler boot.
  def reclaim_orphans(repo, queue, prefix, margin_ms) do
    %{num_rows: n} =
      q!(
        repo,
        "reclaim_orphans",
        """
        WITH stale AS (
          SELECT id FROM gen_durable
          WHERE queue = $1 AND status = 'executing'
            AND left(locked_by, char_length($2)) = $2
            AND lease_expires_at < now() + $3::int * interval '1 millisecond'
          ORDER BY id
          FOR UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
        FROM stale s
        WHERE g.id = s.id
        """,
        [queue, prefix, margin_ms]
      )

    n
  end

  # Release our still-claimed rows back to runnable on graceful shutdown, so the
  # buffered (un-started) work is picked up immediately instead of waiting out the
  # lease. Guarded by `locked_by` so we only ever release our own claims.
  def release(_repo, [], _worker), do: :ok

  def release(repo, ids, worker) when is_list(ids) do
    q!(
      repo,
      "release",
      """
      WITH mine AS (
        SELECT id FROM gen_durable
        WHERE id = ANY($1) AND locked_by = $2 AND status = 'executing'
        ORDER BY id
        FOR UPDATE SKIP LOCKED
      )
      UPDATE gen_durable g
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      FROM mine m
      WHERE g.id = m.id
      """,
      [ids, worker]
    )

    :ok
  end

  # The binary branch covers jsonb scalar-string rows written by versions that
  # double-encoded JSON params (≤ 0.1.8); new rows arrive as decoded maps.
  defp decode_json(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json(value) when is_map(value), do: value
  defp decode_json(nil), do: %{}

  defp decode_json_or_nil(nil), do: nil
  defp decode_json_or_nil(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json_or_nil(value) when is_map(value), do: value
end
