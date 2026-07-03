defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement, one function each, as raw SQL.

  All functions take the `repo` explicitly. The `complete_*` functions run the
  outcome `UPDATE` and the consumed-signal `DELETE` in one transaction.
  `concurrency_key` serialization (advisory lock) helpers live here too; the
  caller is responsible for holding a single connection (`Repo.checkout/1`) for
  the lock's lifetime.

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
  # in a batch, and never claim a key that is already being processed — without the
  # wasted claim→try_lock→reset churn that prefetch would amplify on hot keys.
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
  #            each is its own group and is never collapsed).
  #   UPDATE — flip only the per-key winners (`rn = 1`). The losers (`rn > 1`)
  #            were locked but not touched, so they stay `runnable` and their
  #            lock is released at commit — no advisory-lock bounce. The next
  #            pick skips them via the NOT EXISTS guard once the winner executes.
  #
  # Dedup is a window function *after* locking, so there is no separate re-lock
  # pass: exactly ONE nested loop — the `UPDATE` join by id, which is the optimal
  # and unavoidable way to update N rows by primary key (forcing the planner off
  # it falls back to a full-table Seq Scan, ~10× slower; see PERFORMANCE.md).
  #
  # A same-key cluster filling the window can underfill the batch; completion-
  # driven refill closes the gap on the next pick.
  # The pick combines concurrency_key dedup with token-bucket rate limiting
  # , in one statement:
  #   cand     — top-$2 runnable rows, locked once (FOR UPDATE SKIP LOCKED via gen_durable_pick),
  #              with the per-concurrency_key window rank `rn`.
  #   winners  — the concurrency winners (rn = 1); add the cumulative weight `cw` of the urgency
  #              prefix per rate_limit bucket (ROWS, deterministic by id).
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
    SELECT id, rate_limit, weight, priority, eligible_at,
           row_number() OVER (PARTITION BY coalesce(concurrency_key, id::text)
                              ORDER BY priority, eligible_at) AS rn
    FROM (
      SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at
      FROM gen_durable g
      WHERE g.status = 'runnable' AND g.eligible_at <= now() AND g.queue = $1
        AND (g.concurrency_key IS NULL OR NOT EXISTS (
          SELECT 1 FROM gen_durable e
          WHERE e.concurrency_key = g.concurrency_key AND e.status = 'executing'
        ))
      ORDER BY g.priority, g.eligible_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    ) s
  ),
  winners AS (
    SELECT id, rate_limit AS rkey,
           sum(weight) OVER (PARTITION BY rate_limit
                             ORDER BY priority, eligible_at, id
                             ROWS UNBOUNDED PRECEDING) AS cw
    FROM cand
    WHERE rn = 1
  ),
  locked AS (
    SELECT b.key, b.tokens, b.last_refill, cfg.burst, cfg.rate
    FROM gen_durable_rate_buckets b
    JOIN (SELECT DISTINCT rkey FROM winners WHERE rkey IS NOT NULL) k ON k.rkey = b.key
    JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(b.key, ':', 1)
    ORDER BY b.key
    FOR UPDATE OF b
  ),
  avail AS (
    SELECT key, LEAST(burst, tokens + extract(epoch from clock_timestamp() - last_refill) * rate) AS avail
    FROM locked
  ),
  granted AS (
    SELECT w.id, w.rkey, w.cw FROM winners w JOIN avail a ON a.key = w.rkey WHERE w.cw <= a.avail
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
    SET status = 'executing', locked_by = $3,
        lease_expires_at = now() + $4::int * interval '1 millisecond', updated_at = now()
    FROM winners w LEFT JOIN granted gr ON gr.id = w.id
    WHERE g.id = w.id AND (w.rkey IS NULL OR gr.id IS NOT NULL)
    RETURNING g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.concurrency_key, g.awaits
  ),
  throttled AS (
    SELECT w.rkey AS key, count(*) AS wanted, count(gr.id) AS granted
    FROM winners w LEFT JOIN granted gr ON gr.id = w.id
    WHERE w.rkey IS NOT NULL
    GROUP BY w.rkey
    HAVING count(*) > count(gr.id)
  )
  SELECT 0 AS tag, id, fsm, fsm_version, step, state, attempt, concurrency_key, awaits,
         NULL::text AS rkey, NULL::bigint AS wanted, NULL::bigint AS granted
  FROM claimed
  UNION ALL
  SELECT 1, NULL::bigint, NULL::text, NULL::int, NULL::text, NULL::jsonb, NULL::int, NULL::text,
         NULL::text[], key, wanted, granted
  FROM throttled
  """

  def pick(repo, queue, batch, worker, lease_ttl_ms) do
    %{rows: rows} = q!(repo, "pick", @pick_sql, [queue, batch, worker, lease_ttl_ms])
    {jobs, throttles} = Enum.split_with(rows, fn [tag | _] -> tag == 0 end)

    # a bucket that wanted more than it granted is biting — observable.
    for [_, _, _, _, _, _, _, _, _, key, wanted, granted] <- throttles do
      :telemetry.execute(
        [:gen_durable, :rate_limit, :throttled],
        %{wanted: wanted, granted: granted},
        %{key: key, queue: queue}
      )
    end

    Enum.map(jobs, &to_job(&1, worker))
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

  # --- lease / reaper --------------------------------------------------------

  def heartbeat(_repo, [], _worker, _ttl), do: :ok

  def heartbeat(repo, ids, worker, lease_ttl_ms) when is_list(ids) do
    q!(
      repo,
      "heartbeat",
      """
      UPDATE gen_durable
      SET lease_expires_at = now() + $3::int * interval '1 millisecond', updated_at = now()
      WHERE id = ANY($1) AND locked_by = $2
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
        UPDATE gen_durable
        SET status = 'runnable', locked_by = null, lease_expires_at = null,
            attempt = attempt + 1, updated_at = now()
        WHERE status = 'executing' AND lease_expires_at < now()
        RETURNING id
        """,
        []
      )

    List.flatten(rows)
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

  # :next sets the row's rate_limit key ($5, NULL ⇒ not limited) and weight ($6) for the
  # next step, and ensures the bucket exists (full) so the picker's locked reserve
  # never races a missing row. The `ensure` CTE no-ops when $5 is NULL (split_part(NULL,…) is
  # NULL ⇒ matches no config), when the bucket already exists (ON CONFLICT DO
  # NOTHING), or when the ownership guard failed (committed empty).
  def complete_next(repo, id, worker, step, state_json, consumed_ids, rate_limit, weight) do
    %{rows: [[n]]} =
      q!(
        repo,
        "complete_next",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, status = 'runnable', eligible_at = now(),
              attempt = 0, awaits = null, rate_limit = $5, weight = $6,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $7 AND status = 'executing'
          RETURNING id
        ),
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
        [id, step, state_json, consumed_ids, rate_limit, weight, worker]
      )

    committed?(n)
  end

  # :retry redoes the same step, so it consumes nothing and KEEPS `awaits` — the
  # redo must see the same awaited signals it was handed.
  def complete_retry(repo, id, worker, state_json, delay_ms) do
    %{num_rows: n} =
      q!(
        repo,
        "complete_retry",
        """
        UPDATE gen_durable
        SET state = $2::text::jsonb, status = 'runnable',
            eligible_at = now() + $3::int * interval '1 millisecond',
            attempt = attempt + 1, locked_by = null, lease_expires_at = null,
            updated_at = now()
        WHERE id = $1 AND locked_by = $4 AND status = 'executing'
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
  def complete_await(repo, id, worker, state_json, names, next_step) do
    {:ok, result} =
      repo.transaction(fn ->
        %{num_rows: parked} =
          q!(
            repo,
            "await_park",
            """
            UPDATE gen_durable
            SET step = $4, state = $2::text::jsonb, awaits = $3::text[], eligible_at = now(),
                status = 'awaiting_signal', rate_limit = null, weight = 1,
                locked_by = null, lease_expires_at = null, updated_at = now()
            WHERE id = $1 AND locked_by = $5 AND status = 'executing'
            """,
            [id, state_json, names, next_step, worker]
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
                          WHERE target_id = $1 AND name = ANY(gen_durable.awaits))
            """,
            [id]
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
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
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
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
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
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $5 AND status = 'executing'
          RETURNING id
        ),
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
        "ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
        "SELECT k, cfg.burst, clock_timestamp() FROM unnest($5::text[]) k " <>
        "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
        "WHERE EXISTS (SELECT 1 FROM claim) " <>
        "ORDER BY k ON CONFLICT (key) DO NOTHING), " <>
        "consumed AS (DELETE FROM signals WHERE target_id IN (SELECT id FROM claim) " <>
        "AND id = ANY($4::bigint[])), " <>
        "ins AS (INSERT INTO gen_durable (#{@insert_cols}, parent_id) " <>
        @unnest_row_select <>
        ", $1 " <>
        unnest_from(5) <>
        " WHERE EXISTS (SELECT 1 FROM claim)" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING 1), " <>
        "cnt AS (SELECT count(*) AS n FROM ins) " <>
        "UPDATE gen_durable SET step = $2, state = $3::text::jsonb, " <>
        "children_pending = (SELECT n FROM cnt), " <>
        "status = (CASE WHEN (SELECT n FROM cnt) = 0 THEN 'runnable' " <>
        "ELSE 'awaiting_children' END)::durable_status, " <>
        "eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1, " <>
        "locked_by = null, lease_expires_at = null, updated_at = now() " <>
        "WHERE id IN (SELECT id FROM claim)"

    args =
      [parent_id, next_step, state_json, consumed_ids, bucket_keys(children)] ++
        column_arrays(children) ++ [worker]

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

  # A parent's children, for ctx.childs on wake-up.
  def load_childs(repo, parent_id) do
    %{rows: rows} =
      q!(
        repo,
        "load_childs",
        """
        SELECT id, fsm, status::text, state, result, last_error
        FROM gen_durable WHERE parent_id = $1 ORDER BY id
        """,
        [parent_id]
      )

    Enum.map(rows, fn [id, fsm, status, state, result, last_error] ->
      %{
        id: id,
        fsm: fsm,
        status: status,
        state: decode_json(state),
        result: decode_json_or_nil(result),
        last_error: last_error
      }
    end)
  end

  # `target` is either an internal id (integer) or a correlation_key (string). The signal
  # row is inserted before the wake-flip so the woken step already sees it.
  # Returns `:ok`, or `{:error, :no_target}` when a correlation_key resolves to no
  # occupied instance.
  def deliver_signal(repo, target, name, payload_json, dedup_key) do
    repo.transaction(fn ->
      case resolve_target(repo, target) do
        nil ->
          repo.rollback(:no_target)

        id ->
          q!(
            repo,
            "signal_insert",
            """
            INSERT INTO signals (target_id, name, payload, dedup_key)
            VALUES ($1, $2, $3::text::jsonb, $4)
            ON CONFLICT (target_id, dedup_key) DO NOTHING
            """,
            [id, name, payload_json, dedup_key]
          )

          # Wake flip — the delivery side of the lost-wakeup fix. Matches by id
          # alone (the flip condition lives in CASE, not WHERE) so it always
          # locks a live row: racing a park (complete_await) it queues behind
          # the park's row lock and re-evaluates the CASE against the committed
          # row (READ COMMITTED follows the update chain), flipping the freshly-
          # parked row. A status-guarded WHERE would skip the not-yet-parked row
          # without locking or waiting — the lost wakeup. Terminal rows are
          # excluded: outcomes are one-shot, so no park can be in flight for
          # them, and skipping keeps them out of the wake path entirely.
          q!(
            repo,
            "signal_wake",
            """
            UPDATE gen_durable
            SET status = CASE WHEN status = 'awaiting_signal' AND $2 = ANY(awaits)
                              THEN 'runnable'::durable_status ELSE status END,
                eligible_at = CASE WHEN status = 'awaiting_signal' AND $2 = ANY(awaits)
                                   THEN now() ELSE eligible_at END,
                updated_at = CASE WHEN status = 'awaiting_signal' AND $2 = ANY(awaits)
                                  THEN now() ELSE updated_at END
            WHERE id = $1 AND status NOT IN ('done', 'failed')
            """,
            [id, name]
          )
      end
    end)
    |> case do
      {:ok, _} -> :ok
      {:error, :no_target} -> {:error, :no_target}
    end
  end

  # An integer target is the id itself (the FK enforces existence). A string is a
  # correlation_key — resolved via `correlation_guard` (the partial unique index) to
  # the single occupied instance carrying it, or nil if none: an instance whose key is
  # no longer occupied (its status left correlation_scope) can no longer be woken, and we
  # do not durably hold a signal for an instance that does not exist yet.
  defp resolve_target(_repo, id) when is_integer(id), do: id

  defp resolve_target(repo, key) when is_binary(key) do
    case q!(repo, "resolve_target", "SELECT id FROM gen_durable WHERE correlation_guard = $1", [
           key
         ]) do
      %{rows: [[id]]} -> id
      %{rows: []} -> nil
    end
  end

  # --- insert / batch insert -------------------------------------------------

  # Ensures a token bucket exists (full) for each given rate_limit key, as a leading CTE so
  # the insert stays one statement. `$n` is a text[] of distinct keys; empty ⇒ no-op.
  # ORDER BY k: deterministic insertion order — two statements creating the same new
  # keys via the arbiter index in opposite orders would deadlock.
  defp ensure_buckets_cte(n) do
    "WITH ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
      "SELECT k, cfg.burst, clock_timestamp() FROM unnest($#{n}::text[]) k " <>
      "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
      "ORDER BY k ON CONFLICT (key) DO NOTHING) "
  end

  defp bucket_keys(rows),
    do: rows |> Enum.map(& &1.rate_limit) |> Enum.reject(&is_nil/1) |> Enum.uniq() |> Enum.sort()

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

  def insert(repo, p) do
    sql =
      ensure_buckets_cte(12 + 1) <>
        "INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        "($1, $2, $3, $4::text::jsonb, $5, $6, $7, COALESCE($8::timestamptz, now()), " <>
        "$9, $10::text[]::durable_status[], $11, $12)" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    case q!(repo, "insert", sql, row_args(p) ++ [bucket_keys([p])]) do
      %{rows: [[id]]} -> {:ok, id}
      %{rows: []} -> {:error, :duplicate}
    end
  end

  # Rows ride in as 12 parallel arrays via `unnest` — 13 parameters for any batch
  # size (the wire protocol caps a statement at 65535 parameters, which the old
  # per-row-placeholder form hit at ~5400 rows) and a static, cacheable SQL text.
  def insert_all(repo, rows) when is_list(rows) do
    sql =
      ensure_buckets_cte(12 + 1) <>
        "INSERT INTO gen_durable (#{@insert_cols}) " <>
        @unnest_row_select <>
        " " <>
        unnest_from(0) <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    %{rows: out} = q!(repo, "insert_all", sql, column_arrays(rows) ++ [bucket_keys(rows)])
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

  # --- concurrency_key serialization (advisory lock) ---------------------------

  def advisory_try_lock(repo, key) do
    %{rows: [[locked]]} =
      q!(repo, "advisory_lock", "SELECT pg_try_advisory_lock(hashtext($1))", [key])

    locked
  end

  def advisory_unlock(repo, key) do
    q!(repo, "advisory_unlock", "SELECT pg_advisory_unlock(hashtext($1))", [key])
    :ok
  end

  def reset_to_runnable(repo, id, worker) do
    q!(
      repo,
      "reset_to_runnable",
      """
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1 AND locked_by = $2 AND status = 'executing'
      """,
      [id, worker]
    )

    :ok
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
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = ANY($1) AND locked_by = $2 AND status = 'executing'
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
