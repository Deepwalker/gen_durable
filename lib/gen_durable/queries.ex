defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement from spec §10, one function each, as raw SQL.

  All functions take the `repo` explicitly. The `complete_*` functions run the
  outcome `UPDATE` and the consumed-signal `DELETE` in one transaction.
  `concurrency_key` serialization (advisory lock) helpers live here too; the
  caller is responsible for holding a single connection (`Repo.checkout/1`) for
  the lock's lifetime.
  """

  # --- picker ----------------------------------------------------------------

  # concurrency_key dedup (spec §6): never claim more than one row per concurrency_key
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
  # The pick combines concurrency_key dedup (spec §6) with token-bucket rate limiting
  # (spec §12), in one statement:
  #   cand     — top-$2 runnable rows, locked once (FOR UPDATE SKIP LOCKED via gen_durable_pick),
  #              with the per-concurrency_key window rank `rn`.
  #   winners  — the concurrency winners (rn = 1); add the cumulative weight `cw` of the urgency
  #              prefix per rate_limit bucket (ROWS, deterministic by id).
  #   locked   — lock the rate-bucket rows the winners draw from (the cross-node serialization
  #              point); refill them to `avail` (clock_timestamp, real elapsed).
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
    %{rows: rows} = repo.query!(@pick_sql, [queue, batch, worker, lease_ttl_ms])
    {jobs, throttles} = Enum.split_with(rows, fn [tag | _] -> tag == 0 end)

    # spec §12: a bucket that wanted more than it granted is biting — observable.
    for [_, _, _, _, _, _, _, _, _, key, wanted, granted] <- throttles do
      :telemetry.execute(
        [:gen_durable, :rate_limit, :throttled],
        %{wanted: wanted, granted: granted},
        %{key: key, queue: queue}
      )
    end

    Enum.map(jobs, &to_job/1)
  end

  defp to_job([_tag, id, fsm, fsm_version, step, state, attempt, concurrency_key, awaits | _]) do
    %{
      id: id,
      fsm: fsm,
      fsm_version: fsm_version,
      step: step,
      state: state,
      attempt: attempt,
      concurrency_key: concurrency_key,
      awaits: awaits
    }
  end

  # --- lease / reaper --------------------------------------------------------

  def heartbeat(_repo, [], _worker, _ttl), do: :ok

  def heartbeat(repo, ids, worker, lease_ttl_ms) when is_list(ids) do
    repo.query!(
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
      repo.query!("""
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null,
          attempt = attempt + 1, updated_at = now()
      WHERE status = 'executing' AND lease_expires_at < now()
      RETURNING id
      """)

    List.flatten(rows)
  end

  # Garbage-collect terminal instances (spec §8). Deletes up to `batch` `done`/`failed`
  # rows whose `updated_at` (their termination instant — terminal rows are immutable)
  # is older than `retention_ms`. The `NOT EXISTS` guard spares a terminal child whose
  # parent is still active (`awaiting_children`/runnable/executing): the parent may yet
  # read it via `ctx.childs` on the join (spec §11). A deleted parent SET-NULLs its
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
      repo.query!(
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
          repo.query!("DELETE FROM gen_durable WHERE id = ANY($1::bigint[])", [ids])

        n
    end
  end

  # --- step outcomes (spec §3 / §10) -----------------------------------------
  #
  # Each outcome is a SINGLE statement, not a transaction (one round-trip instead
  # of BEGIN + … + COMMIT). The signal consume (spec §5) rides along as a leading
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

  # The join-barrier decrement (spec §11), used as the main statement of the
  # terminal outcomes after the `consumed` + `terminal` CTEs. No-op when the row
  # has no parent (the join yields nothing). The decrement that hits zero releases
  # the barrier; concurrent siblings serialize on the parent row lock. The
  # `terminal` CTE updates the child (id = $1); this updates the parent (a
  # different row), reading the child's pre-statement `parent_id` under the shared
  # snapshot — so the two table-modifications never touch the same row.
  @notify_parent """
  UPDATE gen_durable p
  SET children_pending = p.children_pending - 1,
      status = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                    THEN 'runnable' ELSE p.status END,
      eligible_at = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                         THEN now() ELSE p.eligible_at END,
      updated_at = now()
  FROM gen_durable c
  WHERE c.id = $1 AND c.parent_id = p.id
  """

  # :next sets the row's rate_limit key ($5, NULL ⇒ not limited) and weight ($6) for the
  # next step (spec §12), and ensures the bucket exists (full) so the picker's locked reserve
  # never races a missing row. The `ensure` CTE no-ops when $5 is NULL (split_part(NULL,…) is
  # NULL ⇒ matches no config) or when the bucket already exists (ON CONFLICT DO NOTHING).
  def complete_next(repo, id, step, state_json, consumed_ids, rate_limit, weight) do
    repo.query!(
      """
      WITH ensure AS (
        INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill)
        SELECT $5, cfg.burst, clock_timestamp()
        FROM gen_durable_rate_configs cfg WHERE cfg.name = split_part($5, ':', 1)
        ON CONFLICT (key) DO NOTHING
      ),
      consumed AS (
        DELETE FROM signals WHERE target_id = $1 AND id = ANY($4::bigint[])
      )
      UPDATE gen_durable
      SET step = $2, state = $3::jsonb, status = 'runnable', eligible_at = now(),
          attempt = 0, awaits = null, rate_limit = $5, weight = $6,
          locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [id, step, state_json, consumed_ids, rate_limit, weight]
    )

    :ok
  end

  # :retry redoes the same step, so it consumes nothing and KEEPS `awaits` — the
  # redo must see the same awaited signals it was handed.
  def complete_retry(repo, id, state_json, delay_ms) do
    repo.query!(
      """
      UPDATE gen_durable
      SET state = $2::jsonb, status = 'runnable',
          eligible_at = now() + $3::int * interval '1 millisecond',
          attempt = attempt + 1, locked_by = null, lease_expires_at = null,
          updated_at = now()
      WHERE id = $1
      """,
      [id, state_json, delay_ms]
    )

    :ok
  end

  # Park on a name set ($3, text[]), transitioning to `next_step` ($4): when any named
  # signal arrives the row becomes runnable at `next_step`, which reads the matching
  # subset as `ctx.awaited`. If a matching signal is already in the inbox (it arrived
  # before this park committed), go straight to runnable so `next_step` runs at once —
  # closing the lost-wakeup race.
  def complete_await(repo, id, state_json, names, next_step) do
    repo.query!(
      """
      UPDATE gen_durable
      SET step = $4, state = $2::jsonb, awaits = $3::text[], eligible_at = now(),
          status = (CASE WHEN EXISTS (
                      SELECT 1 FROM signals WHERE target_id = $1 AND name = ANY($3::text[]))
                    THEN 'runnable' ELSE 'awaiting_signal' END)::durable_status,
          rate_limit = null, weight = 1,
          locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [id, state_json, names, next_step]
    )

    :ok
  end

  def complete_done(repo, id, result_json) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals WHERE target_id = $1
      ),
      terminal AS (
        UPDATE gen_durable
        SET result = $2::jsonb, status = 'done', awaits = null,
            locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
      )
      """ <> @notify_parent,
      [id, result_json]
    )

    :ok
  end

  def complete_stop(repo, id, reason_text) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals WHERE target_id = $1
      ),
      terminal AS (
        UPDATE gen_durable
        SET status = 'failed', last_error = $2, awaits = null,
            locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
      )
      """ <> @notify_parent,
      [id, reason_text]
    )

    :ok
  end

  # :schedule_childs (spec §11) — spawn the batch and park the parent on the join
  # barrier, in one statement (consume + insert children + park). children_pending
  # is set to the number of children actually inserted; zero inserted ⇒ barrier
  # pre-satisfied ⇒ runnable.
  def complete_schedule_childs(repo, parent_id, next_step, state_json, [], consumed_ids) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals WHERE target_id = $1 AND id = ANY($4::bigint[])
      )
      UPDATE gen_durable
      SET step = $2, state = $3::jsonb, children_pending = 0, status = 'runnable',
          eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1,
          locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [parent_id, next_step, state_json, consumed_ids]
    )

    :ok
  end

  def complete_schedule_childs(repo, parent_id, next_step, state_json, children, consumed_ids) do
    placeholders =
      children
      |> Enum.with_index()
      |> Enum.map(fn {_p, i} -> child_row_placeholders(4 + i * 12) end)

    keys_n = 4 + length(children) * 12 + 1

    sql =
      "WITH ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
        "SELECT k, cfg.burst, clock_timestamp() FROM unnest($#{keys_n}::text[]) k " <>
        "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
        "ON CONFLICT (key) DO NOTHING), " <>
        "consumed AS (DELETE FROM signals WHERE target_id = $1 AND id = ANY($4::bigint[])), " <>
        "ins AS (INSERT INTO gen_durable " <>
        "(fsm, fsm_version, step, state, queue, priority, concurrency_key, " <>
        "eligible_at, correlation_key, correlation_scope, rate_limit, weight, parent_id) VALUES " <>
        Enum.join(placeholders, ", ") <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING 1), " <>
        "cnt AS (SELECT count(*) AS n FROM ins) " <>
        "UPDATE gen_durable SET step = $2, state = $3::jsonb, " <>
        "children_pending = (SELECT n FROM cnt), " <>
        "status = (CASE WHEN (SELECT n FROM cnt) = 0 THEN 'runnable' " <>
        "ELSE 'awaiting_children' END)::durable_status, " <>
        "eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1, " <>
        "locked_by = null, lease_expires_at = null, updated_at = now() WHERE id = $1"

    args =
      [parent_id, next_step, state_json, consumed_ids] ++
        Enum.flat_map(children, &row_args/1) ++ [bucket_keys(children)]

    repo.query!(sql, args)
    :ok
  end

  # Child placeholders share $1 (the parent id) as the parent_id column.
  defp child_row_placeholders(base) do
    "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}::jsonb, $#{base + 5}, " <>
      "$#{base + 6}, $#{base + 7}, COALESCE($#{base + 8}::timestamptz, now()), " <>
      "$#{base + 9}, $#{base + 10}::text[]::durable_status[], $#{base + 11}, $#{base + 12}, $1)"
  end

  # --- signals ---------------------------------------------------------------

  def load_signals(repo, target_id) do
    %{rows: rows} =
      repo.query!(
        "SELECT id, name, payload FROM signals WHERE target_id = $1 ORDER BY id",
        [target_id]
      )

    Enum.map(rows, fn [id, name, payload] ->
      %{id: id, name: name, payload: decode_json(payload)}
    end)
  end

  # A parent's children (spec §11), for ctx.childs on wake-up.
  def load_childs(repo, parent_id) do
    %{rows: rows} =
      repo.query!(
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
  # row is inserted before the wake-flip so the woken step already sees it (spec §5).
  # Returns `:ok`, or `{:error, :no_target}` when a correlation_key resolves to no
  # occupied instance.
  def deliver_signal(repo, target, name, payload_json, dedup_key) do
    repo.transaction(fn ->
      case resolve_target(repo, target) do
        nil ->
          repo.rollback(:no_target)

        id ->
          repo.query!(
            """
            INSERT INTO signals (target_id, name, payload, dedup_key)
            VALUES ($1, $2, $3::jsonb, $4)
            ON CONFLICT (target_id, dedup_key) DO NOTHING
            """,
            [id, name, payload_json, dedup_key]
          )

          repo.query!(
            """
            UPDATE gen_durable
            SET status = 'runnable', eligible_at = now(), updated_at = now()
            WHERE id = $1 AND status = 'awaiting_signal' AND $2 = ANY(awaits)
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
    case repo.query!("SELECT id FROM gen_durable WHERE correlation_guard = $1", [key]) do
      %{rows: [[id]]} -> id
      %{rows: []} -> nil
    end
  end

  # --- insert / batch insert -------------------------------------------------

  @insert_cols "fsm, fsm_version, step, state, queue, priority, concurrency_key, eligible_at, correlation_key, correlation_scope, rate_limit, weight"
  # 12 = number of columns above (one positional placeholder each).

  # Ensures a token bucket exists (full) for each given rate_limit key, as a leading CTE so
  # the insert stays one statement. `$n` is a text[] of distinct keys; empty ⇒ no-op (spec §12).
  defp ensure_buckets_cte(n) do
    "WITH ensure AS (INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) " <>
      "SELECT k, cfg.burst, clock_timestamp() FROM unnest($#{n}::text[]) k " <>
      "JOIN gen_durable_rate_configs cfg ON cfg.name = split_part(k, ':', 1) " <>
      "ON CONFLICT (key) DO NOTHING) "
  end

  defp bucket_keys(rows),
    do: rows |> Enum.map(& &1.rate_limit) |> Enum.reject(&is_nil/1) |> Enum.uniq()

  # Seed/refresh the rate-limit policy table at engine start (spec §12). `configs` is a list of
  # `%{name, rate, burst}`. Idempotent: re-running with changed numbers updates them.
  def upsert_rate_configs(_repo, []), do: :ok

  def upsert_rate_configs(repo, configs) when is_list(configs) do
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
        row_placeholders(0) <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    case repo.query!(sql, row_args(p) ++ [bucket_keys([p])]) do
      %{rows: [[id]]} -> {:ok, id}
      %{rows: []} -> {:error, :duplicate}
    end
  end

  def insert_all(repo, rows) when is_list(rows) do
    placeholders =
      rows |> Enum.with_index() |> Enum.map(fn {_p, i} -> row_placeholders(i * 12) end)

    sql =
      ensure_buckets_cte(length(rows) * 12 + 1) <>
        "INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        Enum.join(placeholders, ", ") <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    %{rows: out} = repo.query!(sql, Enum.flat_map(rows, &row_args/1) ++ [bucket_keys(rows)])
    List.flatten(out)
  end

  defp row_placeholders(base) do
    "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}::jsonb, $#{base + 5}, " <>
      "$#{base + 6}, $#{base + 7}, COALESCE($#{base + 8}::timestamptz, now()), " <>
      "$#{base + 9}, $#{base + 10}::text[]::durable_status[], $#{base + 11}, $#{base + 12})"
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
    %{rows: [[locked]]} = repo.query!("SELECT pg_try_advisory_lock(hashtext($1))", [key])
    locked
  end

  def advisory_unlock(repo, key) do
    repo.query!("SELECT pg_advisory_unlock(hashtext($1))", [key])
    :ok
  end

  def reset_to_runnable(repo, id) do
    repo.query!(
      """
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [id]
    )

    :ok
  end

  # Release our still-claimed rows back to runnable on graceful shutdown, so the
  # buffered (un-started) work is picked up immediately instead of waiting out the
  # lease. Guarded by `locked_by` so we only ever release our own claims.
  def release(_repo, [], _worker), do: :ok

  def release(repo, ids, worker) when is_list(ids) do
    repo.query!(
      """
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = ANY($1) AND locked_by = $2 AND status = 'executing'
      """,
      [ids, worker]
    )

    :ok
  end

  defp decode_json(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json(value) when is_map(value), do: value
  defp decode_json(nil), do: %{}

  defp decode_json_or_nil(nil), do: nil
  defp decode_json_or_nil(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json_or_nil(value) when is_map(value), do: value
end
