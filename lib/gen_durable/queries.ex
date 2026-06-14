defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement from spec §10, one function each, as raw SQL.

  All functions take the `repo` explicitly. The `complete_*` functions run the
  outcome `UPDATE` and the consumed-signal `DELETE` in one transaction.
  `partition_key` serialization (advisory lock) helpers live here too; the
  caller is responsible for holding a single connection (`Repo.checkout/1`) for
  the lock's lifetime.
  """

  # --- picker ----------------------------------------------------------------

  # partition_key dedup (spec §6): never claim more than one row per partition_key
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
  #            serialize, so they short-circuit the guard and a non-partitioned
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
  @pick_sql """
  WITH locked AS (
    SELECT id,
           row_number() OVER (PARTITION BY coalesce(partition_key, id::text)
                              ORDER BY priority, eligible_at) AS rn
    FROM (
      SELECT id, partition_key, priority, eligible_at
      FROM gen_durable g
      WHERE g.status = 'runnable' AND g.eligible_at <= now() AND g.queue = $1
        AND (g.partition_key IS NULL OR NOT EXISTS (
          SELECT 1 FROM gen_durable e
          WHERE e.partition_key = g.partition_key AND e.status = 'executing'
        ))
      ORDER BY g.priority, g.eligible_at
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    ) s
  )
  UPDATE gen_durable g
  SET status = 'executing', locked_by = $3,
      lease_expires_at = now() + $4::int * interval '1 millisecond', updated_at = now()
  FROM locked l
  WHERE g.id = l.id AND l.rn = 1
  RETURNING g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.partition_key
  """

  def pick(repo, queue, batch, worker, lease_ttl_ms) do
    %{rows: rows} = repo.query!(@pick_sql, [queue, batch, worker, lease_ttl_ms])
    Enum.map(rows, &to_job/1)
  end

  defp to_job([id, fsm, fsm_version, step, state, attempt, partition_key]) do
    %{
      id: id,
      fsm: fsm,
      fsm_version: fsm_version,
      step: step,
      state: state,
      attempt: attempt,
      partition_key: partition_key
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

  # --- step outcomes (spec §3 / §10) -----------------------------------------
  #
  # Each outcome is a SINGLE statement, not a transaction (one round-trip instead
  # of BEGIN + … + COMMIT). The signal consume (spec §5) rides along as a leading
  # data-modifying CTE, `consumed`, atomic with the outcome UPDATE because one
  # statement is its own implicit transaction. `consumed` deletes exactly the
  # inbox signals whose name matches the row's current `awaits` (a no-op when
  # `awaits` is null) and runs to completion even though the main query never
  # reads it — Postgres guarantees data-modifying CTEs always execute fully.

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

  def complete_next(repo, id, step, state_json) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals s USING gen_durable g
        WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits
      )
      UPDATE gen_durable
      SET step = $2, state = $3::jsonb, status = 'runnable', eligible_at = now(),
          attempt = 0, awaits = null, locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [id, step, state_json]
    )

    :ok
  end

  def complete_replay(repo, id, state_json, delay_ms) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals s USING gen_durable g
        WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits
      )
      UPDATE gen_durable
      SET state = $2::jsonb, status = 'runnable',
          eligible_at = now() + $3::int * interval '1 millisecond',
          attempt = attempt + 1, awaits = null, locked_by = null, lease_expires_at = null,
          updated_at = now()
      WHERE id = $1
      """,
      [id, state_json, delay_ms]
    )

    :ok
  end

  # Park on a signal. If a matching signal is already in the inbox (it arrived
  # before this park committed), go straight back to runnable so the step re-runs
  # and consumes it — closing the lost-wakeup race. `awaits` is set either way, so
  # the eventual progressing outcome consumes by name (spec §5).
  def complete_await(repo, id, state_json, signal_name) do
    repo.query!(
      """
      UPDATE gen_durable
      SET state = $2::jsonb, awaits = $3, eligible_at = now(),
          status = (CASE WHEN EXISTS (SELECT 1 FROM signals WHERE target_id = $1 AND name = $3)
                         THEN 'runnable' ELSE 'awaiting_signal' END)::durable_status,
          locked_by = null, lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [id, state_json, signal_name]
    )

    :ok
  end

  def complete_done(repo, id, result_json) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals s USING gen_durable g
        WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits
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
        DELETE FROM signals s USING gen_durable g
        WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits
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
  def complete_schedule_childs(repo, parent_id, next_step, state_json, []) do
    repo.query!(
      """
      WITH consumed AS (
        DELETE FROM signals s USING gen_durable g
        WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits
      )
      UPDATE gen_durable
      SET step = $2, state = $3::jsonb, children_pending = 0, status = 'runnable',
          eligible_at = now(), attempt = 0, awaits = null, locked_by = null,
          lease_expires_at = null, updated_at = now()
      WHERE id = $1
      """,
      [parent_id, next_step, state_json]
    )

    :ok
  end

  def complete_schedule_childs(repo, parent_id, next_step, state_json, children) do
    placeholders =
      children
      |> Enum.with_index()
      |> Enum.map(fn {_p, i} -> child_row_placeholders(3 + i * 10) end)

    sql =
      "WITH consumed AS (DELETE FROM signals s USING gen_durable g " <>
        "WHERE s.target_id = $1 AND g.id = $1 AND s.name = g.awaits), " <>
        "ins AS (INSERT INTO gen_durable " <>
        "(fsm, fsm_version, step, state, queue, priority, partition_key, " <>
        "unique_key, unique_scope, eligible_at, parent_id) VALUES " <>
        Enum.join(placeholders, ", ") <>
        " ON CONFLICT (unique_guard) WHERE unique_guard IS NOT NULL DO NOTHING RETURNING 1), " <>
        "cnt AS (SELECT count(*) AS n FROM ins) " <>
        "UPDATE gen_durable SET step = $2, state = $3::jsonb, " <>
        "children_pending = (SELECT n FROM cnt), " <>
        "status = (CASE WHEN (SELECT n FROM cnt) = 0 THEN 'runnable' " <>
        "ELSE 'awaiting_children' END)::durable_status, " <>
        "eligible_at = now(), attempt = 0, awaits = null, locked_by = null, " <>
        "lease_expires_at = null, updated_at = now() WHERE id = $1"

    args = [parent_id, next_step, state_json] ++ Enum.flat_map(children, &row_args/1)

    repo.query!(sql, args)
    :ok
  end

  # Child placeholders share $1 (the parent id) as the parent_id column.
  defp child_row_placeholders(base) do
    "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}::jsonb, $#{base + 5}, " <>
      "$#{base + 6}, $#{base + 7}, $#{base + 8}, $#{base + 9}::text[]::durable_status[], " <>
      "COALESCE($#{base + 10}::timestamptz, now()), $1)"
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

  def deliver_signal(repo, target_id, name, payload_json, dedup_key) do
    repo.transaction(fn ->
      repo.query!(
        """
        INSERT INTO signals (target_id, name, payload, dedup_key)
        VALUES ($1, $2, $3::jsonb, $4)
        ON CONFLICT (target_id, dedup_key) DO NOTHING
        """,
        [target_id, name, payload_json, dedup_key]
      )

      repo.query!(
        """
        UPDATE gen_durable
        SET status = 'runnable', eligible_at = now(), updated_at = now()
        WHERE id = $1 AND status = 'awaiting_signal' AND awaits = $2
        """,
        [target_id, name]
      )
    end)

    :ok
  end

  # --- insert / batch insert -------------------------------------------------

  @insert_cols "fsm, fsm_version, step, state, queue, priority, partition_key, unique_key, unique_scope, eligible_at"

  def insert(repo, p) do
    sql =
      "INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        row_placeholders(0) <>
        " ON CONFLICT (unique_guard) WHERE unique_guard IS NOT NULL DO NOTHING RETURNING id"

    case repo.query!(sql, row_args(p)) do
      %{rows: [[id]]} -> {:ok, id}
      %{rows: []} -> {:error, :duplicate}
    end
  end

  def insert_all(repo, rows) when is_list(rows) do
    placeholders =
      rows |> Enum.with_index() |> Enum.map(fn {_p, i} -> row_placeholders(i * 10) end)

    sql =
      "INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        Enum.join(placeholders, ", ") <>
        " ON CONFLICT (unique_guard) WHERE unique_guard IS NOT NULL DO NOTHING RETURNING id"

    %{rows: out} = repo.query!(sql, Enum.flat_map(rows, &row_args/1))
    List.flatten(out)
  end

  defp row_placeholders(base) do
    "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}::jsonb, $#{base + 5}, " <>
      "$#{base + 6}, $#{base + 7}, $#{base + 8}, $#{base + 9}::text[]::durable_status[], " <>
      "COALESCE($#{base + 10}::timestamptz, now()))"
  end

  defp row_args(p) do
    [
      p.fsm,
      p.fsm_version,
      p.step,
      p.state_json,
      p.queue,
      p.priority,
      p.partition_key,
      p.unique_key,
      p.unique_scope,
      p.eligible_at
    ]
  end

  # --- partition_key serialization (advisory lock) ---------------------------

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

  defp decode_json(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json(value) when is_map(value), do: value
  defp decode_json(nil), do: %{}

  defp decode_json_or_nil(nil), do: nil
  defp decode_json_or_nil(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json_or_nil(value) when is_map(value), do: value
end
