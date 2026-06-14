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

  @pick_sql """
  WITH picked AS (
    SELECT id FROM gen_durable
    WHERE status = 'runnable' AND eligible_at <= now()
      AND queue = ANY($1)
    ORDER BY priority, eligible_at
    FOR UPDATE SKIP LOCKED
    LIMIT $2
  )
  UPDATE gen_durable g
  SET status = 'executing', locked_by = $3,
      lease_expires_at = now() + $4::int * interval '1 millisecond', updated_at = now()
  FROM picked WHERE g.id = picked.id
  RETURNING g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.partition_key
  """

  def pick(repo, queues, batch, worker, lease_ttl_ms) do
    %{rows: rows} = repo.query!(@pick_sql, [queues, batch, worker, lease_ttl_ms])
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

  def complete_next(repo, id, step, state_json, consumed) do
    tx(repo, consumed, fn ->
      repo.query!(
        """
        UPDATE gen_durable
        SET step = $2, state = $3::jsonb, status = 'runnable', eligible_at = now(),
            attempt = 0, locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
        """,
        [id, step, state_json]
      )
    end)
  end

  def complete_replay(repo, id, state_json, delay_ms, consumed) do
    tx(repo, consumed, fn ->
      repo.query!(
        """
        UPDATE gen_durable
        SET state = $2::jsonb, status = 'runnable',
            eligible_at = now() + $3::int * interval '1 millisecond',
            attempt = attempt + 1, locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
        """,
        [id, state_json, delay_ms]
      )
    end)
  end

  def complete_await(repo, id, state_json, signal_name, consumed) do
    tx(repo, consumed, fn ->
      repo.query!(
        """
        UPDATE gen_durable
        SET state = $2::jsonb, status = 'awaiting_signal', awaits = $3,
            locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
        """,
        [id, state_json, signal_name]
      )
    end)
  end

  def complete_done(repo, id, result_json, consumed) do
    tx(repo, consumed, fn ->
      repo.query!(
        """
        UPDATE gen_durable
        SET result = $2::jsonb, status = 'done',
            locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
        """,
        [id, result_json]
      )
    end)
  end

  def complete_stop(repo, id, reason_text, consumed) do
    tx(repo, consumed, fn ->
      repo.query!(
        """
        UPDATE gen_durable
        SET status = 'failed', last_error = $2,
            locked_by = null, lease_expires_at = null, updated_at = now()
        WHERE id = $1
        """,
        [id, reason_text]
      )
    end)
  end

  defp tx(repo, consumed, update_fun) do
    repo.transaction(fn ->
      update_fun.()
      delete_signals(repo, consumed)
    end)

    :ok
  end

  defp delete_signals(_repo, []), do: :ok

  defp delete_signals(repo, ids) do
    repo.query!("DELETE FROM signals WHERE id = ANY($1)", [ids])
    :ok
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
        SET status = 'runnable', eligible_at = now(), awaits = null, updated_at = now()
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
end
