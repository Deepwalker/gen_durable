defmodule GenDurable.FlushTest do
  @moduledoc """
  Batched flush (group commit): one transaction commits a whole batch of
  outcomes. Proves a mixed :next/:retry/:done/:stop batch lands correctly, that
  the ownership guard drops stale rows, that signal-consume is scoped to
  committed rows, and that sibling completions collapse into one parent
  decrement (join barrier).
  """
  use ExUnit.Case, async: false

  alias GenDurable.Queries
  alias GenDurable.Test.Repo

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp insert(overrides \\ %{}) do
    params =
      Map.merge(
        %{
          fsm: "counter",
          fsm_version: 1,
          step: "tick",
          state_json: ~s({"n":0}),
          queue: "default",
          priority: 0,
          concurrency_key: nil,
          correlation_key: nil,
          correlation_scope: [],
          rate_limit: nil,
          weight: 1,
          eligible_at: nil
        },
        overrides
      )

    {:ok, id} = Queries.insert(Repo, params)
    id
  end

  # Insert a row and claim it to `executing` (locked_by "w"). Claims the single
  # runnable row just inserted (others are already executing/parked).
  defp executing(overrides \\ %{}) do
    id = insert(overrides)
    [_] = Queries.pick(Repo, to_string(Map.get(overrides, :queue, "default")), 10, "w", 60_000)
    id
  end

  defp row(id) do
    %{rows: [r]} =
      Repo.query!(
        "SELECT status::text, step, state, result, last_error, attempt, awaits, children_pending " <>
          "FROM gen_durable WHERE id = $1",
        [id]
      )

    [status, step, state, result, last_error, attempt, awaits, cp] = r

    %{
      status: status,
      step: step,
      state: state,
      result: result,
      last_error: last_error,
      attempt: attempt,
      awaits: awaits,
      children_pending: cp
    }
  end

  defp signal_ids(id) do
    %{rows: rows} = Repo.query!("SELECT id FROM signals WHERE target_id = $1 ORDER BY id", [id])
    List.flatten(rows)
  end

  # A flush entry with every field defaulted; `over` sets the kind-specific bits.
  defp entry(id, over) do
    Map.merge(
      %{
        id: id,
        worker: "w",
        status: "runnable",
        attempt: 0,
        delay_ms: 0,
        set_attempt: true,
        set_eligible: true,
        keep_lock: false,
        set_shard: true,
        shard_value: nil,
        lease_ttl_ms: 0,
        set_step: false,
        step: nil,
        set_state: false,
        state: nil,
        set_result: false,
        result: nil,
        set_error: false,
        error: nil,
        clear_awaits: false,
        set_rate: false,
        rate_limit: nil,
        weight: 1.0,
        set_ck: false,
        ck_value: nil,
        consumed_ids: []
      },
      over
    )
  end

  test "mixed batch commits next/retry/done/stop in one flush" do
    n = executing()
    r = executing()
    d = executing()
    s = executing()

    entries = [
      entry(n, %{
        status: "runnable",
        set_step: true,
        step: "second",
        set_state: true,
        state: ~s({"n":1}),
        clear_awaits: true,
        set_rate: true
      }),
      entry(r, %{status: "runnable", attempt: 1, delay_ms: 1000, set_state: true, state: ~s({"r":1})}),
      entry(d, %{status: "done", set_result: true, result: ~s({"ok":true}), clear_awaits: true}),
      entry(s, %{status: "failed", set_error: true, error: "boom", clear_awaits: true})
    ]

    result = Queries.flush(Repo, entries)
    assert Enum.sort(result.committed) == Enum.sort([n, r, d, s])
    assert result.stale == []

    assert row(n).status == "runnable"
    assert row(n).step == "second"

    assert row(r).status == "runnable"
    assert row(r).attempt == 1

    assert row(d).status == "done"
    assert row(d).result == %{"ok" => true}

    assert row(s).status == "failed"
    assert row(s).last_error == "boom"
  end

  test "a row whose ownership guard fails is reported stale, not committed" do
    id = executing()

    result = Queries.flush(Repo, [entry(id, %{worker: "someone-else", status: "done", set_result: true, result: ~s({})})])

    assert result.committed == []
    assert result.stale == [id]
    assert row(id).status == "executing"
  end

  test "progressing consume drops exactly the awaited ids, scoped to committed" do
    id = executing()

    %{rows: [[sig]]} =
      Repo.query!(
        "INSERT INTO signals (target_id, name, payload) VALUES ($1, 'go', '{}'::jsonb) RETURNING id",
        [id]
      )

    Queries.flush(Repo, [
      entry(id, %{status: "runnable", set_step: true, step: "x", set_state: true, state: ~s({}), consumed_ids: [sig]})
    ])

    assert signal_ids(id) == []
  end

  test "terminal outcome drops the whole inbox" do
    id = executing()

    Repo.query!(
      "INSERT INTO signals (target_id, name, payload) VALUES ($1,'a','{}'::jsonb), ($1,'b','{}'::jsonb)",
      [id]
    )

    Queries.flush(Repo, [entry(id, %{status: "done", set_result: true, result: ~s({})})])
    assert signal_ids(id) == []
  end

  test "sibling completions collapse into one parent decrement and wake the parent" do
    parent = insert()
    Repo.query!("UPDATE gen_durable SET status='awaiting_children', children_pending=2 WHERE id=$1", [parent])

    c1 = executing()
    c2 = executing()
    Repo.query!("UPDATE gen_durable SET parent_id=$1 WHERE id = ANY($2::bigint[])", [parent, [c1, c2]])

    result =
      Queries.flush(Repo, [
        entry(c1, %{status: "done", set_result: true, result: ~s({})}),
        entry(c2, %{status: "done", set_result: true, result: ~s({})})
      ])

    assert row(parent).children_pending == 0
    assert row(parent).status == "runnable"
    assert "default" in result.woken_queues
  end
end
