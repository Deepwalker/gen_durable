defmodule GenDurable.PerfTest do
  @moduledoc """
  Round-trip guards for the outcome path. Each `complete_*` must be a SINGLE
  database statement (one round-trip), not a `BEGIN … COMMIT` transaction — the
  signal consume and parent-join are folded in as data-modifying CTEs (F4).

  These tests guard the *round-trip count* only — that the collapse didn't
  silently revert to a multi-statement transaction. They say nothing about
  whether that one statement is *cheap*: a fat CTE could still have a bad plan.
  That is verified separately by EXPLAIN ANALYZE on a realistic table (every
  node a PK index scan, faster than the old 3 statements) — see PERFORMANCE.md §3.

  The `:bench` test (excluded by default; `mix test --only bench`) prints the
  old-vs-new wall-clock so the win is measurable, not asserted-on-flaky-timing.
  """
  use ExUnit.Case, async: false

  alias GenDurable.Queries
  alias GenDurable.Test.Repo

  @query_event [:gen_durable, :test, :repo, :query]

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp params(overrides) do
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
  end

  @doc false
  # Telemetry handler (a module function, to avoid the local-handler warning).
  def __forward_sql__(_event, _measure, meta, pid), do: send(pid, {:sql, meta.query})

  # Count the SQL statements the repo issues while `fun` runs (one per round-trip).
  defp statements(fun) do
    handler = "perf-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler, @query_event, &__MODULE__.__forward_sql__/4, self())

    try do
      fun.()
    after
      :telemetry.detach(handler)
    end

    collect([])
  end

  defp collect(acc) do
    receive do
      {:sql, q} -> collect([q | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp setup_executing(overrides \\ %{}) do
    {:ok, id} = Queries.insert(Repo, params(overrides))
    [_] = Queries.pick(Repo, "default", 1, "w", 60_000)
    id
  end

  test "complete_next is a single statement (one round-trip)" do
    id = setup_executing()
    sql = statements(fn -> Queries.complete_next(Repo, id, "tick", ~s({"n":1}), [], nil, 1) end)

    assert length(sql) == 1
    assert hd(sql) =~ "consumed AS"
  end

  test "complete_retry is a single statement" do
    id = setup_executing()
    assert length(statements(fn -> Queries.complete_retry(Repo, id, ~s({}), 0) end)) == 1
  end

  test "complete_await is a single statement" do
    id = setup_executing()

    assert length(statements(fn -> Queries.complete_await(Repo, id, ~s({}), ["go"], "woke") end)) ==
             1
  end

  test "complete_done is a single statement (consume + done + parent-join folded in)" do
    id = setup_executing()
    sql = statements(fn -> Queries.complete_done(Repo, id, ~s({"ok":true})) end)

    assert length(sql) == 1
    assert hd(sql) =~ "WITH consumed"
    assert hd(sql) =~ "children_pending"
  end

  test "complete_stop is a single statement" do
    id = setup_executing()
    assert length(statements(fn -> Queries.complete_stop(Repo, id, "boom") end)) == 1
  end

  test "complete_schedule_childs is a single statement (consume + insert + park)" do
    id = setup_executing()
    children = [params(%{fsm: "child"}), params(%{fsm: "child"})]

    sql =
      statements(fn ->
        Queries.complete_schedule_childs(Repo, id, "join", ~s({}), children, [])
      end)

    assert length(sql) == 1
    assert hd(sql) =~ "INSERT INTO gen_durable"
  end

  @tag :bench
  test "bench: collapsed outcome vs explicit transaction" do
    id = setup_executing()
    iters = 500

    new = bench(iters, fn -> Queries.complete_next(Repo, id, "tick", ~s({"n":1}), [], nil, 1) end)
    old = bench(iters, fn -> complete_next_tx(id, "tick", ~s({"n":1})) end)

    IO.puts("\n  complete_next over #{iters} iters (median µs/call):")
    IO.puts("    collapsed (1 statement):  #{new} µs")
    IO.puts("    explicit txn (BEGIN+2+COMMIT): #{old} µs")
    IO.puts("    speedup: #{Float.round(old / new, 2)}x")

    assert new < old
  end

  # Median per-call microseconds over `n` runs.
  defp bench(n, fun) do
    times =
      for _ <- 1..n do
        t0 = System.monotonic_time(:microsecond)
        fun.()
        System.monotonic_time(:microsecond) - t0
      end

    Enum.at(Enum.sort(times), div(n, 2))
  end

  # The pre-F4 form: consume + update inside an explicit transaction.
  defp complete_next_tx(id, step, state_json) do
    Repo.transaction(fn ->
      Repo.query!(
        "DELETE FROM signals s USING gen_durable g WHERE s.target_id=$1 AND g.id=$1 AND s.name=ANY(g.awaits)",
        [id]
      )

      Repo.query!(
        "UPDATE gen_durable SET step=$2, state=$3::jsonb, status='runnable', eligible_at=now(), attempt=0, awaits=null, locked_by=null, lease_expires_at=null, updated_at=now() WHERE id=$1",
        [id, step, state_json]
      )
    end)

    :ok
  end
end
