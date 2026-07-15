defmodule GenDurable.PerfTest do
  @moduledoc """
  Round-trip guards for the hot path — the pick, `deliver_signal`, and the
  batched outcome flush (`GenDurable.Queries.flush/1`, the group-commit path all
  outcomes take; see `GenDurable.Flusher`).

  The key contract these guard is that a flush of N outcomes is a **bounded**
  number of statements — not one per outcome. That is what makes the group commit
  a win over the old per-instance `complete_*` round-trips (one statement per
  instance per step). They guard the *statement count* only, not that each is
  *cheap* — that is verified by EXPLAIN ANALYZE on a realistic table (see
  PERFORMANCE.md).

  The `:bench` test (excluded by default; `mix test --only bench`) prints one
  batched flush vs N per-outcome flushes so the win is measurable, not
  asserted-on-flaky-timing.
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

  test "a pick with claims is exactly three statements (pick + batched signal/children loads)" do
    for _ <- 1..5, do: {:ok, _} = Queries.insert(Repo, params(%{}))

    sql = statements(fn -> Queries.pick(Repo, "default", 10, "w", 60_000) end)

    # enrichment is per BATCH, not per job — 3 statements for 5 jobs, not 1 + 2×5
    assert length(sql) == 3
  end

  test "an empty pick is a single statement (no enrichment queries)" do
    assert length(statements(fn -> Queries.pick(Repo, "default", 10, "w", 60_000) end)) == 1
  end

  test "deliver_signal is a single statement (resolve + insert + wake folded in)" do
    id = setup_executing()

    sql = statements(fn -> Queries.deliver_signal(Repo, id, "go", ~s({}), nil) end)

    assert length(sql) == 1
    assert hd(sql) =~ "WITH target"
  end

  test "a batched flush commits N outcomes in a bounded number of statements (not N)" do
    small = executing_ids(3)
    big = executing_ids(30)

    a = statements(fn -> Queries.flush(Repo, Enum.map(small, &next_entry/1)) end)
    b = statements(fn -> Queries.flush(Repo, Enum.map(big, &next_entry/1)) end)

    # One BEGIN + one UPDATE + one COMMIT — independent of batch size (these :next
    # entries consume nothing and have no parent join). The per-instance commit
    # storm the flush replaces would be one statement PER row instead.
    assert length(a) == 3
    assert length(b) == length(a)
  end

  @tag :bench
  test "bench: one batched flush vs N per-outcome flushes" do
    iters = 50
    n = 50

    batched =
      bench(iters, fn ->
        ids = executing_ids(n)
        Queries.flush(Repo, Enum.map(ids, &next_entry/1))
      end)

    per =
      bench(iters, fn ->
        ids = executing_ids(n)
        for id <- ids, do: Queries.flush(Repo, [next_entry(id)])
      end)

    IO.puts("\n  #{n} outcomes over #{iters} iters (median us):")
    IO.puts("    one batched flush:        #{batched} us")
    IO.puts("    #{n} per-outcome flushes:  #{per} us")
    IO.puts("    speedup: #{Float.round(per / batched, 2)}x")

    assert batched < per
  end

  defp executing_ids(n) do
    for _ <- 1..n, do: {:ok, _} = Queries.insert(Repo, params(%{}))
    Queries.pick(Repo, "default", n, "w", 60_000) |> Enum.map(& &1.id)
  end

  defp next_entry(id) do
    %{
      kind: :state,
      id: id,
      worker: "w",
      slot: nil,
      notify: false,
      status: "runnable",
      attempt: 0,
      delay_ms: 0,
      set_attempt: true,
      set_eligible: true,
      keep_lock: false,
      set_shard: true,
      shard_value: nil,
      lease_ttl_ms: 0,
      set_step: true,
      step: "next",
      set_state: true,
      state: ~s({"n":1}),
      set_result: false,
      result: nil,
      set_error: false,
      error: nil,
      clear_awaits: true,
      set_rate: true,
      rate_limit: nil,
      weight: 1.0,
      set_ck: false,
      ck_value: nil,
      consumed_ids: [],
      awaits_json: nil,
      timeout_ms: nil,
      presented_ids: [],
      children: []
    }
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
end
