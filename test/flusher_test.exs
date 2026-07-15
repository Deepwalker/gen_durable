defmodule GenDurable.FlusherTest do
  @moduledoc """
  The group-commit coordinator: concurrent Tasks submit outcomes and block until
  a single batched flush commits them. Covers both triggers (count, deadline) and
  the stale (ownership-guard-failed) reply.
  """
  use ExUnit.Case, async: false

  alias GenDurable.{Flusher, Queries}
  alias GenDurable.Test.Repo

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp config,
    do: %{repo: Repo, limiter: {GenDurable.Limiter.Postgres, %{repo: Repo}}, name: __MODULE__.Engine}

  defp start_flusher(opts) do
    name = :"flusher_#{System.unique_integer([:positive])}"
    {:ok, pid} = Flusher.start_link(Keyword.merge([name: name, config: config()], opts))
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp executing do
    {:ok, id} =
      Queries.insert(Repo, %{
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
      })

    [_] = Queries.pick(Repo, "default", 10, "w", 60_000)
    id
  end

  defp next_entry(id) do
    %{
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
      step: "second",
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
      consumed_ids: []
    }
  end

  defp status(id) do
    %{rows: [[s]]} = Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])
    s
  end

  test "count trigger: N concurrent commits coalesce into one flush" do
    f = start_flusher(max_batch: 3, max_delay_ms: 60_000)
    ids = for _ <- 1..3, do: executing()

    results =
      ids
      |> Enum.map(fn id -> Task.async(fn -> Flusher.commit(f, next_entry(id)) end) end)
      |> Task.await_many(5_000)

    assert results == [:committed, :committed, :committed]
    for id <- ids, do: assert(status(id) == "runnable")
  end

  test "deadline trigger: a sub-batch commits after max_delay_ms" do
    f = start_flusher(max_batch: 100, max_delay_ms: 120)
    ids = for _ <- 1..2, do: executing()

    results =
      ids
      |> Enum.map(fn id -> Task.async(fn -> Flusher.commit(f, next_entry(id)) end) end)
      |> Task.await_many(5_000)

    assert results == [:committed, :committed]
    for id <- ids, do: assert(status(id) == "runnable")
  end

  test "a row reclaimed by another worker comes back :stale" do
    f = start_flusher(max_batch: 1, max_delay_ms: 60_000)
    id = executing()

    assert Flusher.commit(f, %{next_entry(id) | worker: "someone-else"}) == :stale
    assert status(id) == "executing"
  end
end
