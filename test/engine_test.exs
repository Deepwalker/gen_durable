defmodule GenDurable.EngineTest do
  use ExUnit.Case, async: false

  alias GenDurable.Test.Repo

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp start_engine(opts \\ []) do
    defaults = [
      repo: Repo,
      fsms: GenDurable.Test.FSMs.all(),
      queues: [default: 5],
      poll_interval: 25,
      lease_ttl: 1_000,
      heartbeat_interval: 300,
      reap_interval: 150
    ]

    start_supervised!({GenDurable, Keyword.merge(defaults, opts)})
  end

  defp status(id) do
    %{rows: [[s, result, err, attempt]]} =
      Repo.query!(
        "SELECT status::text, result, last_error, attempt FROM gen_durable WHERE id = $1",
        [id]
      )

    %{status: s, result: result, last_error: err, attempt: attempt}
  end

  defp eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    run = fn run ->
      case fun.() do
        {:ok, v} ->
          v

        :retry ->
          if System.monotonic_time(:millisecond) > deadline do
            flunk("eventually/2 timed out")
          else
            Process.sleep(20)
            run.(run)
          end
      end
    end

    run.(run)
  end

  defp wait_status(id, target) do
    eventually(fn ->
      row = status(id)
      if row.status == target, do: {:ok, row}, else: :retry
    end)
  end

  test "typed-state FSM drives :next loop to :done, committing each step" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Counter, state: %{target: 3})

    row = wait_status(id, "done")
    assert Jason.decode!(row.result) == %{"count" => 3}
  end

  test "plain-map FSM also reaches :done" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.MapCounter, state: %{"target" => 2})

    row = wait_status(id, "done")
    assert Jason.decode!(row.result) == %{"count" => 2}
  end

  test "await parks, signal wakes, step consumes and deletes the signal" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter)

    wait_status(id, "awaiting_signal")
    :ok = GenDurable.signal(id, "go", %{v: 7})

    row = wait_status(id, "done")
    assert Jason.decode!(row.result) == %{"got" => %{"v" => 7}}

    # Consumed signal was removed in the outcome transaction.
    assert GenDurable.Queries.load_signals(Repo, id) == []
  end

  test "caught exception routes to handle/2, which replays then stops" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Crasher)

    row = wait_status(id, "failed")
    assert row.last_error == "gave up"
    assert row.attempt == 2
  end

  test "worker crash with no outcome is recovered by the reaper (attempt + 1)" do
    start_engine(lease_ttl: 500, heartbeat_interval: 250, reap_interval: 100)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Reborn)

    row = wait_status(id, "done")
    assert Jason.decode!(row.result) == %{"recovered" => true}
    assert row.attempt >= 1
  end

  test "partition_key serializes steps sharing a key (no lost updates)" do
    start_supervised!(%{
      id: :partition_agent,
      start: {Agent, :start_link, [fn -> 0 end, [name: GenDurable.Test.PartitionAgent]]}
    })

    start_engine()

    ids =
      for _ <- 1..4 do
        {:ok, id} = GenDurable.insert(GenDurable.Test.PartitionInc, partition_key: "k")
        id
      end

    for id <- ids, do: wait_status(id, "done")

    assert Agent.get(GenDurable.Test.PartitionAgent, & &1) == 4
  end
end
