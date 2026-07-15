defmodule GenDurable.InlineSlotCreditTest do
  use ExUnit.Case, async: false

  alias GenDurable.Test.Repo

  # Inline FSM that is inserted WITH a configured concurrency_key and then keeps it
  # across an inline chain (plain {:next, ...} => concurrency_key :keep). Each step
  # holds the slot for a while, tracking the max number of instances running at once.
  defmodule HoldFSM do
    use GenDurable.FSM,
      name: "repro_hold",
      version: 1,
      initial: "a",
      inline_execution: true

    defp enter do
      Agent.get_and_update(GenDurable.Repro.Probe, fn %{active: a, max: m} ->
        na = a + 1
        {na, %{active: na, max: max(m, na)}}
      end)
    end

    defp leave do
      Agent.update(GenDurable.Repro.Probe, fn %{active: a} = s -> %{s | active: a - 1} end)
    end

    @impl true
    def step("a", %{state: s}) do
      enter()
      Process.sleep(150)
      leave()
      {:next, "b", s}
    end

    def step("b", %{state: s}) do
      enter()
      Process.sleep(150)
      leave()
      {:next, "c", s}
    end

    def step("c", _ctx) do
      enter()
      Process.sleep(150)
      leave()
      {:done, %{"ok" => true}}
    end
  end

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    {:ok, _} = Agent.start_link(fn -> %{active: 0, max: 0} end, name: GenDurable.Repro.Probe)
    :ok
  end

  test "inline keep-chain does not over-credit its concurrency slot (limit 1 must serialize)" do
    start_supervised!(
      {GenDurable,
       repo: Repo,
       fsms: [HoldFSM],
       queues: [default: 5],
       poll_interval: 25,
       lease_ttl: 5_000,
       heartbeat_interval: 1_000,
       reaper: [interval: 150],
       gc: false,
       concurrency_limits: [pool: [limit: 1]],
       flushers: [%{queues: :all, max_delay_ms: 5}]}
    )

    {:ok, id1} = GenDurable.insert(HoldFSM, concurrency_key: "pool:x")
    {:ok, id2} = GenDurable.insert(HoldFSM, concurrency_key: "pool:x")

    # Wait for both to finish.
    for id <- [id1, id2] do
      deadline = System.monotonic_time(:millisecond) + 15_000

      wait = fn wait ->
        %{rows: [[status]]} =
          Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

        cond do
          status == "done" -> :ok
          System.monotonic_time(:millisecond) > deadline -> flunk("id #{id} stuck at #{status}")
          true -> Process.sleep(25) && wait.(wait)
        end
      end

      wait.(wait)
    end

    %{max: max_concurrent} = Agent.get(GenDurable.Repro.Probe, & &1)

    assert max_concurrent == 1,
           "concurrency_key pool:x has limit 1, but #{max_concurrent} instances ran a step " <>
             "at the same time — the inline keep-chain credited a still-held slot"
  end
end
