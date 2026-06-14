defmodule GenDurable.Test.Counter.State do
  use GenDurable.State

  embedded_schema do
    field(:target, :integer, default: 0)
    field(:n, :integer, default: 0)
  end
end

defmodule GenDurable.Test.Counter do
  @moduledoc "Typed-state FSM: loops on :next until n reaches target, then :done."
  use GenDurable.FSM,
    name: "counter",
    version: 1,
    state: GenDurable.Test.Counter.State,
    initial: "tick"

  @impl true
  def step("tick", %{state: s}) do
    if s.n >= s.target do
      {:done, %{"count" => s.n}}
    else
      {:next, "tick", %{s | n: s.n + 1}}
    end
  end
end

defmodule GenDurable.Test.MapCounter do
  @moduledoc "Plain-map state (no state module)."
  use GenDurable.FSM, name: "map_counter", version: 1, initial: "tick"

  @impl true
  def step("tick", %{state: s}) do
    n = Map.get(s, "n", 0)
    target = Map.get(s, "target", 0)

    if n >= target,
      do: {:done, %{"count" => n}},
      else: {:next, "tick", Map.put(s, "n", n + 1)}
  end
end

defmodule GenDurable.Test.Awaiter do
  @moduledoc "Parks on a signal, completes with its payload once delivered."
  use GenDurable.FSM, name: "awaiter", version: 1, initial: "wait"

  @impl true
  def step("wait", ctx) do
    case Enum.find(ctx.signals, &(&1.name == "go")) do
      nil -> {:await, "go", ctx.state}
      sig -> {:done, %{"got" => sig.payload}}
    end
  end
end

defmodule GenDurable.Test.Crasher do
  @moduledoc "Raises; handle/2 replays a few times then stops."
  use GenDurable.FSM, name: "crasher", version: 1, initial: "boom"

  @impl true
  def step("boom", _ctx), do: raise("kaboom")

  @impl true
  def handle(_reason, ctx) do
    if ctx.attempt < 2, do: {:replay, ctx.state, 0}, else: {:stop, "gave up"}
  end
end

defmodule GenDurable.Test.Reborn do
  @moduledoc "Crashes the worker (no outcome) on the first attempt, then succeeds."
  use GenDurable.FSM, name: "reborn", version: 1, initial: "go"

  @impl true
  def step("go", %{attempt: 0}), do: Process.exit(self(), :kill)
  def step("go", _ctx), do: {:done, %{"recovered" => true}}
end

defmodule GenDurable.Test.PartitionInc do
  @moduledoc "Read-sleep-write against a shared Agent to expose lost updates."
  use GenDurable.FSM, name: "partition_inc", version: 1, initial: "inc"

  @impl true
  def step("inc", _ctx) do
    agent = GenDurable.Test.PartitionAgent
    v = Agent.get(agent, & &1)
    Process.sleep(25)
    Agent.update(agent, fn _ -> v + 1 end)
    {:done, %{"v" => v + 1}}
  end
end

defmodule GenDurable.Test.Plain do
  @moduledoc "Trivial terminal FSM, used for uniqueness tests."
  use GenDurable.FSM, name: "plain", version: 1, initial: "done"

  @impl true
  def step("done", _ctx), do: {:done, %{}}
end

defmodule GenDurable.Test.FSMs do
  def all do
    [
      GenDurable.Test.Counter,
      GenDurable.Test.MapCounter,
      GenDurable.Test.Awaiter,
      GenDurable.Test.Crasher,
      GenDurable.Test.Reborn,
      GenDurable.Test.PartitionInc,
      GenDurable.Test.Plain
    ]
  end
end
