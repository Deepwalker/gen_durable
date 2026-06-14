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

defmodule GenDurable.Test.Child do
  @moduledoc "A child instance: succeeds, or fails when its state says so."
  use GenDurable.FSM, name: "child", version: 1, initial: "go"

  @impl true
  def step("go", %{state: s}) do
    if Map.get(s, "fail"),
      do: {:stop, "child boom"},
      else: {:done, %{"x" => Map.get(s, "x")}}
  end
end

defmodule GenDurable.Test.Parent do
  @moduledoc "Fans out children from `state[\"kids\"]`, joins on all of them (spec §11)."
  use GenDurable.FSM, name: "parent", version: 1, initial: "fan"

  @impl true
  def step("fan", %{state: s}) do
    specs = for kid <- Map.get(s, "kids", []), do: {GenDurable.Test.Child, state: kid}
    {:schedule_childs, "join", specs, s}
  end

  def step("join", %{childs: childs}) do
    {:done,
     %{
       "children" => length(childs),
       "done" => Enum.count(childs, &(&1.status == "done")),
       "failed" => Enum.count(childs, &(&1.status == "failed"))
     }}
  end
end

defmodule GenDurable.Test.Sleeper do
  @moduledoc "Sleeps `state[\"ms\"]` then completes — used to test heartbeat & parallelism."
  use GenDurable.FSM, name: "sleeper", version: 1, initial: "sleep"

  @impl true
  def step("sleep", %{state: s}) do
    Process.sleep(Map.get(s, "ms", 0))
    {:done, %{"slept" => Map.get(s, "ms", 0)}}
  end
end

defmodule GenDurable.Test.Recorder do
  @moduledoc "Appends `state[\"tag\"]` to a shared Agent — used to test pick ordering."
  use GenDurable.FSM, name: "recorder", version: 1, initial: "rec"

  @impl true
  def step("rec", %{state: s}) do
    Agent.update(GenDurable.Test.RecorderAgent, &(&1 ++ [Map.get(s, "tag")]))
    {:done, %{}}
  end
end

defmodule GenDurable.Test.Overlap do
  @moduledoc "Records start/stop instants around a sleep — used to prove cross-key parallelism."
  use GenDurable.FSM, name: "overlap", version: 1, initial: "go"

  @impl true
  def step("go", %{id: id}) do
    agent = GenDurable.Test.OverlapAgent
    Agent.update(agent, &Map.put(&1, {id, :start}, System.monotonic_time(:millisecond)))
    Process.sleep(120)
    Agent.update(agent, &Map.put(&1, {id, :stop}, System.monotonic_time(:millisecond)))
    {:done, %{}}
  end
end

defmodule GenDurable.Test.Auto do
  @moduledoc "No custom `name:` — resolves dynamically from its module name, never registered."
  use GenDurable.FSM, initial: "go"

  @impl true
  def step("go", _ctx), do: {:done, %{"auto" => true}}
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
      GenDurable.Test.Plain,
      GenDurable.Test.Child,
      GenDurable.Test.Parent,
      GenDurable.Test.Sleeper,
      GenDurable.Test.Recorder,
      GenDurable.Test.Overlap
    ]
  end
end
