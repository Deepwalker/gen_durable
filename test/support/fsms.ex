defmodule GenDurable.Test.Counter do
  @moduledoc "Typed-state FSM: loops on :next until n reaches target, then :done."
  use GenDurable.FSM, name: "counter", version: 1, initial: "tick"

  # Nested state schema, adopted by convention — no `state:` opt needed.
  defmodule State do
    use GenDurable.State

    embedded_schema do
      field(:target, :integer, default: 0)
      field(:n, :integer, default: 0)
    end
  end

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
  @moduledoc "Parks on one signal, then runs the next step with ctx.awaited once delivered."
  use GenDurable.FSM, name: "awaiter", version: 1, initial: "wait"

  @impl true
  def step("wait", ctx), do: {:await, "go", "woke", ctx.state}
  def step("woke", ctx), do: {:done, %{"got" => hd(ctx.awaited).payload}}
end

defmodule GenDurable.Test.Selector do
  @moduledoc "Awaits any of a set, branches on which name arrived."
  use GenDurable.FSM, name: "selector", version: 1, initial: "wait"

  @impl true
  def step("wait", ctx), do: {:await, ["approve", "reject"], "decide", ctx.state}

  def step("decide", ctx) do
    sig = hd(ctx.awaited)
    {:done, %{"decision" => sig.name, "by" => sig.payload}}
  end
end

defmodule GenDurable.Test.AwaitRetry do
  @moduledoc """
  Awaits, then `:retry`s the woken step once before finishing. Proves a retry
  keeps `awaits` and re-sees `ctx.awaited` — if the redo lost them, `hd([])` raises.
  """
  use GenDurable.FSM, name: "await_retry", version: 1, initial: "wait"

  @impl true
  def step("wait", ctx), do: {:await, "go", "woke", ctx.state}

  def step("woke", ctx) do
    sig = hd(ctx.awaited)

    if ctx.attempt < 1,
      do: {:retry, ctx.state, 0},
      else: {:done, %{"v" => sig.payload["v"], "attempt" => ctx.attempt}}
  end
end

defmodule GenDurable.Test.Collector do
  @moduledoc "Awaits a pack {a,b,c}; re-awaits (accumulating) until all arrive, then sums them."
  use GenDurable.FSM, name: "collector", version: 1, initial: "wait"

  @names ["a", "b", "c"]

  @impl true
  def step("wait", ctx) do
    names = MapSet.new(ctx.awaited, & &1.name)

    if MapSet.size(names) == length(@names) do
      # All here: process the whole pack in place, then finish (terminal cleans up).
      total = ctx.awaited |> Enum.map(& &1.payload["v"]) |> Enum.sum()
      {:done, %{"sum" => total, "count" => length(ctx.awaited)}}
    else
      # Re-await without consuming — the pack accumulates across wakeups.
      {:await, @names, "wait", ctx.state}
    end
  end
end

defmodule GenDurable.Test.Crasher do
  @moduledoc "Raises; handle/2 retries a few times then stops."
  use GenDurable.FSM, name: "crasher", version: 1, initial: "boom"

  @impl true
  def step("boom", _ctx), do: raise("kaboom")

  @impl true
  def handle(_reason, ctx) do
    if ctx.attempt < 2, do: {:retry, ctx.state, 0}, else: {:stop, "gave up"}
  end
end

defmodule GenDurable.Test.Thrower do
  @moduledoc "Throws; handle/2 receives {:throw, value} and stops with it."
  use GenDurable.FSM, name: "thrower", version: 1, initial: "hurl"

  @impl true
  def step("hurl", _ctx), do: throw(:bail)

  @impl true
  def handle({:throw, value}, _ctx), do: {:stop, "threw #{inspect(value)}"}
end

defmodule GenDurable.Test.BadResult do
  @moduledoc "Returns an unencodable :done result; serialization must route to handle/2."
  use GenDurable.FSM, name: "bad_result", version: 1, initial: "go"

  @impl true
  def step("go", _ctx), do: {:done, %{"oops" => self()}}
end

defmodule GenDurable.Test.Reborn do
  @moduledoc "Crashes the worker (no outcome) on the first attempt, then succeeds."
  use GenDurable.FSM, name: "reborn", version: 1, initial: "go"

  @impl true
  def step("go", %{attempt: 0}), do: Process.exit(self(), :kill)
  def step("go", _ctx), do: {:done, %{"recovered" => true}}
end

defmodule GenDurable.Test.ConcurrencyInc do
  @moduledoc "Read-sleep-write against a shared Agent to expose lost updates."
  use GenDurable.FSM, name: "concurrency_inc", version: 1, initial: "inc"

  @impl true
  def step("inc", _ctx) do
    agent = GenDurable.Test.ConcurrencyAgent
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

defmodule GenDurable.Test.JobOk do
  @moduledoc "Simplest job: runs and finishes. perform/1, default name (resolved dynamically)."
  use GenDurable.FSM

  @impl true
  def perform(_args), do: :ok
end

defmodule GenDurable.Test.JobResult do
  @moduledoc "Job returning a result map; reads args and ctx via perform/2."
  use GenDurable.FSM

  @impl true
  def perform(args, ctx), do: {:ok, %{"doubled" => args["n"] * 2, "attempt" => ctx.attempt}}
end

defmodule GenDurable.Test.JobRetry do
  @moduledoc "Errors until attempt 2, then succeeds — exercises {:error, _} retry."
  use GenDurable.FSM, max_attempts: 5

  @impl true
  def perform(_args, ctx) do
    if ctx.attempt < 2, do: {:error, "transient"}, else: {:ok, %{"ok_at" => ctx.attempt}}
  end

  # Zero backoff so the test doesn't wait on the schedule.
  @impl true
  def backoff(_attempt), do: 0
end

defmodule GenDurable.Test.JobGiveUp do
  @moduledoc "Always errors — exercises max_attempts exhaustion → failed."
  use GenDurable.FSM, max_attempts: 3

  @impl true
  def perform(_args, _ctx), do: {:error, "always"}

  @impl true
  def backoff(_attempt), do: 0
end

defmodule GenDurable.Test.JobCancel do
  @moduledoc "Cancels — failed immediately, no retry."
  use GenDurable.FSM, max_attempts: 5

  @impl true
  def perform(_args, _ctx), do: {:cancel, "nope"}
end

defmodule GenDurable.Test.RateUnknown do
  @moduledoc "First step names an unconfigured rate_limit → :unknown telemetry (then it stalls)."
  use GenDurable.FSM, name: "rate_unknown", version: 1, initial: "go"

  @impl true
  def step("go", %{state: s}), do: {:next, "fin", s, rate_limit: :ghost}
  def step("fin", _ctx), do: {:done, %{}}
end

defmodule GenDurable.Test.FSMs do
  def all do
    [
      GenDurable.Test.Counter,
      GenDurable.Test.RateUnknown,
      GenDurable.Test.MapCounter,
      GenDurable.Test.Awaiter,
      GenDurable.Test.Selector,
      GenDurable.Test.Collector,
      GenDurable.Test.AwaitRetry,
      GenDurable.Test.Crasher,
      GenDurable.Test.Thrower,
      GenDurable.Test.BadResult,
      GenDurable.Test.Reborn,
      GenDurable.Test.ConcurrencyInc,
      GenDurable.Test.Plain,
      GenDurable.Test.Child,
      GenDurable.Test.Parent,
      GenDurable.Test.Sleeper,
      GenDurable.Test.Recorder,
      GenDurable.Test.Overlap
    ]
  end
end
