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

defmodule GenDurable.Test.AwaitTimeout do
  @moduledoc "Awaits with a timeout; empty ctx.awaited on wake means the timeout fired."
  use GenDurable.FSM, name: "await_timeout", version: 1, initial: "wait"

  @impl true
  def step("wait", ctx), do: {:await, "go", "woke", ctx.state, timeout: 300}
  def step("woke", %{awaited: []}), do: {:done, %{"timed_out" => true}}
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

defmodule GenDurable.Test.CrossQueueParent do
  @moduledoc "Fans children into ANOTHER queue — proves fan-out pokes across queues."
  use GenDurable.FSM, name: "xparent", version: 1, initial: "fan"

  @impl true
  def step("fan", %{state: s}) do
    kids =
      for x <- 1..Map.get(s, "n", 2),
          do: {GenDurable.Test.Child, state: %{x: x}, queue: "kids"}

    {:schedule_childs, "join", kids, s}
  end

  def step("join", %{childs: childs}),
    do: {:done, %{"done" => Enum.count(childs, &(&1.status == "done"))}}
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

defmodule GenDurable.Test.InlineChain do
  @moduledoc """
  Inline FSM: three steps back-to-back. Each records `{step, self()}` into `InlineAgent`
  so a test can prove every step ran in ONE worker task (inline) — no requeue/re-pick.
  """
  use GenDurable.FSM, name: "inline_chain", version: 1, initial: "a", inline_execution: true

  @impl true
  def step(name, %{state: s}) do
    Agent.update(GenDurable.Test.InlineAgent, &(&1 ++ [{name, self()}]))

    case name do
      "a" -> {:next, "b", s}
      "b" -> {:next, "c", s}
      "c" -> {:done, %{"ok" => true}}
    end
  end
end

defmodule GenDurable.Test.InlineBoundary do
  @moduledoc """
  Inline FSM with a per-step opt-out: step `a` forces a requeue (`inline_execution: false`),
  so only `b`→`c` chains inline (asserted via the `run_ahead` telemetry).
  """
  use GenDurable.FSM, name: "inline_boundary", version: 1, initial: "a", inline_execution: true

  @impl true
  def step("a", %{state: s}), do: {:next, "b", s, inline_execution: false}
  def step("b", %{state: s}), do: {:next, "c", s}
  def step("c", _ctx), do: {:done, %{"ok" => true}}
end

defmodule GenDurable.Test.InlineRate do
  @moduledoc "Inline FSM whose 2nd step wants `weight` tokens of the `:tight` rate limit."
  use GenDurable.FSM, name: "inline_rate", version: 1, initial: "a", inline_execution: true

  @impl true
  def step("a", %{state: s}), do: {:next, "b", s, rate_limit: :tight, weight: Map.get(s, "w", 1)}
  def step("b", _ctx), do: {:done, %{"ok" => true}}
end

defmodule GenDurable.Test.InlineConc do
  @moduledoc "Inline FSM that adopts a NEW `concurrency_key` on its 2nd step (state drives the key)."
  use GenDurable.FSM, name: "inline_conc", version: 1, initial: "a", inline_execution: true

  @impl true
  def step("a", %{state: s}), do: {:next, "b", s, concurrency_key: Map.fetch!(s, "key")}
  def step("b", _ctx), do: {:done, %{"ok" => true}}
end

defmodule GenDurable.Test.FSMs do
  def all do
    [
      GenDurable.Test.Counter,
      GenDurable.Test.InlineChain,
      GenDurable.Test.InlineBoundary,
      GenDurable.Test.InlineRate,
      GenDurable.Test.InlineConc,
      GenDurable.Test.RateUnknown,
      GenDurable.Test.MapCounter,
      GenDurable.Test.Awaiter,
      GenDurable.Test.AwaitTimeout,
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
      GenDurable.Test.CrossQueueParent,
      GenDurable.Test.Sleeper,
      GenDurable.Test.Recorder,
      GenDurable.Test.Overlap
    ]
  end
end
