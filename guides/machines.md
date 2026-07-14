# State machines

A durable FSM is a module with a `step/2` clause per step name. The engine's one guarantee:
**on step completion, the new state is committed to the database before execution proceeds.**
On a crash before commit, the step re-executes from scratch (at-least-once). The unit of
re-execution is the **whole step** — make each step idempotent, and keep steps small.

```elixir
defmodule Checkout do
  use GenDurable.FSM, version: 1, queue: "checkout"

  defmodule State do
    use GenDurable.State

    embedded_schema do
      field :order, :integer
      field :n, :integer, default: 0
    end
  end

  @impl true
  def step("start", %{state: s}), do: {:next, "ship", %{s | n: s.n + 1}}
  def step("ship", %{state: s}), do: {:done, %{"order" => s.order}}
end

{:ok, id} = GenDurable.insert(Checkout, state: %{order: 42})
```

The `step` text column maps to a function-clause head; the instance walks from clause to clause
until a terminal outcome.

## State

`ctx.state` is the instance's state, loaded before every step and committed after it.

- **Nested `State` schema (recommended).** Declare a `defmodule State` with `use
  GenDurable.State` inside the FSM module; it is adopted by convention (no option needed) and
  `ctx.state` is that typed struct. Return a struct of the same type from your outcome.
- **Explicit `:state`.** Point at a schema module elsewhere with `use GenDurable.FSM, state:
  MyState`.
- **Plain map.** Omit both; `ctx.state` is a string-keyed map. Supported but untyped.

## Outcomes

A step (and the error handler `handle/2`) returns one of:

| Outcome | Effect |
|---|---|
| `{:next, step, state}` | transition to `step`, `runnable`, `attempt := 0` |
| `{:next, step, state, opts}` | …with per-transition [`rate_limit:`/`weight:`](rate_limiting.md), [`concurrency_key:`](concurrency.md), or `inline_execution:` (below) |
| `{:retry, state, delay_ms}` | re-run the **same** step after `delay_ms`, `attempt += 1` |
| `{:await, names, next_step, state}` | park until a [signal](signals.md) arrives |
| `{:await, names, next_step, state, timeout: ms}` | …but wake after `ms` even without one |
| `{:schedule_childs, next_step, children, state}` | [fan out](children.md) and wait for the children |
| `{:done, result}` | terminal, `done`, `result` recorded |
| `{:stop, reason}` | terminal, `failed`, `reason` recorded |

`:next` resets the attempt counter (a fresh step); `:retry` keeps re-running the current step
with `attempt += 1` (the poll/backoff primitive) — the delay and the retry policy are yours,
computed from `ctx.attempt`.

## Inline execution (run-ahead)

By default a `:next` commits the row back to `runnable` and the picker claims it again for the
next step — durable, but a full re-pick per step. `use GenDurable.FSM, inline_execution: true`
runs the next step **in the same worker**, on the claim already held, when its rate/concurrency
tokens can be secured:

```elixir
defmodule Pipeline do
  use GenDurable.FSM, inline_execution: true

  def step("fetch",   ctx), do: {:next, "parse",   fetch(ctx.state)}
  def step("parse",   ctx), do: {:next, "call_api", parse(ctx.state), concurrency_key: {:vendor, ctx.state.vendor}}
  def step("call_api", ctx), do: {:next, "persist", call(ctx.state), rate_limit: :vendor}
  def step("persist", ctx), do: {:done, persist(ctx.state)}
end
```

Each step still declares its own [`rate_limit:`](rate_limiting.md) and
[`concurrency_key:`](concurrency.md); the engine secures those tokens out-of-band before
running the next step inline. **If a token is denied** (rate exhausted, concurrency slot full,
or an unconfigured key is held elsewhere), that step is *not* run inline — the row requeues and
the picker admits it exactly as without inlining. So inline is a throughput optimization, never
a change in what runs: the same limits hold. Durability is unchanged too — the next step's
state is committed before it runs, and a crash re-runs it.

Override the FSM default for a single transition with `inline_execution:` in the opts — force a
boundary step to requeue (so the picker re-checks priority) even in an inline FSM:

```elixir
def step("charge", ctx), do: {:next, "settle", ctx.state, inline_execution: false}
```

A caveat worth knowing: an inline chain holds one executor slot and doesn't let the picker
re-evaluate priority until it yields (a terminal/`await`/`retry` outcome, a denied token, or a
step with `inline_execution: false`). Reach for it on throughput-bound pipelines; drop a
boundary `inline_execution: false` where a higher-priority backlog must be able to cut in.

## Errors

A raised exception routes to `handle(reason, ctx)`, which returns any outcome above (it
defaults to `{:stop, reason}`, and is overridable):

```elixir
@impl true
def handle(reason, ctx) do
  if ctx.attempt < 5, do: {:retry, ctx.state, 1_000 * ctx.attempt}, else: {:stop, reason}
end
```

An uncaught `throw` routes there too, as `{:throw, value}` — it is a controlled non-local
return, not a crash. A worker **crash** (a bare `exit`, a kill — no return at all) is
different: the [reaper](operations.md#reaper) recovers it and re-runs the step from
scratch — `handle/2` is *not* called.

## Options

`use GenDurable.FSM, ...`:

- `:name` — the `fsm` column value (default `inspect(module)`).
- `:version` — old versions coexist as separately-registered modules; instances finish on the
  version they started.
- `:queue` — default queue for instances.
- `:initial` — the first step (default `"start"`).
- `:state` — the `GenDurable.State` schema module (usually unnecessary; see above).
- `:inline_execution` — run `:next` steps inline in the same worker (default `false`); see
  [Inline execution](#inline-execution-run-ahead).
