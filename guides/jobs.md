# Jobs

The simplest durable unit is a **job**: one function that runs to completion, with retries
for free. Define `perform/1` or `perform/2` (instead of `step/2`) and you get a one-shot
durable job — no step names, no outcome tuples.

```elixir
defmodule Cleanup do
  use GenDurable.FSM

  @impl true
  def perform(args, _ctx) do
    File.rm_rf!(args["path"])
    :ok
  end
end

GenDurable.insert(Cleanup, args: %{"path" => "/tmp/x"})
```

`perform` receives the instance args (the plain map passed as `:args`/`:state`, or the typed
struct if a [`State` schema](machines.md#state) is declared) and, in the `/2` form, the
`t:GenDurable.Context.t/0`.

## Return values

| Return | Result |
|---|---|
| `:ok` | terminal — `done`, empty result |
| `{:ok, map}` | terminal — `done`, `map` recorded as the result |
| `{:error, reason}` | retried with `backoff/1` until `:max_attempts`, then `failed` |
| `{:cancel, reason}` | `failed` immediately, no retry |
| a raised exception | treated as `{:error, exception}` |

## Retries and backoff

`{:error, _}` (or a raised exception) is retried until `:max_attempts` (default `20`), then
the instance is `failed`. The delay between attempts is `backoff/1` — a capped exponential by
default (`min(1000 * 2^attempt, 300_000)` ms). Override either:

```elixir
defmodule Charge do
  use GenDurable.FSM, max_attempts: 10

  @impl true
  def perform(args, ctx) do
    case Stripe.charge(args["amount"]) do
      :ok -> :ok
      {:error, :card_declined} -> {:cancel, :declined}   # don't retry a hard decline
      {:error, _transient} -> {:error, :try_again}        # retried with backoff
    end
  end

  @impl true
  def backoff(attempt), do: 5_000 * (attempt + 1)         # linear, 5s steps
end
```

`ctx.attempt` (0-based) is available in `perform/2` and `backoff/1`.

## When you outgrow a job

A job is a degenerate one-step machine. The moment you need to wait for an external event,
fan work out, or move through named phases, switch to [`step/2`](machines.md) — a module
defines **either** `perform` **or** `step/2`, never both.
