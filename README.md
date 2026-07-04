# gen_durable

A Postgres-backed durable-execution engine for Elixir. You declare a finite-state machine; the
engine commits its state to Postgres before each step proceeds, so an instance survives process
and node death and resumes where it left off.

Inspired by durable-execution systems (Temporal, DBOS) and Postgres-backed job runners (Oban) —
but the unit of durability is an explicit FSM step, and the state lives in the database, not in a
process. There is **no GenServer per instance**: an FSM is a row, and each step runs as an
ephemeral task. The runtime backbone (scheduler, reaper, GC) is a small set of GenServers that
pick runnable rows and dispatch them.

**The one guarantee:** on step completion, the new state is committed to the database before
execution proceeds. On a crash before commit, the step re-executes from scratch (at-least-once).
Idempotency of step effects is the user's responsibility.

## Install

```elixir
def deps, do: [{:gen_durable, "~> 0.2.0"}]
```

Add the migration (the DDL lives in the library) and run it:

```elixir
defmodule MyApp.Repo.Migrations.SetupGenDurable do
  use Ecto.Migration

  def up,   do: GenDurable.Migration.up()
  def down, do: GenDurable.Migration.down()
end
```

Start the engine in your supervision tree, after your repo:

```elixir
children = [
  MyApp.Repo,
  {GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5]}
]
```

## A first machine

```elixir
defmodule Checkout do
  use GenDurable.FSM, queue: "checkout"

  defmodule State do
    use GenDurable.State
    embedded_schema do
      field :order, :integer
    end
  end

  @impl true
  # park until the payment webhook fires, then run "ship" with it in ctx.awaited
  def step("start", ctx), do: {:await, "payment_confirmed", "ship", ctx.state}
  def step("ship",  ctx), do: {:done, %{"order" => ctx.state.order, "paid" => hd(ctx.awaited).payload}}
end

{:ok, _id} = GenDurable.insert(Checkout, state: %{order: 42}, correlation_key: "order:42")

# later, from a webhook that only knows the business key:
GenDurable.signal("order:42", "payment_confirmed", %{amount: 100})
```

For the trivial "run once and finish" case, define [`perform/1`](guides/jobs.md) instead of
`step/2` and you get a durable job with retries for free.

## Features

| Guide | What |
|---|---|
| [Jobs](guides/jobs.md) | one-shot durable jobs (`perform/1\|2`) with retries and backoff |
| [State machines](guides/machines.md) | `step/2`, typed `State`, the outcome contract, error handling |
| [Signals & await](guides/signals.md) | park on external events; durable, at-least-once, sets and packs |
| [Child fan-out](guides/children.md) | `schedule_childs` — fan work out, join on all of it |
| [Rate limiting](guides/rate_limiting.md) | per-step token-bucket limits, partitioned, weighted |
| [Concurrency keys](guides/concurrency.md) | serialize per key, parallel across keys |
| [Instance identity](guides/identity.md) | `correlation_key` — address a signal by business key + dedup |
| [Scheduling & queues](guides/scheduling.md) | delays, priority, queues, recurring work |
| [Operations](guides/operations.md) | migration, crash recovery, GC, the config reference, telemetry |

## Documentation

- **[Performance](PERFORMANCE.md)** — the cost model, the picker, and EXPLAIN plans.
- **[Changelog](CHANGELOG.md)**.

## Development

The toolchain (Elixir 1.18 / OTP 27 + Postgres) is pinned in `.devcontainer/`.

```bash
make up     # build the devcontainer
make test   # run the suite
```
