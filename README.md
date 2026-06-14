# gen_durable

A Postgres-backed durable FSM engine for Elixir, on top of GenServer.

**The one guarantee:** on step completion, the new state is committed to the database
before execution proceeds. On a crash before commit, the step re-executes from scratch
(at-least-once). Idempotency of step effects is the user's responsibility.

See [`gen_durable_spec.md`](gen_durable_spec.md) for the normative specification and
[`gen_durable_plan.md`](gen_durable_plan.md) for the implementation roadmap.

## Two primitives

- **durable step** — user code that returns an outcome on completion.
- **durable await** — a step parks the instance until a named signal arrives.

Everything else (fan-out, fan-in) is expressed with these two in user code. The engine
knows nothing about parent/child trees — "children" are ordinary independent instances.

## Step outcomes

| Outcome | Effect |
|---|---|
| `{:next, step, state}` | transition to `step`, `runnable`, `attempt := 0` |
| `{:replay, state, delay_ms}` | same step again, `runnable`, `attempt += 1`, after `delay_ms` |
| `{:await, signal_name, state}` | park, `awaiting_signal` |
| `{:done, result}` | terminal, `done` |
| `{:stop, reason}` | terminal, `failed` |

## Usage

Define the state (a typed Ecto embedded schema) and the machine:

```elixir
defmodule Checkout.State do
  use GenDurable.State

  embedded_schema do
    field :order, :integer
    field :n, :integer, default: 0
  end
end

defmodule Checkout do
  use GenDurable.FSM, version: 1, queue: "checkout", state: Checkout.State, initial: "start"

  @impl true
  def step("start", %{state: s}), do: {:next, "await_pay", %{s | n: s.n + 1}}

  def step("await_pay", ctx) do
    case Enum.find(ctx.signals, &(&1.name == "payment_confirmed")) do
      nil -> {:await, "payment_confirmed", ctx.state}
      sig -> {:next, "ship", apply_payment(ctx.state, sig)}
    end
  end

  def step("ship", _ctx), do: {:done, %{"shipped" => true}}

  @impl true
  def handle(reason, ctx) do
    if ctx.attempt < 5, do: {:replay, ctx.state, 1_000 * ctx.attempt}, else: {:stop, reason}
  end
end
```

Migrate (the DDL lives in the library, Oban-style):

```elixir
defmodule MyApp.Repo.Migrations.SetupGenDurable do
  use Ecto.Migration

  def up,   do: GenDurable.Migration.up()
  def down, do: GenDurable.Migration.down()
end
```

Start the engine in your supervision tree and use it:

```elixir
children = [
  MyApp.Repo,
  {GenDurable, repo: MyApp.Repo, fsms: [Checkout], queues: [default: 10, checkout: 5]}
]

{:ok, id} = GenDurable.insert(Checkout, state: %{order: 42}, partition_key: "order:42")
:ok = GenDurable.signal(id, "payment_confirmed", %{amount: 100}, dedup_key: "evt-7")
```

## Development

The toolchain (Elixir 1.18 / OTP 27 + Postgres) is pinned in `.devcontainer/`.

```bash
docker compose -p gen_durable -f .devcontainer/docker-compose.yml up -d --build
docker compose -p gen_durable -f .devcontainer/docker-compose.yml exec app mix test
```
