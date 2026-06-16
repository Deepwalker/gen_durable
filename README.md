# gen_durable

A Postgres-backed durable FSM engine for Elixir, on top of GenServer.

**The one guarantee:** on step completion, the new state is committed to the database
before execution proceeds. On a crash before commit, the step re-executes from scratch
(at-least-once). Idempotency of step effects is the user's responsibility.

See [`gen_durable_spec.md`](gen_durable_spec.md) for the normative specification,
[`gen_durable_plan.md`](gen_durable_plan.md) for the implementation roadmap, and
[`PERFORMANCE.md`](PERFORMANCE.md) for the cost model and EXPLAIN plans.

## Three primitives

- **durable step** — user code that returns an outcome on completion.
- **durable await** — a step parks the instance until a signal from a named set
  arrives, then runs a chosen step with the matched signals in `ctx.awaited` (the
  full inbox is always in `ctx.all`). It is sugar over a step that inspects the
  inbox itself; the engine just pre-filters the set you waited for and consumes it.
- **durable childs** — a step fans out a batch of child instances and parks on a
  built-in await-on-all-children barrier; when every child reaches a terminal
  state the parent's next step runs with `ctx.childs` holding their results.

## Step outcomes

| Outcome | Effect |
|---|---|
| `{:next, step, state}` | transition to `step`, `runnable`, `attempt := 0` |
| `{:replay, state, delay_ms}` | same step again, `runnable`, `attempt += 1`, after `delay_ms` |
| `{:await, names, next_step, state}` | park (`awaiting_signal`) on one name or a set; when any arrives, run `next_step` with the matched signals as `ctx.awaited` |
| `{:schedule_childs, next_step, children, state}` | spawn `children`, park on the join barrier (`awaiting_children`); run `next_step` once all finish |
| `{:done, result}` | terminal, `done` |
| `{:stop, reason}` | terminal, `failed` |

Each child is `{FsmModule, insert_opts}` (or a bare `FsmModule`); they are ordinary
instances stamped with a `parent_id`, and a failed child still releases its slot in
the barrier.

## Usage

The simplest durable unit is a **job**: one function that runs to completion, with
retries for free. Define `perform/1` or `perform/2`:

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

`perform` returns `:ok` / `{:ok, result}` (→ `done`), `{:error, reason}` (retried with
`backoff/1` until `:max_attempts`, default 20, then `failed`), or `{:cancel, reason}`
(`failed`, no retry); a raised exception is treated as `{:error, _}`.

For multiple steps, awaits, or child fan-out, define `step/2` instead — the full
state machine, with its state as a nested typed Ecto embedded schema (adopted by
convention — no `state:` opt needed). A module defines **either** `step/2` **or**
`perform`, never both:

```elixir
defmodule Checkout do
  use GenDurable.FSM, version: 1, queue: "checkout", initial: "start"

  defmodule State do
    use GenDurable.State

    embedded_schema do
      field :order, :integer
      field :n, :integer, default: 0
    end
  end

  @impl true
  def step("start", %{state: s}) do
    # park until the signal arrives, then run "ship" with it in ctx.awaited
    {:await, "payment_confirmed", "ship", %{s | n: s.n + 1}}
  end

  def step("ship", ctx), do: {:done, %{"shipped" => true, "paid" => hd(ctx.awaited).payload}}

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
  {GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5]}
]

{:ok, id} = GenDurable.insert(Checkout, state: %{order: 42}, partition_key: "order:42")
:ok = GenDurable.signal(id, "payment_confirmed", %{amount: 100}, dedup_key: "evt-7")
```

You don't list your FSM modules: they're resolved from the row (the `fsm` column
defaults to the module name). Pass `:fsms` only to register a machine with a
custom `:name`, or to keep an old `:version` running alongside a new one (spec §8).

## Configuration

The engine is started as `{GenDurable, opts}`. Full reference lives in the
`GenDurable.Supervisor` and `GenDurable.Scheduler` docs; the options:

| Option | Default | Meaning |
|---|---|---|
| `:repo` | — (required) | the host's `Ecto.Repo` |
| `:fsms` | `[]` | FSM modules to register — only for custom `:name` or versioning; otherwise resolved from the row |
| `:queues` | `[default: 10]` | `queue_name => concurrency` (max Tasks running at once) |
| `:lease_ttl` | `60_000` | ms a claimed row stays leased before the reaper may reclaim it |
| `:heartbeat_interval` | `20_000` | ms between lease extensions for claimed rows (`buffer ++ in_flight`) |
| `:poll_interval` | `1_000` | base ms between idle polls |
| `:reap_interval` | `30_000` | ms between reaper sweeps |
| `:gc_interval` | `60_000` | ms between GC sweeps of terminal rows; `nil` disables GC |
| `:gc_retention` | `86_400_000` | ms a `done`/`failed` row is kept after terminating before GC may delete it (default 1 day) |
| `:gc_batch` | `10_000` | max rows deleted per GC sweep (it re-sweeps at once when a sweep fills the batch) |
| `:prefetch` | `0` | rows each queue claims into its buffer **beyond** its running slots |
| `:min_demand` | `1` | batch gate: skip picking unless at least this many slots are free |
| `:max_poll_interval` | `5_000` | idle-backoff ceiling: an empty pick on an idle queue doubles the poll interval up to here, then snaps back when work appears |
| `:drain_timeout` | `5_000` | on shutdown, ms each queue waits for in-flight steps to finish before giving up to the reaper (buffered, un-started rows are released immediately) |

Timings are in milliseconds; keep `heartbeat_interval × 3 ≲ lease_ttl` for margin
(the "Balanced" defaults satisfy this).

`:prefetch`, `:min_demand`, and `:max_poll_interval` are the **feeder aggressiveness**
knobs. Defaults are conservative (fair across nodes, low idle DB chatter). Raising
`:prefetch` claims more work ahead into an in-memory buffer — buffered rows are
heartbeated, so depth is safe with respect to `:lease_ttl`, but a deep buffer trades
off cross-node fairness, priority freshness, and crash blast radius. See
`GenDurable.Scheduler` for the trade-offs.

Per-instance options to `insert/2` / `insert_all/3`: `:state`, `:step`, `:queue`,
`:priority`, `:partition_key`, `:unique_key`, `:unique_scope`, and scheduling sugar
`:eligible_at` (a `DateTime`) / `:schedule_at` (a `DateTime`) / `:schedule_in` (ms).

## Development

The toolchain (Elixir 1.18 / OTP 27 + Postgres) is pinned in `.devcontainer/`.

```bash
docker compose -p gen_durable -f .devcontainer/docker-compose.yml up -d --build
docker compose -p gen_durable -f .devcontainer/docker-compose.yml exec app mix test
```
