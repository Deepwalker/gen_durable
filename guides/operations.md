# Operations

## Migration

The DDL lives in the library; the host writes a one-line migration that delegates to it:

```elixir
defmodule MyApp.Repo.Migrations.SetupGenDurable do
  use Ecto.Migration

  def up,   do: GenDurable.Migration.up()
  def down, do: GenDurable.Migration.down()
end
```

`up/1` records the installed schema version in a table comment and only applies missing
increments, so the host-facing call stays stable as the library evolves. Pre-1.0 there is a
single schema version, edited in place — upgrading means re-creating the schema. `:prefix`
puts the tables in a non-`public` Postgres schema (the runtime also needs it on the repo's
`search_path`).

## Starting the engine

Add it to your supervision tree after your repo:

```elixir
children = [
  MyApp.Repo,
  {GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5]}
]
```

You don't list your FSM modules — they're resolved from the row (the `fsm` column defaults to
the module name). Pass `:fsms` only to register a machine with a custom `:name`, or to keep an
old `:version` running alongside a new one.

Several engine instances can coexist (different repos, disjoint queue sets): give each a
`:name` (an atom, default `GenDurable`) and route API calls to it with the same option —
`GenDurable.insert(Checkout, state: %{...}, name: MyApp.Engine)`. Starting two instances with
the same name fails with `:already_started`.

## Crash recovery: lease + reaper

Every claimed row holds a **lease** (`lease_expires_at`), extended by a heartbeat while the step
runs. If a worker dies mid-step, the lease expires and the **reaper** returns the row to
`runnable` with `attempt += 1` — the step re-runs from scratch. This is the at-least-once safety
floor: a crashed step (no outcome) is retried whether or not your code asked for it, so step
effects must tolerate running again.

Outcomes are **ownership-guarded**: a step that outlives its lease (e.g. its scheduler crashed,
so nothing extended the lease, and the row was reclaimed while it ran) gets its late outcome
dropped rather than committed over the new claimant's work. The drop is observable as
`[:gen_durable, :outcome, :stale]`; the current claimant redoes the step.

A restarted scheduler also **reclaims its dead predecessor's claims at startup**
(`[:gen_durable, :scheduler, :reclaimed]`): rows claimed by an earlier incarnation of the same
instance+queue on the same VM go straight back to `runnable` instead of waiting out the lease.

## Garbage collection

Terminal (`done`/`failed`) rows are deleted by a built-in GC sweep, so finished work doesn't
accumulate forever. The delete scales with the batch, not the table size.

Configured by the `:gc` option — `[interval:, retention:, batch:]`, or `false` to not run GC
on this node (see [Topologies](#topologies)):

- `interval` (default `60_000` ms) — time between sweeps.
- `retention` (default `86_400_000` ms ≈ 1 day) — how long a row is kept after it terminates.
- `batch` (default `10_000`) — max rows per sweep; it re-sweeps at once when a sweep fills.

A terminal child whose parent is still mid-join is spared until the parent finishes.

The sweep also prunes stale [rate-limit buckets](rate_limiting.md): a bucket idle long enough
to have fully refilled (and any bucket whose named limit was removed from the config) is
deleted — it is recreated full on next use, which is exactly the state it would have refilled
to. Partitioned keys (`{name, partition}`) mint a bucket per partition ever seen, so without
this the bucket table would grow without bound.

## Configuration reference

The engine is started as `{GenDurable, opts}`:

| Option | Default | Meaning |
|---|---|---|
| `:name` | `GenDurable` | instance identity; several named engines can coexist |
| `:repo` | — (required) | the host's `Ecto.Repo` |
| `:fsms` | `[]` | modules to register — only for a custom `:name` or versioning |
| `:queues` | `[default: 10]` | `queue_name => concurrency` |
| `:rate_limits` | `[]` | named [token-bucket limits](rate_limiting.md) (`[api: [allowed: 100, period: {1, :minute}, shards: 1]]`) |
| `:concurrency_limits` | `[]` | named [concurrency gates](concurrency.md) for `concurrency_key` (`[api: [limit: 100, shards: 1]]`) |
| `:lease_ttl` | `60_000` | ms a claimed row stays leased before the reaper may reclaim it |
| `:heartbeat_interval` | `20_000` | ms between lease extensions for claimed rows |
| `:poll_interval` | `1_000` | base ms between idle polls (inserts, signal wakes, and fan-out transitions are discovered immediately via the poke transport — see `:poke`; the poll covers retry backoffs and the reaper's wakes) |
| `:poke` | `:local` | how inserts and engine-driven wakes announce runnable work: `:local` (same node), `:cluster` (all nodes, Erlang distribution), `{:redis, url_or_opts}` (Redis Pub/Sub; optional `:redix` dep). Best-effort — the poll is the floor |
| `:await` | `[tick: 25]` | `GenDurable.await/3` watcher probe interval — the latency granularity for results committed on other nodes (same-node results push instantly) |
| `:reaper` | `[interval: 30_000]` | reaper sweeps (the interval is also the [await-timeout](signals.md#timeouts) resolution); `false` = none on this node |
| `:gc` | `[interval: 60_000, retention: 86_400_000, batch: 10_000]` | GC sweeps; `false` = none on this node |
| `:prefetch` | `0` | rows each queue buffers beyond its running slots |
| `:min_demand` | `1` | skip picking unless at least this many slots are free |
| `:max_poll_interval` | `5_000` | idle-backoff ceiling for the poll interval |
| `:drain_timeout` | `5_000` | ms each queue waits for in-flight steps on shutdown |

Keep `heartbeat_interval × 3 ≲ lease_ttl` for margin (the defaults satisfy this).

### Tuning the feeder

`:prefetch`, `:min_demand`, and `:max_poll_interval` are the feeder aggressiveness knobs.
Defaults are conservative (fair across nodes, low idle DB chatter). Raising `:prefetch` claims
work ahead into an in-memory buffer — buffered rows are heartbeated, so depth is safe against
`:lease_ttl`, but a deep buffer trades off cross-node fairness, priority freshness, and crash
blast radius.

## Topologies

Every node runs the full engine by default. Three knobs shape a node's role: `:queues`
(schedulers), `:reaper`, and `:gc`.

```elixir
# worker node — executes; maintenance left to others
{GenDurable, repo: MyApp.Repo, queues: [default: 10], reaper: false, gc: false}

# web node — only inserts and signals; runs nothing
{GenDurable, repo: MyApp.Repo, queues: [], reaper: false, gc: false}

# maintenance node — reaper + GC, no execution
{GenDurable, repo: MyApp.Repo, queues: []}
```

> **The cluster must run at least one reaper and one GC somewhere.** Without a reaper, a
> crashed worker's rows stay `executing` forever — no retry, and a K = 1 `concurrency_key`
> they hold stays blocked. Without GC, terminal rows accumulate, stale rate buckets are never
> pruned, and concurrency-gate counters are never reconciled after crash leaks. These are
> per-node placement knobs, not feature switches.

Running reaper/GC on **several** nodes is safe — the sweeps claim via ordered
`SKIP LOCKED`, so concurrent sweeps skip each other's work — just redundant. There is no
leader election, deliberately: correctness never depends on "exactly one".

In a split topology, pair the queues with a `:poke` transport: with the default
`poke: :local` a web node has nobody to poke, and workers discover its inserts on their
next poll. `poke: :cluster` (Erlang distribution) or `poke: {:redis, url}` (no
distribution required) delivers the nudge to the worker nodes immediately — see
`GenDurable.Poke`.

Seeding of `rate_limits:` / `concurrency_limits:` follows the config itself: a node seeds
what it declares and touches nothing else. Keep the declarations on the nodes that own them
(two nodes declaring the *same name* differently last-write-win on every boot).

## Telemetry

Attach to these `[:gen_durable, …]` events:

| Event | When | Measurements / Metadata |
|---|---|---|
| `[:step, :stop]` | a step finished | `%{duration}` / `%{id, fsm, step, kind}` |
| `[:pick, :stop]` | a picker batch ran | `%{count, demand}` / `%{queue, worker}` |
| `[:scheduler, :saturation]` | per-poll gauge | `%{in_flight, buffer, concurrency, prefetch}` / `%{queue}` |
| `[:scheduler, :drain]` | graceful queue shutdown | `%{released, in_flight}` / `%{queue}` |
| `[:scheduler, :reclaimed]` | startup reclaim of a dead predecessor's claims | `%{count}` / `%{queue}` |
| `[:concurrency, :contended]` | a cross-node [concurrency_key](concurrency.md) claim race; the pick retried | `%{count}` / `%{queue}` |
| `[:concurrency, :throttled]` | a [concurrency gate](concurrency.md) admitted fewer than wanted | `%{wanted, admitted}` / `%{key, queue}` |
| `[:outcome, :stale]` | a reclaimed row rejected its old worker's outcome | `%{count}` / `%{id, fsm, step, kind}` |
| `[:reaper, :reaped]` | expired leases reclaimed | `%{count}` / `%{}` |
| `[:await, :timeout]` | [await deadlines](signals.md#timeouts) fired | `%{count}` / `%{}` |
| `[:gc, :swept]` | terminal rows / stale buckets deleted, gate counters reconciled | `%{count, buckets, gates}` / `%{}` |
| `[:rate_limit, :throttled]` | a [bucket](rate_limiting.md) bit | `%{wanted, granted}` / `%{key, queue}` |
| `[:rate_limit, :unknown]` | a step named an unconfigured limit | `%{count}` / `%{key, name, fsm, step}` |
