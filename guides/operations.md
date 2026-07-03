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
increments, so the host-facing call stays stable as the library evolves. `:prefix` puts the
tables in a non-`public` Postgres schema.

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

- `:gc_interval` (default `60_000` ms) — time between sweeps; `nil` disables GC entirely.
- `:gc_retention` (default `86_400_000` ms ≈ 1 day) — how long a row is kept after it terminates.
- `:gc_batch` (default `10_000`) — max rows per sweep; it re-sweeps at once when a sweep fills.

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
| `:rate_limits` | `[]` | named [token-bucket limits](rate_limiting.md) |
| `:lease_ttl` | `60_000` | ms a claimed row stays leased before the reaper may reclaim it |
| `:heartbeat_interval` | `20_000` | ms between lease extensions for claimed rows |
| `:poll_interval` | `1_000` | base ms between idle polls |
| `:reap_interval` | `30_000` | ms between reaper sweeps |
| `:gc_interval` | `60_000` | ms between GC sweeps; `nil` disables GC |
| `:gc_retention` | `86_400_000` | ms a terminal row is kept before GC may delete it |
| `:gc_batch` | `10_000` | max rows deleted per GC sweep |
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

## Telemetry

Attach to these `[:gen_durable, …]` events:

| Event | When | Measurements / Metadata |
|---|---|---|
| `[:step, :stop]` | a step finished | `%{duration}` / `%{id, fsm, step, kind}` |
| `[:pick, :stop]` | a picker batch ran | `%{count, demand}` / `%{queue, worker}` |
| `[:scheduler, :saturation]` | per-poll gauge | `%{in_flight, buffer, concurrency, prefetch}` / `%{queue}` |
| `[:scheduler, :drain]` | graceful queue shutdown | `%{released, in_flight}` / `%{queue}` |
| `[:concurrency, :contended]` | a [concurrency_key](concurrency.md) lock was contended | `%{count}` / `%{id, fsm, concurrency_key}` |
| `[:reaper, :reaped]` | expired leases reclaimed | `%{count}` / `%{ids}` |
| `[:gc, :swept]` | terminal rows deleted | `%{count}` / `%{}` |
| `[:rate_limit, :throttled]` | a [bucket](rate_limiting.md) bit | `%{wanted, granted}` / `%{key, queue}` |
| `[:rate_limit, :unknown]` | a step named an unconfigured limit | `%{count}` / `%{key, name, fsm, step}` |
