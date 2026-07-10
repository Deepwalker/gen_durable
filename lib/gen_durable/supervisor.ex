defmodule GenDurable.Supervisor do
  @moduledoc """
  Top-level engine supervisor. Started by the host (Oban-style) as
  `{GenDurable, opts}` in their own supervision tree.

  ## Options

    * `:name` — the instance identity (an atom, default `GenDurable`). Everything
      an instance owns — its supervisor registration, task supervisor, config
      entry, FSM registry — is keyed by it, so several engines (e.g. different
      repos, or disjoint queue sets) coexist; address API calls with `name:`.
      Starting two instances with the same name fails with `:already_started`.
    * `:repo` — the host's `Ecto.Repo` (required).
    * `:fsms` — FSM modules to register explicitly (default `[]`). Only needed for a
      custom `:name` or to keep an old `:version` running; otherwise a machine is
      resolved from the `fsm` column (its module name). See `GenDurable.Registry`.
    * `:queues` — keyword list of `queue_name => concurrency`
      (default `[default: 10]`). `[]` runs no schedulers — this node only
      inserts/signals (see the topologies section of the operations guide).
    * `:lease_ttl`, `:heartbeat_interval`, `:poll_interval` —
      timings in ms (balanced defaults: 60_000 / 20_000 / 1_000).
    * `:reaper` — the lease-reclamation sweeper: `[interval: 30_000]` (ms), or
      `false` to not run one on this node.
    * `:gc` — the retention/maintenance sweeper: `[interval: 60_000,
      retention: 86_400_000, batch: 10_000]` (ms between sweeps; ms a
      `done`/`failed` row is kept after it terminates; max rows deleted per
      sweep), or `false` to not run one on this node.

  `reaper: false` / `gc: false` are **per-node** knobs for topology (a web-only
  or worker-only node), not switches you may flip everywhere: the cluster needs
  at least one node running each, or expired leases are never reclaimed and
  terminal rows / stale limiter counters accumulate forever. Running them on
  several nodes is safe (sweeps claim via ordered `SKIP LOCKED`), just
  redundant.
    * `:prefetch` — extra rows each queue claims and buffers beyond its running
      slots (default `0` ⇒ no over-fetch). See `GenDurable.Scheduler`.
    * `:min_demand` — batch gate for the picker (default `1`).
    * `:max_poll_interval` — idle-backoff ceiling in ms (default `5_000`).
    * `:drain_timeout` — on shutdown, how long (ms) each queue waits for its
      in-flight steps to finish before giving up to the reaper (default `5_000`).
      Buffered (un-started) rows are released immediately regardless.
    * `:rate_limits` — named token-bucket rate limits (default `[]`), e.g.
      `[stripe: [allowed: 100, period: {1, :minute}], emails: [allowed: 5, period: 60, burst: 10]]`.
      `allowed`/`period` set the sustained rate; `burst` (default `allowed`) the capacity.
      A step opts into a limit by returning `rate_limit: :stripe` (or `{:stripe, key}`).
    * `:concurrency_limits` — named concurrency caps for `concurrency_key` (default
      `[]`), e.g. `[stripe: [limit: 1000, shards: 10]]`: at most `limit` concurrent
      executions per key whose name matches (`concurrency_key: {:stripe, tenant}` ⇒
      key `"stripe:tenant"`, name `"stripe"`). An unconfigured key keeps the default
      limit of 1 (mutual exclusion). `shards` (default 1) splits the cap across
      bucket rows — completions of one key serialize per shard, so size
      `shards ≥ limit × commit_latency / step_duration`. **Careful**: a config name
      captures every key with that prefix — configuring a name that collides with
      identity-style keys (e.g. `order:` used for mutual exclusion) silently raises
      their limit and breaks the exclusion.

  `:prefetch`, `:min_demand`, and `:max_poll_interval` are the feeder
  aggressiveness knobs and apply to every queue; see `GenDurable.Scheduler` for
  the trade-offs (DB chatter vs. latency vs. cross-node fairness).
  """

  use Supervisor
  require Logger

  @defaults [
    fsms: [],
    queues: [default: 10],
    lease_ttl: 60_000,
    heartbeat_interval: 20_000,
    poll_interval: 1_000,
    reaper: [],
    gc: [],
    prefetch: 0,
    min_demand: 1,
    max_poll_interval: 5_000,
    drain_timeout: 5_000,
    rate_limits: [],
    concurrency_limits: []
  ]

  @reaper_defaults [interval: 30_000]
  @gc_defaults [interval: 60_000, retention: 86_400_000, batch: 10_000]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, GenDurable))
  end

  @impl true
  def init(opts) do
    opts = Keyword.merge(@defaults, opts)
    repo = Keyword.fetch!(opts, :repo)
    name = Keyword.get(opts, :name, GenDurable)

    # Validate before any side effect (persistent_term, config seeding).
    reaper_cfg = component_opts(opts, :reaper, @reaper_defaults)
    gc_cfg = component_opts(opts, :gc, @gc_defaults)

    rate_configs = parse_rate_limits(Keyword.fetch!(opts, :rate_limits))

    config = %{
      name: name,
      repo: repo,
      # The FSM registry table is owned by this supervisor process — it lives
      # and dies with the instance, and executors reach it through the config.
      registry: GenDurable.Registry.new(Keyword.fetch!(opts, :fsms)),
      lease_ttl_ms: Keyword.fetch!(opts, :lease_ttl),
      rate_limit_names: MapSet.new(rate_configs, & &1.name)
    }

    # Keyed by the instance name, so engines coexist. Not erased on shutdown (a
    # Supervisor has no terminate hook); a stale entry is harmless — API writes
    # against a stopped engine are legal, the rows wait in the database.
    :persistent_term.put({GenDurable, name}, config)

    # Seed the policy tables so the picker can join them. No-ops when empty.
    :ok = GenDurable.Queries.upsert_rate_configs(repo, rate_configs)

    :ok =
      GenDurable.Queries.upsert_concurrency_configs(
        repo,
        parse_concurrency_limits(Keyword.fetch!(opts, :concurrency_limits))
      )

    task_sup = Module.concat(name, TaskSupervisor)

    schedulers =
      for {queue_name, concurrency} <- Keyword.fetch!(opts, :queues) do
        queue = to_string(queue_name)

        drain_timeout = Keyword.fetch!(opts, :drain_timeout)

        spec = %{
          config: config,
          queue: queue,
          concurrency: concurrency,
          prefetch: Keyword.fetch!(opts, :prefetch),
          min_demand: Keyword.fetch!(opts, :min_demand),
          poll_interval: Keyword.fetch!(opts, :poll_interval),
          max_poll_interval: Keyword.fetch!(opts, :max_poll_interval),
          heartbeat_interval: Keyword.fetch!(opts, :heartbeat_interval),
          drain_timeout: drain_timeout,
          task_sup: task_sup
        }

        # Give terminate/2 room to drain: shutdown must outlast drain_timeout, or
        # the supervisor brutal-kills the scheduler mid-drain.
        Supervisor.child_spec({GenDurable.Scheduler, spec},
          id: {GenDurable.Scheduler, queue},
          shutdown: drain_timeout + 1_000
        )
      end

    children =
      [{Task.Supervisor, name: task_sup}] ++
        reaper_child(repo, name, reaper_cfg) ++ gc_child(repo, name, gc_cfg) ++ schedulers

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp reaper_child(repo, name, cfg) do
    case cfg do
      false ->
        Logger.info("gen_durable #{inspect(name)}: reaper disabled on this node")
        []

      cfg ->
        [{GenDurable.Reaper, %{repo: repo, interval: cfg[:interval]}}]
    end
  end

  defp gc_child(repo, name, cfg) do
    case cfg do
      false ->
        Logger.info("gen_durable #{inspect(name)}: gc disabled on this node")
        []

      cfg ->
        [
          {GenDurable.GC,
           %{
             repo: repo,
             interval: cfg[:interval],
             retention_ms: cfg[:retention],
             batch: cfg[:batch]
           }}
        ]
    end
  end

  # A runtime component's option is `false` (do not run it on this node) or a
  # keyword list merged over its defaults. Unknown keys and non-integer values
  # are configuration mistakes — fail the boot, don't run with them ignored.
  defp component_opts(opts, key, defaults) do
    case Keyword.fetch!(opts, key) do
      false ->
        false

      cfg when is_list(cfg) ->
        if not Keyword.keyword?(cfg),
          do: raise(ArgumentError, "#{inspect(key)} must be `false` or a keyword list")

        case Keyword.keys(cfg) -- Keyword.keys(defaults) do
          [] -> :ok
          bad -> raise ArgumentError, "unknown #{inspect(key)} option(s): #{inspect(bad)}"
        end

        for {k, v} <- cfg, not is_integer(v) or v < 0 do
          raise ArgumentError,
                "#{inspect(key)} option #{inspect(k)} must be a non-negative integer, " <>
                  "got: #{inspect(v)}"
        end

        Keyword.merge(defaults, cfg)

      other ->
        raise ArgumentError,
              "#{inspect(key)} must be `false` or a keyword list, got: #{inspect(other)}"
    end
  end

  # `[stripe: [allowed: 100, period: {1, :minute}], …]` → `[%{name, rate, burst}]`.
  # rate = allowed / period_seconds (the sustained throughput); burst defaults to allowed.
  defp parse_rate_limits(rate_limits) do
    for {name, cfg} <- rate_limits do
      allowed = Keyword.fetch!(cfg, :allowed)
      period = period_seconds(Keyword.fetch!(cfg, :period))

      %{
        name: to_string(name),
        rate: allowed / period,
        burst: Keyword.get(cfg, :burst, allowed) * 1.0
      }
    end
  end

  # `[stripe: [limit: 1000, shards: 10]]` → `[%{name, cap, shards}]`. `shards`
  # defaults to 1 and is clamped to the cap (more shards than slots is nonsense).
  defp parse_concurrency_limits(limits) do
    for {name, cfg} <- limits do
      cap = Keyword.fetch!(cfg, :limit)

      if not is_integer(cap) or cap < 1,
        do: raise(ArgumentError, "concurrency limit #{inspect(name)} must be a positive integer")

      %{name: to_string(name), cap: cap, shards: cfg |> Keyword.get(:shards, 1) |> min(cap)}
    end
  end

  defp period_seconds(n) when is_integer(n) and n > 0, do: n
  defp period_seconds({n, unit}) when is_integer(n) and n > 0, do: n * unit_seconds(unit)

  defp unit_seconds(u) when u in [:second, :seconds], do: 1
  defp unit_seconds(u) when u in [:minute, :minutes], do: 60
  defp unit_seconds(u) when u in [:hour, :hours], do: 3600
  defp unit_seconds(u) when u in [:day, :days], do: 86_400
end
