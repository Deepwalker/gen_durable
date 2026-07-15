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
    * `:poke` — how an insert announces new work to schedulers (see
      `GenDurable.Poke`): `:local` (default) — the caller's node only;
      `:cluster` — every node, over Erlang distribution; `{:redis, url_or_opts}`
      — Redis Pub/Sub, for clusters without distribution (requires the optional
      `:redix` dependency; the value is passed to `Redix.start_link/1`).
      Best-effort in every mode — the poll interval is the discovery floor.
    * `:await` — tuning for `GenDurable.await/3`: `[tick: 25]` (ms between the
      batched watcher's probes — the latency granularity for results committed
      on OTHER nodes; same-node results are pushed instantly). Idle-free, so
      there is no `false`.

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
      `[stripe: [allowed: 100, period: {1, :minute}], emails: [allowed: 5, period: 60, burst: 10, shards: 4]]`.
      `allowed`/`period` set the sustained rate; `burst` (default `allowed`) the capacity.
      `shards` (default 1) splits rate/burst across bucket rows so concurrent
      (cross-node) pickers grab disjoint shards instead of serializing — size it to
      the number of nodes that contend the hottest key. A step opts into a limit by
      returning `rate_limit: :stripe` (or `{:stripe, key}`).
    * `:concurrency_limits` — named concurrency caps for `concurrency_key` (default
      `[]`), e.g. `[stripe: [limit: 1000, shards: 10]]`: at most `limit` concurrent
      executions per key whose name matches (`concurrency_key: {:stripe, tenant}` ⇒
      key `"stripe:tenant"`, name `"stripe"`). An unconfigured key keeps the default
      limit of 1 (mutual exclusion). `shards` (default 1, clamped to `limit`) splits
      the cap across bucket rows so cross-node pickers take disjoint shards and
      completions credit back per shard — size `shards ≥ contending nodes`.
      **Careful**: a config name
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
    poke: :local,
    await: [],
    prefetch: 0,
    min_demand: 1,
    max_poll_interval: 5_000,
    drain_timeout: 5_000,
    rate_limits: [],
    concurrency_limits: [],
    flushers: [%{queues: :all}]
  ]

  @reaper_defaults [interval: 30_000]
  @gc_defaults [interval: 60_000, retention: 86_400_000, batch: 10_000]
  @await_defaults [tick: 25]

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
    poke = validate_poke!(Keyword.fetch!(opts, :poke))

    await_cfg =
      case component_opts(opts, :await, @await_defaults) do
        # await is idle-free (no waiters -> no timer, no queries), so there is
        # nothing to disable
        false ->
          raise ArgumentError, ":await cannot be false — pass [tick: ms] to tune it"

        cfg ->
          # tick: 0 would be a hot loop (one probe per scheduler pass)
          if cfg[:tick] < 1,
            do: raise(ArgumentError, ":await tick must be at least 1 ms, got: #{cfg[:tick]}")

          cfg
      end

    rate_configs = parse_rate_limits(Keyword.fetch!(opts, :rate_limits))
    conc_configs = parse_concurrency_limits(Keyword.fetch!(opts, :concurrency_limits))

    # The out-of-band admission backend: :postgres (default — the sharded buckets table) or
    # {:redis, url_or_opts} (a lease-scored ZSET semaphore + token buckets). Redis needs its
    # own connection, returned here to slot into the tree before the schedulers pick.
    {limiter, limiter_children} =
      build_limiter(
        Keyword.get(opts, :limiter, :postgres),
        repo,
        name,
        Keyword.fetch!(opts, :lease_ttl)
      )

    config = %{
      name: name,
      repo: repo,
      limiter: limiter,
      # The FSM registry table is owned by this supervisor process — it lives
      # and dies with the instance, and executors reach it through the config.
      registry: GenDurable.Registry.new(Keyword.fetch!(opts, :fsms)),
      lease_ttl_ms: Keyword.fetch!(opts, :lease_ttl),
      rate_limit_names: MapSet.new(rate_configs, & &1.name),
      # Which concurrency-key names are CONFIGURED (have a `conc` bucket). Lets the
      # executor route an inline step's new concurrency_key: a configured name is
      # admitted out-of-band via the limiter, an unconfigured one rides the in-band
      # K=1 arbiter. See `GenDurable.Executor`.
      concurrency_limit_names: MapSet.new(conc_configs, & &1.name),
      poke: poke,
      # origin tag for cross-node pokes: lets a subscriber drop messages this
      # VM published (its schedulers were already poked directly)
      poke_token: GenDurable.Scheduler.vm_id()
    }

    # Keyed by the instance name, so engines coexist. Not erased on shutdown (a
    # Supervisor has no terminate hook); a stale entry is harmless — API writes
    # against a stopped engine are legal, the rows wait in the database.
    :persistent_term.put({GenDurable, name}, config)

    # Seed the policy so the limiter can admit against it. No-ops when empty.
    :ok =
      GenDurable.Limiter.sync_config(
        limiter,
        Enum.map(rate_configs, &{:rate, &1.name, &1.rate, &1.capacity, &1.shards}) ++
          Enum.map(conc_configs, &{:conc, &1.name, &1.capacity, &1.shards})
      )

    task_sup = Module.concat(name, TaskSupervisor)

    # Group-commit coordinators. Each `flushers:` spec becomes one Flusher owning
    # the queues that route to it (first-match-wins); a queue's scheduler stamps
    # its flusher on every dispatched job so the worker Task commits through it.
    flusher_specs = parse_flushers(Keyword.fetch!(opts, :flushers))
    validate_flusher_coverage!(flusher_specs, Keyword.fetch!(opts, :queues))

    flusher_children =
      for {spec, i} <- Enum.with_index(flusher_specs) do
        Supervisor.child_spec(
          {GenDurable.Flusher,
           [
             name: flusher_name(name, i),
             config: config,
             max_batch: spec.max_batch,
             max_delay_ms: spec.max_delay_ms
           ]},
          id: {GenDurable.Flusher, i}
        )
      end

    schedulers =
      for {queue_name, concurrency} <- Keyword.fetch!(opts, :queues) do
        queue = to_string(queue_name)

        drain_timeout = Keyword.fetch!(opts, :drain_timeout)

        spec = %{
          config: config,
          scope: GenDurable.Poke.scope(name),
          queue: queue,
          concurrency: concurrency,
          prefetch: Keyword.fetch!(opts, :prefetch),
          min_demand: Keyword.fetch!(opts, :min_demand),
          poll_interval: Keyword.fetch!(opts, :poll_interval),
          max_poll_interval: Keyword.fetch!(opts, :max_poll_interval),
          heartbeat_interval: Keyword.fetch!(opts, :heartbeat_interval),
          drain_timeout: drain_timeout,
          task_sup: task_sup,
          flusher: flusher_name(name, resolve_flusher_index(flusher_specs, queue))
        }

        # Give terminate/2 room to drain: shutdown must outlast drain_timeout, or
        # the supervisor brutal-kills the scheduler mid-drain.
        Supervisor.child_spec({GenDurable.Scheduler, spec},
          id: {GenDurable.Scheduler, queue},
          shutdown: drain_timeout + 1_000
        )
      end

    children =
      [
        # The instance's :pg scope (schedulers join their queue's group) — how
        # a poke finds the schedulers to nudge, locally or across the cluster.
        # Started even with no queues so the poke path is uniform: an empty
        # group = nobody to poke.
        %{id: {:pg, name}, start: {:pg, :start_link, [GenDurable.Poke.scope(name)]}},
        # await machinery: the Watcher owns both the executor-facing waiter
        # table (same-node push nudges) and the batched cross-node poller.
        # Idle-free; started even with no queues — await is called wherever
        # inserts happen.
        %{
          id: GenDurable.Await.Watcher,
          start:
            {GenDurable.Await.Watcher, :start_link,
             [
               %{
                 name: GenDurable.Await.watcher(name),
                 table: GenDurable.Await.table(name),
                 repo: repo,
                 tick: await_cfg[:tick]
               }
             ]}
        },
        {Task.Supervisor, name: task_sup}
      ] ++
        limiter_children ++
        poke_children(name, poke) ++
        reaper_child(repo, name, reaper_cfg) ++
        gc_child(repo, limiter, name, gc_cfg) ++ flusher_children ++ schedulers

    Supervisor.init(children, strategy: :one_for_one)
  end

  # The {:redis, _} transport needs two processes: a publisher connection (the
  # insert side) and the Pub/Sub listener (the subscriber side). :local and
  # :cluster need nothing beyond the :pg scope.
  defp poke_children(name, {:redis, redis}) do
    publisher = GenDurable.Poke.publisher(name)

    publisher_arg =
      case redis do
        url when is_binary(url) -> {url, [name: publisher]}
        opts when is_list(opts) -> Keyword.put(opts, :name, publisher)
      end

    [
      Supervisor.child_spec({Redix, publisher_arg}, id: {Redix, publisher}),
      %{
        id: GenDurable.Poke.Listener,
        start:
          {GenDurable.Poke.Listener, :start_link,
           [%{name: name, redis: redis, token: GenDurable.Scheduler.vm_id()}]}
      }
    ]
  end

  defp poke_children(_name, _mode), do: []

  # `flushers:` — a list of specs, each a group-commit coordinator for the queues
  # that route to it. A queue picks the FIRST spec whose `queues` matches it
  # (`:all` matches everything), so specific selectors go before an `:all` catch-all.
  # Per-spec `max_batch`/`max_delay_ms` tune that flusher. Default: one `:all` flusher.
  defp parse_flushers(specs) when is_list(specs) and specs != [] do
    Enum.map(specs, fn spec ->
      %{
        queues: normalize_flusher_queues(Map.get(spec, :queues, :all)),
        max_batch: Map.get(spec, :max_batch, 100),
        max_delay_ms: Map.get(spec, :max_delay_ms, 100)
      }
    end)
  end

  defp parse_flushers(other),
    do: raise(ArgumentError, ":flushers must be a non-empty list of specs, got: #{inspect(other)}")

  defp normalize_flusher_queues(:all), do: :all
  defp normalize_flusher_queues(list) when is_list(list), do: Enum.map(list, &to_string/1)
  defp normalize_flusher_queues(one), do: [to_string(one)]

  defp flusher_matches?(:all, _queue), do: true
  defp flusher_matches?(list, queue), do: queue in list

  defp resolve_flusher_index(specs, queue),
    do: Enum.find_index(specs, &flusher_matches?(&1.queues, queue))

  defp flusher_name(name, i), do: Module.concat(name, "Flusher#{i}")

  defp validate_flusher_coverage!(specs, queues) do
    for {queue_name, _concurrency} <- queues do
      queue = to_string(queue_name)

      unless resolve_flusher_index(specs, queue) do
        raise ArgumentError,
              "no :flushers spec matches queue #{inspect(queue)} — " <>
                "add a %{queues: :all} spec (last) to cover it"
      end
    end
  end

  # Build the limiter `{module, handle}` and any children it needs. Postgres needs none;
  # Redis needs a dedicated (sync-connecting) Redix connection, ordered before the schedulers.
  defp build_limiter(:postgres, repo, _name, _lease_ttl),
    do: {{GenDurable.Limiter.Postgres, %{repo: repo}}, []}

  defp build_limiter({:redis, redis}, _repo, name, lease_ttl) do
    validate_limiter_redis!(redis)
    conn = Module.concat(name, LimiterRedis)
    handle = %{conn: conn, lease_ttl_ms: lease_ttl, cfg_key: {GenDurable, name, :limiter_cfg}}
    {{GenDurable.Limiter.Redis, handle}, [limiter_redix_child(redis, conn)]}
  end

  defp build_limiter(other, _repo, _name, _lease_ttl) do
    raise ArgumentError,
          ":limiter must be :postgres or {:redis, url_or_opts}, got: #{inspect(other)}"
  end

  defp limiter_redix_child(redis, conn) do
    arg =
      case redis do
        url when is_binary(url) -> {url, [name: conn, sync_connect: true]}
        opts when is_list(opts) -> Keyword.merge(opts, name: conn, sync_connect: true)
      end

    Supervisor.child_spec({Redix, arg}, id: {Redix, conn})
  end

  defp validate_limiter_redis!(redis) when is_binary(redis) or is_list(redis) do
    unless Code.ensure_loaded?(Redix) do
      raise ArgumentError,
            "limiter: {:redis, _} requires the optional :redix dependency — " <>
              "add {:redix, \"~> 1.2\"} to your deps"
    end
  end

  defp validate_limiter_redis!(other) do
    raise ArgumentError, "limiter {:redis, _} needs a url or opts, got: #{inspect(other)}"
  end

  defp validate_poke!(mode) when mode in [:local, :cluster], do: mode

  defp validate_poke!({:redis, redis} = mode) when is_binary(redis) or is_list(redis) do
    if Code.ensure_loaded?(Redix) do
      mode
    else
      raise ArgumentError,
            "poke: {:redis, _} requires the optional :redix dependency — " <>
              "add {:redix, \"~> 1.2\"} to your deps"
    end
  end

  defp validate_poke!(other) do
    raise ArgumentError,
          ":poke must be :local, :cluster, or {:redis, url_or_opts}, got: #{inspect(other)}"
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

  defp gc_child(repo, limiter, name, cfg) do
    case cfg do
      false ->
        Logger.info("gen_durable #{inspect(name)}: gc disabled on this node")
        []

      cfg ->
        [
          {GenDurable.GC,
           %{
             repo: repo,
             limiter: limiter,
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

  # `[stripe: [allowed: 100, period: {1, :minute}, shards: 4], …]` →
  # `[%{name, rate, capacity, shards}]`. rate = allowed / period_seconds (the
  # sustained throughput); capacity (burst) defaults to allowed; shards defaults
  # to 1 — opt-in cross-node pick parallelism, splitting rate/burst evenly.
  defp parse_rate_limits(rate_limits) do
    for {name, cfg} <- rate_limits do
      allowed = Keyword.fetch!(cfg, :allowed)
      period = period_seconds(Keyword.fetch!(cfg, :period))

      %{
        name: to_string(name),
        rate: allowed / period,
        capacity: Keyword.get(cfg, :burst, allowed) * 1.0,
        shards: cfg |> Keyword.get(:shards, 1) |> max(1)
      }
    end
  end

  # `[stripe: [limit: 1000, shards: 10]]` → `[%{name, capacity, shards}]`.
  # `shards` defaults to 1, is clamped to the cap (more shards than slots is
  # nonsense), and sets the cross-node pick parallelism for the gate.
  defp parse_concurrency_limits(limits) do
    for {name, cfg} <- limits do
      cap = Keyword.fetch!(cfg, :limit)

      if not is_integer(cap) or cap < 1,
        do: raise(ArgumentError, "concurrency limit #{inspect(name)} must be a positive integer")

      %{
        name: to_string(name),
        capacity: cap,
        shards: cfg |> Keyword.get(:shards, 1) |> max(1) |> min(cap)
      }
    end
  end

  defp period_seconds(n) when is_integer(n) and n > 0, do: n
  defp period_seconds({n, unit}) when is_integer(n) and n > 0, do: n * unit_seconds(unit)

  defp unit_seconds(u) when u in [:second, :seconds], do: 1
  defp unit_seconds(u) when u in [:minute, :minutes], do: 60
  defp unit_seconds(u) when u in [:hour, :hours], do: 3600
  defp unit_seconds(u) when u in [:day, :days], do: 86_400
end
