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
      (default `[default: 10]`).
    * `:lease_ttl`, `:heartbeat_interval`, `:poll_interval`, `:reap_interval` —
      timings in ms (Balanced defaults: 60_000 / 20_000 / 1_000 / 30_000).
    * `:gc_interval` — ms between GC sweeps of terminal rows (default `60_000`).
      Set to `nil` to disable GC entirely (terminal rows then accumulate).
    * `:gc_retention` — ms a `done`/`failed` row is kept after it terminates before
      GC may delete it (default `86_400_000`, i.e. 1 day).
    * `:gc_batch` — max rows deleted per GC sweep (default `10_000`).
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

  `:prefetch`, `:min_demand`, and `:max_poll_interval` are the feeder
  aggressiveness knobs and apply to every queue; see `GenDurable.Scheduler` for
  the trade-offs (DB chatter vs. latency vs. cross-node fairness).
  """

  use Supervisor

  @defaults [
    fsms: [],
    queues: [default: 10],
    lease_ttl: 60_000,
    heartbeat_interval: 20_000,
    poll_interval: 1_000,
    reap_interval: 30_000,
    gc_interval: 60_000,
    gc_retention: 86_400_000,
    gc_batch: 10_000,
    prefetch: 0,
    min_demand: 1,
    max_poll_interval: 5_000,
    drain_timeout: 5_000,
    rate_limits: []
  ]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, GenDurable))
  end

  @impl true
  def init(opts) do
    opts = Keyword.merge(@defaults, opts)
    repo = Keyword.fetch!(opts, :repo)
    name = Keyword.get(opts, :name, GenDurable)

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

    # Seed the rate-limit policy table so the picker can join it. No-op when empty.
    :ok = GenDurable.Queries.upsert_rate_configs(repo, rate_configs)

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
      [
        {Task.Supervisor, name: task_sup},
        {GenDurable.Reaper, %{repo: repo, interval: Keyword.fetch!(opts, :reap_interval)}}
      ] ++ gc_child(repo, opts) ++ schedulers

    Supervisor.init(children, strategy: :one_for_one)
  end

  # GC is optional: `gc_interval: nil` omits the process entirely.
  defp gc_child(repo, opts) do
    case Keyword.fetch!(opts, :gc_interval) do
      nil ->
        []

      interval ->
        [
          {GenDurable.GC,
           %{
             repo: repo,
             interval: interval,
             retention_ms: Keyword.fetch!(opts, :gc_retention),
             batch: Keyword.fetch!(opts, :gc_batch)
           }}
        ]
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

  defp period_seconds(n) when is_integer(n) and n > 0, do: n
  defp period_seconds({n, unit}) when is_integer(n) and n > 0, do: n * unit_seconds(unit)

  defp unit_seconds(u) when u in [:second, :seconds], do: 1
  defp unit_seconds(u) when u in [:minute, :minutes], do: 60
  defp unit_seconds(u) when u in [:hour, :hours], do: 3600
  defp unit_seconds(u) when u in [:day, :days], do: 86_400
end
