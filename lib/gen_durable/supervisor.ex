defmodule GenDurable.Supervisor do
  @moduledoc """
  Top-level engine supervisor. Started by the host (Oban-style) as
  `{GenDurable, opts}` in their own supervision tree.

  ## Options

    * `:repo` — the host's `Ecto.Repo` (required).
    * `:fsms` — FSM modules to register explicitly (default `[]`). Only needed for a
      custom `:name` or to keep an old `:version` running; otherwise a machine is
      resolved from the `fsm` column (its module name). See `GenDurable.Registry`.
    * `:queues` — keyword list of `queue_name => concurrency`
      (default `[default: 10]`).
    * `:lease_ttl`, `:heartbeat_interval`, `:poll_interval`, `:reap_interval` —
      timings in ms (Balanced defaults: 60_000 / 20_000 / 1_000 / 30_000).
    * `:prefetch` — extra rows each queue claims and buffers beyond its running
      slots (default `0` ⇒ no over-fetch). See `GenDurable.Scheduler`.
    * `:min_demand` — batch gate for the picker (default `1`).
    * `:max_poll_interval` — idle-backoff ceiling in ms (default `5_000`).
    * `:drain_timeout` — on shutdown, how long (ms) each queue waits for its
      in-flight steps to finish before giving up to the reaper (default `5_000`).
      Buffered (un-started) rows are released immediately regardless.

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
    prefetch: 0,
    min_demand: 1,
    max_poll_interval: 5_000,
    drain_timeout: 5_000
  ]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    opts = Keyword.merge(@defaults, opts)
    repo = Keyword.fetch!(opts, :repo)

    config = %{repo: repo, lease_ttl_ms: Keyword.fetch!(opts, :lease_ttl)}
    :persistent_term.put({GenDurable, :config}, config)

    task_sup = GenDurable.TaskSupervisor

    schedulers =
      for {name, concurrency} <- Keyword.fetch!(opts, :queues) do
        queue = to_string(name)

        drain_timeout = Keyword.fetch!(opts, :drain_timeout)

        spec = %{
          config: config,
          queue: queue,
          concurrency: concurrency,
          prefetch: Keyword.fetch!(opts, :prefetch),
          min_demand: Keyword.fetch!(opts, :min_demand),
          worker: worker_id(queue),
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
        {GenDurable.Registry, fsms: Keyword.fetch!(opts, :fsms)},
        {Task.Supervisor, name: task_sup},
        {GenDurable.Reaper, %{repo: repo, interval: Keyword.fetch!(opts, :reap_interval)}}
      ] ++ schedulers

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp worker_id(queue), do: "#{queue}@#{node()}-#{System.unique_integer([:positive])}"
end
