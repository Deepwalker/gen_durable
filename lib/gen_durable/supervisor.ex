defmodule GenDurable.Supervisor do
  @moduledoc """
  Top-level engine supervisor. Started by the host (Oban-style) as
  `{GenDurable, opts}` in their own supervision tree.

  ## Options

    * `:repo` — the host's `Ecto.Repo` (required).
    * `:fsms` — FSM modules to register (default `[]`).
    * `:queues` — keyword list of `queue_name => concurrency`
      (default `[default: 10]`).
    * `:lease_ttl`, `:heartbeat_interval`, `:poll_interval`, `:reap_interval` —
      timings in ms (Balanced defaults: 60_000 / 20_000 / 1_000 / 30_000).
    * `:prefetch` — extra rows each queue claims and buffers beyond its running
      slots (default `0` ⇒ no over-fetch). See `GenDurable.Scheduler`.
    * `:min_demand` — batch gate for the picker (default `1`).
    * `:max_poll_interval` — idle-backoff ceiling in ms (default `5_000`).

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
    max_poll_interval: 5_000
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
          task_sup: task_sup
        }

        Supervisor.child_spec({GenDurable.Scheduler, spec}, id: {GenDurable.Scheduler, queue})
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
