defmodule GenDurable.GC do
  @moduledoc """
  Periodically deletes terminal (`done`/`failed`) instances older than the
  retention window — the engine's built-in pruner.

  Each sweep removes at most `batch` rows (`Queries.gc/3`); if it fills the
  batch, more remain, so it re-sweeps immediately to drain a backlog rather than
  waiting a full interval. A terminal child whose parent is still mid-join is
  spared — see `Queries.gc/3`.

  Configured per node via the engine's `:gc` option (`[interval:, retention:,
  batch:]`, or `false` to not run GC on this node — at least one node in the
  cluster must, or terminal rows and stale limiter counters accumulate forever).
  """

  use GenServer

  alias GenDurable.Queries

  # Unnamed: nothing calls it, and a global name would forbid a second engine
  # instance (each instance runs its own GC under its own supervisor).
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      repo: opts.repo,
      limiter: opts.limiter,
      interval: opts.interval,
      retention_ms: opts.retention_ms,
      batch: opts.batch
    }

    schedule(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:gc, state) do
    swept = Queries.gc(state.repo, state.retention_ms, state.batch)
    # The limiter self-heals its own counters (PG: sweep stale rate buckets + reconcile
    # concurrency gates from the executing-rows truth). Stats ride into the telemetry.
    %{buckets: buckets, gates: gates} = GenDurable.Limiter.reconcile(state.limiter)

    if swept > 0 or buckets > 0 or gates > 0 do
      :telemetry.execute(
        [:gen_durable, :gc, :swept],
        %{count: swept, buckets: buckets, gates: gates},
        %{}
      )
    end

    # Filled the batch ⇒ a backlog likely remains ⇒ sweep again at once.
    schedule(if swept == state.batch, do: 0, else: state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :gc, interval)
end
