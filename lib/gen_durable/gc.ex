defmodule GenDurable.GC do
  @moduledoc """
  Periodically deletes terminal (`done`/`failed`) instances older than the
  retention window — the engine's built-in pruner.

  Each sweep removes at most `:gc_batch` rows (`Queries.gc/3`); if it fills the
  batch, more remain, so it re-sweeps immediately to drain a backlog rather than
  waiting a full interval. A terminal child whose parent is still mid-join is
  spared — see `Queries.gc/3`.

  Disable by passing `gc_interval: nil` to the engine; the GC process is then not
  started and terminal rows accumulate until pruned externally.
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
    buckets = Queries.gc_buckets(state.repo)

    if swept > 0 or buckets > 0 do
      :telemetry.execute([:gen_durable, :gc, :swept], %{count: swept, buckets: buckets}, %{})
    end

    # Filled the batch ⇒ a backlog likely remains ⇒ sweep again at once.
    schedule(if swept == state.batch, do: 0, else: state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :gc, interval)
end
