defmodule GenDurable.Reaper do
  @moduledoc """
  Periodically returns `executing` rows with an expired lease to `runnable`,
  bumping `attempt`. This is the at-least-once safety floor for
  worker crashes; `handle/2` is intentionally not involved.

  The same tick also fires await timeouts: parked rows whose `await_deadline`
  passed go back to `runnable` (a wake, not a failure — `attempt` untouched),
  so timeout resolution is bounded by `:reap_interval`.
  """

  use GenServer

  alias GenDurable.Queries

  # Unnamed: nothing calls it, and a global name would forbid a second engine
  # instance (each instance runs its own reaper under its own supervisor).
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{repo: opts.repo, interval: opts.interval}
    schedule(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:reap, state) do
    reaped = Queries.reap(state.repo)

    # count only — after a mass crash the id list can be huge, and telemetry
    # metadata should stay cheap to carry.
    if reaped != [] do
      :telemetry.execute([:gen_durable, :reaper, :reaped], %{count: length(reaped)}, %{})
    end

    expired = Queries.expire_awaits(state.repo)

    if expired > 0 do
      :telemetry.execute([:gen_durable, :await, :timeout], %{count: expired}, %{})
    end

    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :reap, interval)
end
