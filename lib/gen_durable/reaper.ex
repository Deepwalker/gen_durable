defmodule GenDurable.Reaper do
  @moduledoc """
  Periodically returns `executing` rows with an expired lease to `runnable`,
  bumping `attempt` (spec §4.3 / §10). This is the at-least-once safety floor for
  worker crashes; `handle/2` is intentionally not involved.
  """

  use GenServer

  alias GenDurable.Queries

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{repo: opts.repo, interval: opts.interval}
    schedule(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:reap, state) do
    reaped = Queries.reap(state.repo)

    if reaped != [] do
      :telemetry.execute([:gen_durable, :reaper, :reaped], %{count: length(reaped)}, %{
        ids: reaped
      })
    end

    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :reap, interval)
end
