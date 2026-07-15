defmodule GenDurable.Flusher do
  @moduledoc """
  Group-commit coordinator for step outcomes.

  Instead of every worker Task committing its own outcome (one `complete_*`
  round-trip per instance per step), a Task hands its outcome here and **blocks
  until it is durably written** — `commit_before_proceed` is preserved; the Task
  sits and waits for the write, it does not run ahead of it. The flusher
  coalesces the outcomes of all the Tasks waiting on it into **one transaction**
  (`GenDurable.Queries.flush/2`: a single guarded `UPDATE … FROM unnest(...)`
  plus the consume/notify riders) and releases every waiter when it commits.

  Two triggers, whichever fires first:

    * `max_batch` (default 100) — flush as soon as this many outcomes are buffered;
    * `max_delay_ms` (default 100) — flush this long after the first buffered
      outcome, bounding the durability/latency window under light load.

  Under load the batch auto-grows: while a flush transaction runs, the flusher's
  mailbox fills, so the next flush is bigger — the single serialization point
  does not become a linear bottleneck. Step *execution* stays fully parallel
  across Tasks; only the commit is coalesced.

  A running engine wires one flusher per `flushers:` spec (see
  `GenDurable.Supervisor`), each owning a disjoint set of queues; the
  `Testing.drain/1` path flushes synchronously without one.
  """
  use GenServer

  alias GenDurable.{Await, Limiter, Poke, Queries}

  @typedoc """
  One buffered outcome. The `set_*` flags + precomputed values drive the batched
  `UPDATE` (`Queries.flush/2`); `slot`/`notify` drive the post-flush side effects.
  """
  @type entry :: %{
          required(:id) => integer(),
          required(:worker) => String.t(),
          required(:slot) => term() | nil,
          required(:notify) => boolean(),
          required(:status) => String.t(),
          required(:attempt) => integer(),
          required(:delay_ms) => integer(),
          required(:set_step) => boolean(),
          required(:step) => String.t() | nil,
          required(:set_state) => boolean(),
          required(:state) => String.t() | nil,
          required(:set_result) => boolean(),
          required(:result) => String.t() | nil,
          required(:set_error) => boolean(),
          required(:error) => String.t() | nil,
          required(:clear_awaits) => boolean(),
          required(:set_rate) => boolean(),
          required(:rate_limit) => String.t() | nil,
          required(:weight) => float(),
          required(:set_ck) => boolean(),
          required(:ck_value) => String.t() | nil,
          required(:consumed_ids) => [integer()]
        }

  # A blocked Task should wait as long as a flush can reasonably take; the lease
  # (renewed by the scheduler's heartbeat while the Task waits) is the real floor,
  # so this only guards against a wedged flusher.
  @commit_timeout 30_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Submit `entry` and block until the batch containing it commits. Returns
  `:committed` (its state landed), `:stale` (the ownership guard failed — the row
  was reclaimed, a new claimant redoes the step), or `{:error, reason}` (the flush
  transaction failed; the row stays `executing` for the reaper).
  """
  @spec commit(GenServer.server(), entry()) :: :committed | :stale | {:error, term()}
  def commit(server, entry), do: GenServer.call(server, {:commit, entry}, @commit_timeout)

  @doc """
  Flush `entries` synchronously in the calling process — no coordinator, no
  batching — and run the side effects. The `Testing.drain/1` path uses this
  (batch of one per step); returns the MapSet of committed ids.
  """
  @spec commit_sync(map(), [entry()]) :: MapSet.t()
  def commit_sync(config, entries) do
    %{committed: committed, woken_queues: woken} = Queries.flush(config.repo, entries)
    committed = MapSet.new(committed)
    side_effects(config, entries, committed, woken)
    committed
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       config: Keyword.fetch!(opts, :config),
       max_batch: Keyword.get(opts, :max_batch, 100),
       max_delay_ms: Keyword.get(opts, :max_delay_ms, 100),
       # {entry, from} newest-first; reversed to oldest-first at flush time
       buffer: [],
       count: 0,
       timer: nil
     }}
  end

  @impl true
  def handle_call({:commit, entry}, from, state) do
    state = %{state | buffer: [{entry, from} | state.buffer], count: state.count + 1}

    cond do
      state.count >= state.max_batch ->
        {:noreply, flush(state)}

      state.timer == nil ->
        {:noreply, %{state | timer: Process.send_after(self(), :flush_deadline, state.max_delay_ms)}}

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:flush_deadline, state), do: {:noreply, flush(state)}

  defp flush(%{count: 0} = state), do: state

  defp flush(state) do
    if state.timer, do: Process.cancel_timer(state.timer)

    pairs = Enum.reverse(state.buffer)
    entries = Enum.map(pairs, fn {e, _from} -> e end)

    try do
      %{committed: committed, woken_queues: woken} = Queries.flush(state.config.repo, entries)
      committed = MapSet.new(committed)

      for {e, from} <- pairs do
        GenServer.reply(from, if(MapSet.member?(committed, e.id), do: :committed, else: :stale))
      end

      side_effects(state.config, entries, committed, woken)
    rescue
      e ->
        for {_e, from} <- pairs, do: GenServer.reply(from, {:error, e})

        :telemetry.execute(
          [:gen_durable, :flush, :error],
          %{count: length(pairs)},
          %{error: e}
        )
    end

    %{state | buffer: [], count: 0, timer: nil}
  end

  # Every side effect that used to run per-Task after `complete_*`, now batched and
  # resolved in one place: credit all freed concurrency slots in a single limiter
  # call, nudge local awaiters of settled instances, poke queues whose parent-join
  # a terminal completion just satisfied.
  defp side_effects(config, entries, committed, woken_queues) do
    committed_entries = Enum.filter(entries, &MapSet.member?(committed, &1.id))

    slots = for e <- committed_entries, not is_nil(e.slot), do: e.slot
    Limiter.credit(config.limiter, slots)

    for e <- committed_entries, e.notify, do: Await.notify_local(config.name, e.id)

    # A fan-out's freshly-inserted children may live in other queues — poke them.
    for e <- committed_entries, Map.get(e, :kind) == :schedule_childs do
      Poke.dispatch_rows(config.name, e.children)
    end

    for q <- Enum.uniq(woken_queues), do: Poke.dispatch(config.name, q)

    :ok
  end
end
