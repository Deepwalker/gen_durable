defmodule GenDurable.InsertBatcher do
  @moduledoc """
  In-memory insert batcher — the fire-and-forget, lossy counterpart to the outcome
  group-commit `GenDurable.Flusher`.

  `GenDurable.insert_async/2` buffers a row's params here and returns `:ok`
  **immediately**, without waiting for the write. The batcher coalesces buffered rows
  into one `GenDurable.Queries.insert_all/2` on a size/time trigger and then pokes the
  affected queues (out of band). The **opposite durability contract** to the Flusher:
  the caller does not block on the write, so a row still buffered when the VM dies
  abruptly is **lost**. For throughput workloads where an occasional dropped insert is
  acceptable; at-least-once work uses the synchronous `insert/2`.

  Two triggers, whichever fires first (same shape as the Flusher):

    * `max_batch` (default 1000) — flush as soon as this many rows are buffered;
    * `max_delay_ms` (default 100) — flush this long after the first buffered row,
      bounding the loss/latency window under light load.

  Under load the batch auto-grows: while a flush runs, the mailbox fills, so the next
  flush is bigger — the single serialization point does not become a linear bottleneck.

  **Backpressure.** The pending depth is an `:atomics` counter, bumped by `insert_async`
  before it casts and decremented here per flush. When the depth reaches `max_buffer`,
  `insert_async` falls back to a synchronous, durable `insert` instead of buffering — so
  an overwhelmed batcher degrades to durable writes rather than growing without bound.

  On graceful shutdown the batcher **drains** its buffer (a final `insert_all`), so a
  clean stop loses nothing; the loss window is only an abrupt VM death with rows still
  buffered.
  """
  use GenServer

  alias GenDurable.{Poke, Queries}

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @doc "Buffer one row's params (async). The atomics depth was already bumped by the caller."
  def enqueue(server, params), do: GenServer.cast(server, {:insert, params})

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 runs on supervisor shutdown and drains the buffer.
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       config: Keyword.fetch!(opts, :config),
       ref: Keyword.fetch!(opts, :ref),
       max_batch: Keyword.fetch!(opts, :max_batch),
       max_delay_ms: Keyword.fetch!(opts, :max_delay_ms),
       # params newest-first; reversed to oldest-first (insertion order) at flush
       buffer: [],
       count: 0,
       timer: nil
     }}
  end

  @impl true
  def handle_cast({:insert, params}, state) do
    state = %{state | buffer: [params | state.buffer], count: state.count + 1}

    cond do
      state.count >= state.max_batch ->
        {:noreply, flush(state)}

      state.timer == nil ->
        {:noreply,
         %{state | timer: Process.send_after(self(), :flush_deadline, state.max_delay_ms)}}

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:flush_deadline, state), do: {:noreply, flush(state)}

  @impl true
  def terminate(_reason, state) do
    _ = flush(state)
    :ok
  end

  defp flush(%{count: 0} = state), do: state

  defp flush(state) do
    if state.timer, do: Process.cancel_timer(state.timer)

    rows = Enum.reverse(state.buffer)
    n = state.count

    try do
      _ids = Queries.insert_all(state.config.repo, rows)
      Poke.dispatch_rows(state.config.name, rows)
    rescue
      e ->
        # Lossy by contract: a failed batch is dropped (not retried), made observable.
        :telemetry.execute([:gen_durable, :insert_batch, :error], %{count: n}, %{error: e})
    after
      # Release the backpressure depth whether the batch landed or was dropped.
      :atomics.sub(state.ref, 1, n)
    end

    %{state | buffer: [], count: 0, timer: nil}
  end
end
