defmodule GenDurable.Scheduler do
  @moduledoc """
  Per-queue feeder + executor loop. Backpressure-driven: it claims work
  from the database into a small in-memory buffer, then spawns at most
  `concurrency` supervised Tasks at a time, draining the buffer as slots free.

  ## Aggressiveness knobs

  Throughput is *not* capped by `poll_interval` — a finished Task immediately
  refills from the buffer (no round-trip) or picks a fresh batch. The poll timer
  only governs how promptly newly-inserted work is *discovered* while idle. The
  knobs let a deployment trade DB chatter, latency, and fairness:

    * `concurrency` — max Tasks running at once (the executor width).
    * `prefetch` — extra rows to claim and hold in the buffer *beyond* the running
      slots. `0` (default) means no over-fetch: identical to picking exactly the
      free slots. Raising it batches picks and absorbs completion bursts, at the
      cost of cross-node fairness and priority freshness (claimed rows are
      invisible to other nodes). Buffered rows are **heartbeated**, so they never
      go stale regardless of depth — depth is decoupled from `lease_ttl`.
    * `min_demand` — batch gate: don't pick unless at least this many slots are
      free (so picks come fat, not one row at a time). Ignored when the queue is
      fully idle, to avoid starvation.
    * `poll_interval` — base idle poll. `max_poll_interval` — backoff ceiling: an
      empty pick on a fully idle queue doubles the interval up to this cap, then a
      non-empty pick (or any in-flight work) snaps back to the base. This is the
      lever that cuts idle DB load. (LISTEN/NOTIFY is banned, so polling is the
      only discovery path — the point is to poll *adaptively*, not constantly.)

  Buffered and in-flight rows are both `executing` + leased; the heartbeat (one
  batched UPDATE per tick over `buffer ++ in_flight`) keeps every claimed row
  alive. If this scheduler dies, the whole claimed set waits one `lease_ttl` for
  the reaper — so deeper buffers mean a larger crash blip, bounded by the (short)
  TTL, not by the buffer depth.

  `concurrency_key` serialization is handled per-job at execution time: a
  session-level advisory lock is taken on a checked-out connection held for the
  whole step, and released after the outcome commits. If the lock is contended,
  the row is returned to `runnable` and skipped.
  """

  use GenServer

  alias GenDurable.{Executor, Queries}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    # Trap exits so a supervisor shutdown runs terminate/2 (graceful drain).
    Process.flag(:trap_exit, true)

    state = %{
      config: opts.config,
      queue: opts.queue,
      concurrency: opts.concurrency,
      prefetch: opts.prefetch,
      min_demand: opts.min_demand,
      worker: opts.worker,
      poll_interval: opts.poll_interval,
      max_poll_interval: opts.max_poll_interval,
      cur_poll: opts.poll_interval,
      heartbeat_interval: opts.heartbeat_interval,
      drain_timeout: opts.drain_timeout,
      task_sup: opts.task_sup,
      buffer: [],
      in_flight: %{}
    }

    schedule(:poll, 0)
    schedule(:heartbeat, state.heartbeat_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    {state, fetched} = fill(state)
    state = adapt(state, fetched)
    saturation(state)
    schedule(:poll, state.cur_poll)
    {:noreply, state}
  end

  def handle_info(:heartbeat, state) do
    ids = buffer_ids(state.buffer) ++ in_flight_ids(state.in_flight)
    Queries.heartbeat(state.config.repo, ids, state.worker, state.config.lease_ttl_ms)
    schedule(:heartbeat, state.heartbeat_interval)
    {:noreply, state}
  end

  # Task finished and returned a value: success path (outcome committed). Refill
  # immediately — draining the buffer is a no-op on the DB; only an empty buffer
  # triggers a pick.
  def handle_info({ref, _result}, state) when is_map_key(state.in_flight, ref) do
    Process.demonitor(ref, [:flush])
    state = update_in(state.in_flight, &Map.delete(&1, ref))
    {state, fetched} = fill(state)
    {:noreply, adapt(state, fetched)}
  end

  # Task crashed before returning: no outcome was written. Leave the row
  # 'executing'; the reaper will recover it. Do NOT call handle/2.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state)
      when is_map_key(state.in_flight, ref) do
    state = update_in(state.in_flight, &Map.delete(&1, ref))
    {state, fetched} = fill(state)
    {:noreply, adapt(state, fetched)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Graceful shutdown: stop claiming, hand the buffered (un-started)
  # rows straight back to `runnable` so they are picked up immediately instead of
  # waiting out the lease, then wait up to `drain_timeout` for in-flight steps to
  # commit their outcomes. The Task.Supervisor is shut down *after* the schedulers
  # (sibling order), so in-flight tasks are still alive to finish here. Anything
  # still running at the deadline is left to the reaper (the lease floor).
  @impl true
  def terminate(_reason, state) do
    buffered = buffer_ids(state.buffer)
    Queries.release(state.config.repo, buffered, state.worker)

    :telemetry.execute(
      [:gen_durable, :scheduler, :drain],
      %{released: length(buffered), in_flight: map_size(state.in_flight)},
      %{queue: state.queue}
    )

    drain(state.in_flight, System.monotonic_time(:millisecond) + state.drain_timeout)
  end

  # Wait for in-flight tasks to report completion (their outcome already committed)
  # or crash, until the set empties or the deadline passes.
  defp drain(in_flight, _deadline) when map_size(in_flight) == 0, do: :ok

  defp drain(in_flight, deadline) do
    timeout = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {ref, _result} when is_map_key(in_flight, ref) ->
        Process.demonitor(ref, [:flush])
        drain(Map.delete(in_flight, ref), deadline)

      {:DOWN, ref, :process, _pid, _reason} when is_map_key(in_flight, ref) ->
        drain(Map.delete(in_flight, ref), deadline)
    after
      timeout -> :ok
    end
  end

  # Pick into the buffer if warranted, then spawn from the buffer into free slots.
  defp fill(state) do
    {state, fetched} = refill(state)
    {drain(state), fetched}
  end

  # Claim up to `demand` rows into the buffer. `demand` is the gap between the
  # claim ceiling (concurrency + prefetch) and what we already hold (running +
  # buffered). Gated by `min_demand` to batch picks, but never when fully idle.
  defp refill(state) do
    claimed = map_size(state.in_flight) + length(state.buffer)
    demand = state.concurrency + state.prefetch - claimed
    idle? = map_size(state.in_flight) == 0 and state.buffer == []

    if demand > 0 and (demand >= state.min_demand or idle?) do
      config = state.config
      jobs = Queries.pick(config.repo, state.queue, demand, state.worker, config.lease_ttl_ms)

      :telemetry.execute(
        [:gen_durable, :pick, :stop],
        %{count: length(jobs), demand: demand},
        %{queue: state.queue, worker: state.worker}
      )

      {%{state | buffer: state.buffer ++ jobs}, length(jobs)}
    else
      {state, 0}
    end
  end

  # Spawn buffered jobs into free executor slots, highest-priority first (the
  # buffer preserves the picker's ORDER BY priority, eligible_at).
  defp drain(%{buffer: [job | rest]} = state)
       when map_size(state.in_flight) < state.concurrency do
    task = Task.Supervisor.async_nolink(state.task_sup, fn -> execute_job(state.config, job) end)

    state = %{
      state
      | buffer: rest,
        in_flight: Map.put(state.in_flight, task.ref, {job.id, job.concurrency_key})
    }

    drain(state)
  end

  defp drain(state), do: state

  # Idle backoff: an empty pick on a fully idle queue stretches the poll interval;
  # any fetched work or any in-flight/buffered work snaps it back to the base.
  defp adapt(state, fetched) do
    cur =
      cond do
        fetched > 0 ->
          state.poll_interval

        map_size(state.in_flight) == 0 and state.buffer == [] ->
          min(state.cur_poll * 2, state.max_poll_interval)

        true ->
          state.poll_interval
      end

    %{state | cur_poll: cur}
  end

  # Run the step, serializing on concurrency_key when present.
  defp execute_job(config, %{concurrency_key: nil} = job), do: Executor.run(config, job)

  defp execute_job(config, %{concurrency_key: key} = job) do
    repo = config.repo

    repo.checkout(fn ->
      if Queries.advisory_try_lock(repo, key) do
        try do
          Executor.run(config, job)
        after
          Queries.advisory_unlock(repo, key)
        end
      else
        # Another worker holds this key; hand the row back. With the picker's
        # concurrency_key dedup this should be rare (only a cross-node or unlock-gap
        # race), so it is worth a telemetry signal.
        :telemetry.execute(
          [:gen_durable, :concurrency, :contended],
          %{count: 1},
          %{id: job.id, fsm: job.fsm, concurrency_key: key}
        )

        Queries.reset_to_runnable(repo, job.id, job.worker)
        :skipped
      end
    end)
  end

  # Gauge of how loaded this queue is — emitted each poll so a handler can read
  # in-flight depth, buffer depth, and free slots to tune the feeder knobs.
  defp saturation(state) do
    :telemetry.execute(
      [:gen_durable, :scheduler, :saturation],
      %{
        in_flight: map_size(state.in_flight),
        buffer: length(state.buffer),
        concurrency: state.concurrency,
        prefetch: state.prefetch
      },
      %{queue: state.queue}
    )
  end

  defp buffer_ids(buffer), do: Enum.map(buffer, & &1.id)
  defp in_flight_ids(in_flight), do: in_flight |> Map.values() |> Enum.map(&elem(&1, 0))

  defp schedule(msg, after_ms), do: Process.send_after(self(), msg, after_ms)
end
