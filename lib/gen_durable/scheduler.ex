defmodule GenDurable.Scheduler do
  @moduledoc """
  Per-queue picker loop (spec §6). Demand-driven: each poll picks at most the
  number of free concurrency slots and runs each instance in a supervised Task.
  Also heartbeats the leases of in-flight instances (one batched UPDATE per
  tick, regardless of concurrency).

  `partition_key` serialization (spec §6) is handled per-job: a session-level
  advisory lock is taken on a checked-out connection held for the whole step,
  and released after the outcome commits. If the lock is contended, the row is
  returned to `runnable` and skipped.
  """

  use GenServer

  alias GenDurable.{Executor, Queries}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      config: opts.config,
      queues: [opts.queue],
      concurrency: opts.concurrency,
      worker: opts.worker,
      poll_interval: opts.poll_interval,
      heartbeat_interval: opts.heartbeat_interval,
      task_sup: opts.task_sup,
      in_flight: %{}
    }

    schedule(:poll, 0)
    schedule(:heartbeat, state.heartbeat_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    free = state.concurrency - map_size(state.in_flight)
    state = if free > 0, do: dispatch(state, free), else: state
    schedule(:poll, state.poll_interval)
    {:noreply, state}
  end

  def handle_info(:heartbeat, state) do
    ids = state.in_flight |> Map.values() |> Enum.map(&elem(&1, 0))
    Queries.heartbeat(state.config.repo, ids, state.worker, state.config.lease_ttl_ms)
    schedule(:heartbeat, state.heartbeat_interval)
    {:noreply, state}
  end

  # Task finished and returned a value: success path (outcome committed).
  def handle_info({ref, _result}, state) when is_map_key(state.in_flight, ref) do
    Process.demonitor(ref, [:flush])
    state = update_in(state.in_flight, &Map.delete(&1, ref))
    send(self(), :poll)
    {:noreply, state}
  end

  # Task crashed before returning: no outcome was written. Leave the row
  # 'executing'; the reaper will recover it (spec §4.3). Do NOT call handle/2.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state)
      when is_map_key(state.in_flight, ref) do
    state = update_in(state.in_flight, &Map.delete(&1, ref))
    send(self(), :poll)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp dispatch(state, free) do
    config = state.config
    jobs = Queries.pick(config.repo, state.queues, free, state.worker, config.lease_ttl_ms)

    Enum.reduce(jobs, state, fn job, st ->
      task =
        Task.Supervisor.async_nolink(st.task_sup, fn -> execute_job(config, job) end)

      put_in(st.in_flight[task.ref], {job.id, job.partition_key})
    end)
  end

  # Run the step, serializing on partition_key when present.
  defp execute_job(config, %{partition_key: nil} = job), do: Executor.run(config, job)

  defp execute_job(config, %{partition_key: key} = job) do
    repo = config.repo

    repo.checkout(fn ->
      if Queries.advisory_try_lock(repo, key) do
        try do
          Executor.run(config, job)
        after
          Queries.advisory_unlock(repo, key)
        end
      else
        # Another worker holds this key; hand the row back.
        Queries.reset_to_runnable(repo, job.id)
        :skipped
      end
    end)
  end

  defp schedule(msg, after_ms), do: Process.send_after(self(), msg, after_ms)
end
