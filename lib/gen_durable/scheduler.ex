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
      invisible to other nodes), and buffered rows carry their pick-time inbox
      snapshot (`ctx.all`/`ctx.childs` are batch-loaded with the pick). Buffered
      rows are **heartbeated**, so their leases never go stale regardless of
      depth — depth is decoupled from `lease_ttl`.
    * `min_demand` — batch gate: don't pick unless at least this many slots are
      free (so picks come fat, not one row at a time). Ignored when the queue is
      fully idle, to avoid starvation.
    * `poll_interval` — base idle poll. `max_poll_interval` — backoff ceiling: an
      empty pick on a fully idle queue doubles the interval up to this cap, then a
      non-empty pick (or any in-flight work) snaps back to the base. This is the
      lever that cuts idle DB load. (LISTEN/NOTIFY is banned, so polling is the
      discovery path — the point is to poll *adaptively*, not constantly.)

  **Inserts, signal wakes, and fan-out transitions poke the queue's schedulers
  directly** (see `GenDurable.Poke`): a row that just became runnable is
  discovered immediately, not on the next poll tick — same-node always,
  cross-node with the `:cluster` or `{:redis, _}` transports. Polling covers
  what a poke cannot see — retry backoffs, the reaper's wakes, and remote
  events under the default `:local` transport.

  Buffered and in-flight rows are both `executing` + leased; the heartbeat (one
  batched UPDATE per tick over `buffer ++ in_flight`) keeps every claimed row
  alive. If this scheduler dies, the whole claimed set waits one `lease_ttl` for
  the reaper — so deeper buffers mean a larger crash blip, bounded by the (short)
  TTL, not by the buffer depth.

  `concurrency_key` serialization is enforced at claim time by the database
  (the UNIQUE partial index over executing keys): the pick either commits the
  only executing row for a key or retries on the violation — no per-step locks,
  no pinned connections. See `GenDurable.Queries` (the pick).
  """

  use GenServer

  alias GenDurable.{Executor, Queries}

  # Retry cadence for re-joining the poke scope while it restarts.
  @rejoin_retry 100

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc false
  # The stable claim-identity prefix for this instance+queue on this VM; a
  # worker id is the prefix plus a per-incarnation unique suffix. Startup
  # reclaim matches the prefix to recognize a dead predecessor's claims, so the
  # prefix must be unique per LIVE VM: the node name when distributed, else
  # hostname + OS pid (two non-distributed VMs would otherwise both be
  # "nonode@nohost" and reclaim each other's live claims).
  def claim_prefix(name, queue), do: "#{inspect(name)}:#{queue}@#{vm_id()}-"

  @doc false
  # A token unique per LIVE VM: the node name when distributed, else
  # hostname + OS pid. Shared by the claim identity (above) and the poke
  # transport (origin tagging — see GenDurable.Poke).
  def vm_id do
    if Node.alive?() do
      Atom.to_string(node())
    else
      {:ok, host} = :inet.gethostname()
      "#{host}##{System.pid()}"
    end
  end

  @impl true
  def init(opts) do
    # Trap exits so a supervisor shutdown runs terminate/2 (graceful drain).
    Process.flag(:trap_exit, true)

    prefix = claim_prefix(opts.config.name, opts.queue)
    worker = prefix <> Integer.to_string(System.unique_integer([:positive]))

    # Startup reclaim: rows still claimed by a DEAD incarnation of this
    # instance+queue+VM (same prefix, older unique suffix) go straight back to
    # runnable instead of waiting out lease_ttl. A prior incarnation is
    # certainly dead — the prefix pins the VM (see claim_prefix/2) and, within
    # a VM, the supervisor serializes scheduler lifetimes. Its orphaned tasks
    # may still be running; the outcome ownership guard drops their late
    # commits, so an early re-run is safe (at-least-once). The staleness margin
    # (remaining lease below `lease_ttl - 2 × heartbeat`) additionally protects
    # against claim-prefix collisions: a live owner's freshly-beaten claims are
    # never touched — see Queries.reclaim_orphans/4.
    margin_ms = max(opts.config.lease_ttl_ms - 2 * opts.heartbeat_interval, 0)
    reclaimed = Queries.reclaim_orphans(opts.config.repo, opts.queue, prefix, margin_ms)

    # Join the instance's poke scope under the queue name, so inserts can find
    # this scheduler (locally, or from any node in :cluster mode). Membership
    # is cleaned up by :pg when this process dies — and dies with the scope
    # process (its ETS table), so the scope is monitored and re-joined if it
    # ever restarts; otherwise every poke on this node would silently no-op
    # until the schedulers themselves restarted.
    scope_ref =
      case join_scope(opts.scope, opts.queue) do
        {:ok, ref} ->
          ref

        :retry ->
          Process.send_after(self(), :rejoin_scope, @rejoin_retry)
          nil
      end

    if reclaimed > 0 do
      :telemetry.execute(
        [:gen_durable, :scheduler, :reclaimed],
        %{count: reclaimed},
        %{queue: opts.queue}
      )
    end

    state = %{
      config: opts.config,
      scope: opts.scope,
      scope_ref: scope_ref,
      queue: opts.queue,
      concurrency: opts.concurrency,
      prefetch: opts.prefetch,
      # Clamped: a min_demand above the claim ceiling could never be satisfied,
      # so refill would only ever fire from full idle — a silent config footgun.
      min_demand: min(opts.min_demand, opts.concurrency + opts.prefetch),
      worker: worker,
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

  # A local insert nudged us: runnable work exists NOW. Coalesce any burst of
  # pokes already in the mailbox into one refill, then go through the normal
  # demand gates (ceiling, min_demand) — a poke is discovery, not admission.
  # A fetch snaps the idle backoff to base; a miss (row raced away to another
  # node, or throttled) leaves the poll cadence untouched — unlike an empty
  # poll, an empty poke is not evidence of continued idleness.
  def handle_info(:poke, state) do
    flush_pokes()
    {state, fetched} = fill(state)
    state = if fetched > 0, do: %{state | cur_poll: state.poll_interval}, else: state
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

  # The poke scope died and took the membership ETS with it; re-join once the
  # supervisor has it back up. The poll covers the gap.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{scope_ref: ref} = state) do
    Process.send_after(self(), :rejoin_scope, @rejoin_retry)
    {:noreply, %{state | scope_ref: nil}}
  end

  def handle_info(:rejoin_scope, state) do
    case join_scope(state.scope, state.queue) do
      {:ok, ref} ->
        {:noreply, %{state | scope_ref: ref}}

      :retry ->
        Process.send_after(self(), :rejoin_scope, @rejoin_retry)
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Monitor-then-join, tolerating the scope dying at any point in between —
  # the caller retries on :retry.
  defp join_scope(scope, queue) do
    case Process.whereis(scope) do
      nil ->
        :retry

      pid ->
        ref = Process.monitor(pid)

        try do
          :ok = :pg.join(scope, queue, self())
          {:ok, ref}
        catch
          _, _ ->
            Process.demonitor(ref, [:flush])
            :retry
        end
    end
  end

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

  # Spawn buffered jobs into free executor slots in buffer order. The buffer
  # roughly follows the picker's urgency order (the claim UPDATE's RETURNING
  # order is not SQL-guaranteed, so this is best-effort, not a contract).
  defp drain(%{buffer: [job | rest]} = state)
       when map_size(state.in_flight) < state.concurrency do
    task = Task.Supervisor.async_nolink(state.task_sup, fn -> Executor.run(state.config, job) end)

    state = %{
      state
      | buffer: rest,
        in_flight: Map.put(state.in_flight, task.ref, job.id)
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

  defp flush_pokes do
    receive do
      :poke -> flush_pokes()
    after
      0 -> :ok
    end
  end

  defp buffer_ids(buffer), do: Enum.map(buffer, & &1.id)
  defp in_flight_ids(in_flight), do: Map.values(in_flight)

  defp schedule(msg, after_ms), do: Process.send_after(self(), msg, after_ms)
end
