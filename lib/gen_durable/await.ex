defmodule GenDurable.Await do
  @moduledoc """
  Wait for an instance to settle — the sync-over-async bridge
  (`GenDurable.await/3` is the public entry).

  The answer is **always read from the row** (the single source of truth);
  everything else only shortens the time until that read:

    * the **local fast path** — the executor, after committing a settled
      outcome, nudges same-node waiters through an ETS table owned by the
      watcher (one lookup per outcome; free when nobody waits). Waiters are
      never `Process.link`ed to anything — an engine shutdown can strand a
      waiter, not kill it;
    * the **watcher** — a per-instance batched poller: waiters register their
      ids, one `SELECT … WHERE id = ANY(...)` per tick covers all of them
      (cross-node executions land within a tick); idle means zero queries;
    * with **no running instance** (bare-repo usage) a plain per-caller poll
      loop, so `await/3` works everywhere `insert` does.

  A nudge is only a hint — the waiter re-checks the row and keeps waiting if
  it is not settled by its predicate. "Settled" means nothing left to do
  without external input: `done`, `failed`, `awaiting_signal`,
  `awaiting_children` (with `until: :terminal`, only the first two). Every
  subscription point is guarded: an engine stopping or a component restarting
  mid-await degrades the caller to polling (the row outlives the engine),
  never crashes it — blocked waiters re-check and re-assert their watch on a
  coarse cadence as the backstop.
  """

  alias GenDurable.Queries

  @tick 25

  # The blocked waiter's self-defense cadence: every @rearm ms it re-checks
  # the row and re-asserts its Watcher entry, so a lost subscription (the
  # Watcher crashed and restarted, taking its table and probe map with it)
  # costs bounded latency instead of the full await deadline.
  @rearm 1_000

  @doc false
  # The waiter table (`{id, pid}` bag), owned by the Watcher. Waiters land in
  # it via Watcher.watch — never by writing from their own process, and never
  # via Registry, whose `register/3` links the caller to a registry partition
  # (a stopping engine would then take every parked-in-await caller down with
  # it; here it merely strands them, and the re-arm cadence recovers).
  def table(name), do: Module.concat(name, Awaiters)

  @doc false
  def watcher(name), do: Module.concat(name, AwaitWatcher)

  @doc false
  # The executor's post-commit hook: nudge this node's waiters of `id`.
  # Fire-and-forget; over-nudging is harmless (waiters re-check the row).
  def notify_local(name, id) do
    for {_id, pid} <- :ets.lookup(table(name), id) do
      send(pid, {:gen_durable_await, id})
    end

    :ok
  catch
    # no table — the instance is not running (bare-repo usage, shutdown)
    _, _ -> :ok
  end

  @doc false
  # See GenDurable.await/3 for the public contract.
  def await(repo, name, id, timeout, until) when until in [:settled, :terminal] do
    deadline = System.monotonic_time(:millisecond) + timeout

    case check(repo, id, until) do
      {:settled, reply} ->
        reply

      {:pending, _snap} ->
        if subscribe(name, id, until) do
          try do
            wait(repo, name, id, until, deadline)
          after
            unsubscribe(name, id)
          end
        else
          poll(repo, id, until, deadline)
        end
    end
  end

  # Register for nudges — one Watcher call covers both push paths (it owns
  # the executor-facing table and its own probe map). The Watcher lives under
  # the engine's supervisor and can die between any check and the call
  # (engine stopping, component restarting) — degrade to the poll loop, never
  # crash the caller.
  defp subscribe(name, id, until) do
    :ok = GenDurable.Await.Watcher.watch(name, id, until)
    true
  catch
    _, _ -> false
  end

  # Runs in an `after` block: it must never raise — an exception here would
  # replace the caller's already-computed reply.
  defp unsubscribe(name, id) do
    GenDurable.Await.Watcher.unwatch(name, id)
    flush_nudges(id)
  catch
    _, _ -> :ok
  end

  # Mailbox hygiene for long-lived callers: drop nudges that raced the
  # unwatch. Best-effort — an executor that read the waiter table just before
  # we left can still deliver one late message after this flush.
  defp flush_nudges(id) do
    receive do
      {:gen_durable_await, ^id} -> flush_nudges(id)
    after
      0 -> :ok
    end
  end

  # Blocked path: woken by an executor nudge or a watcher tick, whichever
  # comes first; re-checks the row on every wake (nudges are hints, the row is
  # the truth). The deadline check runs on the freshest snapshot. Each receive
  # is capped at @rearm so a lost subscription degrades to a coarse poll, and
  # the Watcher entry is re-asserted on the way (idempotent map put).
  defp wait(repo, name, id, until, deadline) do
    timeout = min(max(deadline - System.monotonic_time(:millisecond), 0), @rearm)

    receive do
      {:gen_durable_await, ^id} ->
        case check(repo, id, until) do
          {:settled, reply} -> reply
          {:pending, _snap} -> wait(repo, name, id, until, deadline)
        end
    after
      timeout ->
        case check(repo, id, until) do
          {:settled, reply} ->
            reply

          {:pending, snap} ->
            if System.monotonic_time(:millisecond) >= deadline do
              {:busy, snap}
            else
              rewatch(name, id, until)
              wait(repo, name, id, until, deadline)
            end
        end
    end
  end

  # Guarded like subscribe/3: a Watcher mid-restart just leaves us on the
  # @rearm cadence until the next pass.
  defp rewatch(name, id, until) do
    :ok = GenDurable.Await.Watcher.watch(name, id, until)
  catch
    _, _ -> :ok
  end

  # No running instance: a plain per-caller poll loop. Works wherever `insert`
  # with a bare repo does (tests, scripts); costs one PK read per tick per
  # caller, which is fine for the places that have no engine to amortize it.
  defp poll(repo, id, until, deadline) do
    Process.sleep(@tick)

    case check(repo, id, until) do
      {:settled, reply} ->
        reply

      {:pending, snap} ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: {:busy, snap},
          else: poll(repo, id, until, deadline)
    end
  end

  defp check(repo, id, until) do
    case Queries.await_status(repo, id) do
      nil ->
        {:settled, :not_found}

      %{status: "done"} = snap ->
        {:settled, {:done, snap.result}}

      %{status: "failed"} = snap ->
        {:settled, {:failed, snap.error}}

      %{status: parked} = snap when parked in ~w(awaiting_signal awaiting_children) ->
        snap = snapshot(snap)
        if until == :settled, do: {:settled, {:awaiting, snap}}, else: {:pending, snap}

      snap ->
        {:pending, snapshot(snap)}
    end
  end

  defp snapshot(snap),
    do: %{status: String.to_atom(snap.status), step: snap.step, attempt: snap.attempt}

  defmodule Watcher do
    @moduledoc false
    # The subscription point behind Await: a watch adds the waiter to the
    # executor-facing ETS table (the push path) and to the probe map — one
    # probe per tick covers every watched id, settled/missing ids get their
    # waiters nudged. Ids stay watched until an explicit unwatch (an
    # `until: :terminal` waiter keeps waiting past a parked nudge) or the
    # waiter's death (monitored). No waiters — no timer, no queries. Both the
    # table and the map die with this process; blocked waiters re-assert
    # themselves on their re-arm cadence after a restart.
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts.name)

    def watch(name, id, until),
      do: GenServer.call(GenDurable.Await.watcher(name), {:watch, id, until})

    def unwatch(name, id),
      do: GenServer.cast(GenDurable.Await.watcher(name), {:unwatch, id, self()})

    @impl true
    def init(opts) do
      table =
        :ets.new(opts.table, [:bag, :protected, :named_table, read_concurrency: true])

      {:ok,
       %{repo: opts.repo, tick: opts.tick, table: table, waiters: %{}, monitors: %{}, timer: nil}}
    end

    @impl true
    def handle_call({:watch, id, until}, {pid, _}, state) do
      # :bag — re-asserting the same {id, pid} (the waiter's re-arm) is a no-op
      true = :ets.insert(state.table, {id, pid})

      state =
        state
        |> put_in([:waiters, Access.key(id, %{}), pid], until)
        |> monitor(pid)
        |> ensure_timer()

      {:reply, :ok, state}
    end

    @impl true
    def handle_cast({:unwatch, id, pid}, state),
      do: {:noreply, state |> drop(id, pid) |> maybe_demonitor(pid)}

    @impl true
    def handle_info(:tick, state) do
      state = %{state | timer: nil}

      case Map.keys(state.waiters) do
        [] ->
          {:noreply, state}

        ids ->
          for {id, status} <- probe(state.repo, ids),
              {pid, until} <- state.waiters[id],
              nudge?(status, until) do
            send(pid, {:gen_durable_await, id})
          end

          {:noreply, ensure_timer(state)}
      end
    end

    def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
      state =
        Enum.reduce(Map.keys(state.waiters), state, fn id, acc -> drop(acc, id, pid) end)

      {:noreply, %{state | monitors: Map.delete(state.monitors, pid)}}
    end

    # A transient DB error (dropped connection, failover) must not crash the
    # Watcher: the restart would come up with an empty waiters map and strand
    # every in-flight cross-node await until its deadline (waiters register
    # once; only their @rearm cadence would eventually recover). Skip the
    # tick; the next one retries.
    defp probe(repo, ids) do
      Queries.await_probe(repo, ids)
    rescue
      _ -> []
    end

    # A missing (swept) or terminal row settles every waiter. A parked row
    # settles a `:settled` waiter but is a non-event for an `until: :terminal`
    # one — nudging those every tick for the whole park (hours, potentially)
    # would turn the batched watcher into a per-waiter busy-poll.
    defp nudge?(nil, _until), do: true
    defp nudge?(status, _until) when status in ~w(done failed), do: true
    defp nudge?(_parked, until), do: until == :settled

    defp drop(state, id, pid) do
      case state.waiters do
        %{^id => pids} ->
          true = :ets.delete_object(state.table, {id, pid})

          case Map.delete(pids, pid) do
            rest when map_size(rest) == 0 ->
              %{state | waiters: Map.delete(state.waiters, id)}

            rest ->
              %{state | waiters: Map.put(state.waiters, id, rest)}
          end

        _ ->
          state
      end
    end

    defp monitor(state, pid) do
      case state.monitors do
        %{^pid => _ref} -> state
        _ -> %{state | monitors: Map.put(state.monitors, pid, Process.monitor(pid))}
      end
    end

    # A caller's last unwatch releases its monitor — long-lived callers (a web
    # worker awaiting many ids over its life) must not accumulate one monitor
    # per distinct pid forever.
    defp maybe_demonitor(state, pid) do
      if Enum.any?(state.waiters, fn {_id, pids} -> is_map_key(pids, pid) end) do
        state
      else
        case Map.pop(state.monitors, pid) do
          {nil, _} ->
            state

          {ref, monitors} ->
            Process.demonitor(ref, [:flush])
            %{state | monitors: monitors}
        end
      end
    end

    defp ensure_timer(%{timer: nil} = state) when map_size(state.waiters) > 0,
      do: %{state | timer: Process.send_after(self(), :tick, state.tick)}

    defp ensure_timer(state), do: state
  end
end
