defmodule GenDurable.Poke do
  @moduledoc """
  The poke transport: how "runnable work exists NOW" reaches schedulers instead of
  leaving discovery to the poll timer. Configured per instance via the `:poke`
  engine option:

    * `:local` (default) — poke the caller's node only. Zero moving parts beyond the
      per-instance emitter; other nodes discover the work on their next poll.
    * `:cluster` — poke every node's schedulers of the queue over Erlang
      distribution. Membership rides an OTP `:pg` scope, so only nodes that actually
      run the queue are reached. Without distribution it degrades to `:local`.
    * `{:redis, url_or_opts}` — publish over Redis Pub/Sub, for clusters without
      Erlang distribution. Requires the optional `:redix` dependency. The caller's
      node is poked directly (no Redis round-trip, and a Redis outage cannot lose
      local pokes); the publish carries the origin VM's token so subscribers skip
      self-originated messages.
    * `:postgres` — publish over Postgres `LISTEN`/`NOTIFY`, for clusters that share
      a Postgres but have **neither** Erlang distribution **nor** Redis. No extra
      dependency (reuses the repo's Postgrex). The caller's node is poked directly;
      other nodes receive a `NOTIFY` on the instance channel, tagged with the origin
      VM token so a node drops its own message. A subscriber (`PgListener`) holds a
      dedicated `Postgrex.Notifications` connection and turns foreign notifications
      into local pokes.
    * `:none` — never poke anyone, ever. No emitter, no listener, no cross-node
      traffic; the poll interval is the sole discovery mechanism. For deployments
      that accept poll latency, or where `NOTIFY`/distribution/Redis are unwanted.

  ## Out-of-band emission

  Every poke is emitted **out of band**: `dispatch/2` and `dispatch_rows/2` are async
  casts to a per-instance `Emitter` process, so the caller (an insert, a signal wake,
  a batched outcome flush) returns immediately having done **zero** transport work on
  its hot path. For `:postgres` this is not just tidy — it moves the `NOTIFY` off the
  insert's commit path, where it would otherwise serialize commits fleet-wide on the
  database-global notify-queue lock.

  The Emitter **coalesces per queue over a short in-VM window** (`@window_ms`): a
  burst/stream of pokes for one queue fans out **at most once per window per node**.
  This send-side window *replaces* the old Redis `SET NX PX` distributed dedup lock —
  fan-in is now bounded by node count (each node emits ≤ 1 broadcast per queue per
  window), not by the insert rate.

  Besides inserts, pokes announce every engine-driven wake: a signal flipping a
  parked row, a fan-out's freshly-inserted children (in *their* queues), and a parent
  whose join the last child just completed. Delivery is **best-effort in every mode**
  — a lost poke costs one poll interval, never correctness. The poll remains the
  discovery floor for what a poke cannot see (retry backoffs, the reaper's wakes,
  remote events under `:local`) — and is the **only** discovery mechanism under
  `:none`.

  A poke only wakes an **idle** scheduler — the idle → work transition it exists for.
  A scheduler with work in flight drops it and rediscovers new work on its next task
  completion (or poll), so a fan-out never becomes N nodes all picking on every
  insert. That receive-side gate composes with the Emitter's send-side window.
  """

  # :redix is an optional dependency; these calls only execute when the
  # {:redis, _} transport is configured (validated at engine boot).
  @compile {:no_warn_undefined, Redix}

  @doc false
  # The instance's :pg scope (schedulers join their queue's group in it).
  def scope(name), do: Module.concat(name, Schedulers)

  @doc false
  # The registered name of the instance's Redis publisher connection.
  def publisher(name), do: Module.concat(name, PokePublisher)

  @doc false
  # The registered name of the instance's out-of-band poke Emitter.
  def emitter(name), do: Module.concat(name, PokeEmitter)

  @doc false
  # The Pub/Sub (Redis) / NOTIFY (Postgres) channel of an instance.
  def channel(name), do: "gen_durable:#{inspect(name)}:poke"

  @doc false
  # Route a poke through the instance's out-of-band Emitter — async, coalesced. Called
  # by insert/insert_all and the outcome flush for every queue that just received a due
  # row. `:none` and a not-running instance (bare-repo Testing usage, stale config) are
  # no-ops (the poll is the floor).
  def dispatch(name, queue) do
    case :persistent_term.get({GenDurable, name}, nil) do
      %{poke: :none} -> :ok
      %{poke: _} -> GenDurable.Poke.Emitter.emit(emitter(name), queue)
      nil -> :ok
    end
  end

  @doc false
  # Poke the local schedulers of `queue` (instance `name`) synchronously. Fire-and-forget
  # — a node that runs no scheduler for the queue is a no-op. Used by the listeners (a
  # foreign notification is already coalesced at its origin) and the Emitter's local leg.
  def local(name, queue), do: fanout(name, queue, :local)

  @doc false
  # Dispatch one poke per distinct queue of the given insert-params that are due NOW —
  # the shared shape behind insert/insert_all and the executor's child fan-out.
  # Future-scheduled rows wake nobody (not pickable yet).
  def dispatch_rows(name, params) do
    for queue <-
          params |> Enum.filter(&due_now?/1) |> Enum.map(&to_string(&1.queue)) |> Enum.uniq() do
      dispatch(name, queue)
    end

    :ok
  end

  @doc false
  # The actual fan-out for one queue, run BY the Emitter (out of band, post-coalescing).
  # This is where the transport branch lives; every branch pokes the local node too, so a
  # broadcast hiccup never loses a local poke.
  def emit_now(config, queue) do
    name = config.name

    case config.poke do
      :cluster ->
        fanout(name, queue, :all)

      {:redis, _} ->
        fanout(name, queue, :local)
        publish(config, queue)

      :postgres ->
        fanout(name, queue, :local)
        notify(config, queue)

      _ ->
        fanout(name, queue, :local)
    end
  end

  defp due_now?(%{eligible_at: nil}), do: true

  defp due_now?(%{eligible_at: %DateTime{} = at}),
    do: DateTime.compare(at, DateTime.utc_now()) != :gt

  # an exotic timestamp shape we can't compare — poke anyway; the cost of a
  # false poke is one empty pick, the cost of a missed one is poll latency
  defp due_now?(_), do: true

  defp fanout(name, queue, reach) do
    scope = scope(name)

    if Process.whereis(scope) do
      members =
        case reach do
          :all -> :pg.get_members(scope, queue)
          :local -> :pg.get_local_members(scope, queue)
        end

      for pid <- members, do: send(pid, :poke)
    end

    :ok
  catch
    # the instance shut down between the whereis check and the lookup
    _, _ -> :ok
  end

  # Publish the poke for OTHER nodes over Redis; the local leg already ran. A plain
  # PUBLISH now — the burst dedup that used to ride a `SET NX PX` distributed lock
  # moved to the Emitter's in-VM window (one broadcast per queue per window per node).
  # Tagged with this VM's token so our own listener drops it. Best-effort.
  defp publish(%{name: name, poke_token: token}, queue) do
    Redix.noreply_command(publisher(name), ["PUBLISH", channel(name), token <> "|" <> queue])
    :ok
  catch
    _, _ -> :ok
  end

  # Publish the poke for OTHER nodes over Postgres `NOTIFY`; the local leg already ran.
  # Runs on the repo's pooled connection as its own autocommit statement — NOT inside
  # any insert transaction (the Emitter runs out of band), so it never adds notify-queue
  # lock contention to a commit. Tagged with this VM's token; best-effort.
  defp notify(%{repo: repo, name: name, poke_token: token}, queue) do
    _ = repo.query("SELECT pg_notify($1, $2)", [channel(name), token <> "|" <> queue])
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defmodule Emitter do
    @moduledoc false
    # The out-of-band, per-instance send side of every transport. `dispatch` casts a
    # queue here; the Emitter coalesces per queue over `@window_ms` (leading edge fires
    # immediately, then at most one trailing fan-out per window while pokes keep
    # arriving) and calls `Poke.emit_now/2`. This is the send-side dedup that replaced
    # the Redis distributed lock.
    use GenServer

    alias GenDurable.Poke

    @window_ms 100

    def start_link(opts),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    # Out-of-band cast; a no-op when the emitter isn't running (bare-repo, shutdown race).
    def emit(server, queue) do
      case GenServer.whereis(server) do
        nil -> :ok
        pid -> GenServer.cast(pid, {:poke, queue})
      end
    end

    @impl true
    def init(opts) do
      {:ok,
       %{
         config: Keyword.fetch!(opts, :config),
         window_ms: Keyword.get(opts, :window_ms, @window_ms),
         # queue => monotonic ms of last fan-out (leading edge / suppression clock)
         fired_at: %{},
         # queues poked during their suppression window, awaiting a trailing fan-out
         pending: MapSet.new(),
         timer: nil
       }}
    end

    @impl true
    def handle_cast({:poke, queue}, state) do
      now = System.monotonic_time(:millisecond)

      if suppressed?(state, queue, now) do
        {:noreply, arm(%{state | pending: MapSet.put(state.pending, queue)})}
      else
        Poke.emit_now(state.config, queue)
        {:noreply, %{state | fired_at: Map.put(state.fired_at, queue, now)}}
      end
    end

    @impl true
    def handle_info(:flush, state) do
      now = System.monotonic_time(:millisecond)
      {ready, waiting} = Enum.split_with(state.pending, &(not suppressed?(state, &1, now)))

      fired_at =
        Enum.reduce(ready, state.fired_at, fn queue, acc ->
          Poke.emit_now(state.config, queue)
          Map.put(acc, queue, now)
        end)

      state = %{state | fired_at: fired_at, pending: MapSet.new(waiting), timer: nil}
      {:noreply, if(waiting == [], do: state, else: arm(state))}
    end

    defp suppressed?(state, queue, now) do
      case Map.get(state.fired_at, queue) do
        nil -> false
        last -> now - last < state.window_ms
      end
    end

    defp arm(%{timer: nil} = state),
      do: %{state | timer: Process.send_after(self(), :flush, state.window_ms)}

    defp arm(state), do: state
  end

  defmodule Listener do
    @moduledoc false
    # The subscriber side of the {:redis, _} transport: holds the Pub/Sub
    # connection and turns foreign-origin messages into local pokes.
    use GenServer

    @compile {:no_warn_undefined, Redix.PubSub}

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(%{name: name, redis: redis, token: token}) do
      {:ok, pubsub} = start_pubsub(redis)
      {:ok, _ref} = Redix.PubSub.subscribe(pubsub, GenDurable.Poke.channel(name), self())
      {:ok, %{name: name, token: token, pubsub: pubsub}}
    end

    defp start_pubsub(url) when is_binary(url), do: Redix.PubSub.start_link(url)
    defp start_pubsub(opts) when is_list(opts), do: Redix.PubSub.start_link(opts)

    @impl true
    def handle_info({:redix_pubsub, _pid, _ref, :message, %{payload: payload}}, state) do
      route(payload, state)
      {:noreply, state}
    end

    # :subscribed / :disconnected notices — Redix logs disconnections itself,
    # and a poke gap is covered by the poll
    def handle_info(_msg, state), do: {:noreply, state}

    defp route(payload, state) do
      case String.split(payload, "|", parts: 2) do
        # self-originated: the direct local leg already poked this node
        [token, _queue] when token == state.token -> :ok
        [_token, queue] -> GenDurable.Poke.local(state.name, queue)
        _ -> :ok
      end
    end
  end

  defmodule PgListener do
    @moduledoc false
    # The subscriber side of the `:postgres` transport: holds a dedicated
    # `Postgrex.Notifications` connection (LISTEN needs its own session — it must not
    # borrow a pooled repo connection), LISTENs on the instance channel, and turns
    # foreign-origin notifications into local pokes. Same payload convention as Redis.
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(%{name: name, repo: repo, token: token}) do
      {:ok, conn} = Postgrex.Notifications.start_link(conn_opts(repo))
      {:ok, _ref} = Postgrex.Notifications.listen(conn, GenDurable.Poke.channel(name))
      {:ok, %{name: name, token: token, conn: conn}}
    end

    # Postgrex connection opts from the repo config — take only the keys Postgrex knows,
    # dropping Ecto-pool keys.
    defp conn_opts(repo) do
      Keyword.take(repo.config(), [
        :hostname,
        :port,
        :username,
        :password,
        :database,
        :socket,
        :socket_dir,
        :ssl,
        :ssl_opts,
        :parameters,
        :types
      ])
    end

    @impl true
    def handle_info({:notification, _conn, _ref, _channel, payload}, state) do
      case String.split(payload, "|", parts: 2) do
        # self-originated: the direct local leg already poked this node
        [token, _queue] when token == state.token -> :ok
        [_token, queue] -> GenDurable.Poke.local(state.name, queue)
        _ -> :ok
      end

      {:noreply, state}
    end

    def handle_info(_msg, state), do: {:noreply, state}
  end
end
