defmodule GenDurable.Poke do
  @moduledoc """
  The poke transport: how an insert announces "runnable work exists NOW" to
  schedulers, instead of leaving discovery to the poll timer. Configured per
  instance via the `:poke` engine option:

    * `:local` (default) — poke the caller's node only. Zero moving parts;
      other nodes discover the work on their next poll.
    * `:cluster` — poke every node's schedulers of the queue over Erlang
      distribution. Membership rides an OTP `:pg` scope (one per instance;
      schedulers join their queue's group), so a poke is an ETS lookup plus
      direct sends — no broadcast storms, only nodes that actually run the
      queue are reached. Without distribution it degrades to `:local`.
    * `{:redis, url_or_opts}` — publish over Redis Pub/Sub, for clusters
      without Erlang distribution. Requires the optional `:redix` dependency.
      The caller's node is poked directly (no Redis round-trip, and a Redis
      outage cannot lose local pokes); the publish carries the origin VM's
      token so subscribers skip self-originated messages. A per-queue
      distributed dedup lock (`SET NX PX`) collapses a burst/stream of inserts
      into at most **one broadcast per ~100ms window across the whole fleet**,
      so the fan-out does not scale with the insert rate.

  Besides inserts, pokes announce every engine-driven wake: a signal flipping
  a parked row, a fan-out's freshly-inserted children (in *their* queues), and
  a parent whose join the last child just completed. Delivery is
  **best-effort in every mode** — a lost poke costs one poll interval, never
  correctness. The poll remains the discovery floor for what a poke cannot
  see: retry backoffs, the reaper's wakes (await timeouts, crash reclaims),
  and remote events under `:local`.

  A poke only wakes an **idle** scheduler — the idle → work transition it
  exists for. A scheduler with work in flight drops it and rediscovers new work
  on its next task completion (or poll), so a fan-out never becomes N nodes all
  picking on every insert. Together with the Redis dedup lock, a hot insert
  stream stays a trickle of picks, not a herd.
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
  # The Redis Pub/Sub channel of an instance.
  def channel(name), do: "gen_durable:#{inspect(name)}:poke"

  @doc false
  # Route a poke through the instance's configured transport. Called by
  # insert/insert_all for every queue that just received a due row. With no
  # running instance (bare-repo Testing usage, stale config) every branch
  # degrades to a no-op.
  def dispatch(name, queue) do
    case :persistent_term.get({GenDurable, name}, nil) do
      %{poke: :cluster} ->
        fanout(name, queue, :all)

      %{poke: {:redis, _}} = config ->
        fanout(name, queue, :local)
        publish(config, queue)

      _ ->
        fanout(name, queue, :local)
    end
  end

  @doc false
  # Poke the local schedulers of `queue` (instance `name`). Fire-and-forget —
  # a node that runs no scheduler for the queue is a no-op.
  def local(name, queue), do: fanout(name, queue, :local)

  @doc false
  # Dispatch one poke per distinct queue of the given insert-params that are
  # due NOW — the shared shape behind insert/insert_all and the executor's
  # child fan-out. Future-scheduled rows wake nobody (not pickable yet).
  def dispatch_rows(name, params) do
    for queue <-
          params |> Enum.filter(&due_now?/1) |> Enum.map(&to_string(&1.queue)) |> Enum.uniq() do
      dispatch(name, queue)
    end

    :ok
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

  # Publish the poke for OTHER nodes; the local leg already ran. A distributed
  # dedup lock collapses a burst/stream of inserts for one queue into at most
  # ONE broadcast per window across the whole fleet: `SET NX PX` wins the window
  # and only the winner PUBLISHes, so the fan-out no longer scales with the
  # insert rate (the herd it was slamming the DB with). One `EVAL` does the
  # lock+publish atomically in a single round-trip. Tagged with this VM's token
  # so our own listener drops it (see Listener). Best-effort: Redis down or the
  # publisher restarting loses nothing but latency (the poll is the floor).
  @poke_dedup_ms 100
  @poke_dedup_lua "if redis.call('SET', KEYS[1], '1', 'NX', 'PX', ARGV[1]) then " <>
                    "return redis.call('PUBLISH', KEYS[2], ARGV[2]) else return 0 end"

  defp publish(%{name: name, poke_token: token}, queue) do
    Redix.noreply_command(publisher(name), [
      "EVAL",
      @poke_dedup_lua,
      "2",
      dedup_key(name, queue),
      channel(name),
      Integer.to_string(@poke_dedup_ms),
      token <> "|" <> queue
    ])

    :ok
  catch
    _, _ -> :ok
  end

  @doc false
  # Per-queue distributed-dedup lock key: `SET NX PX` on it gates the broadcast
  # so a stream of inserts pokes the fleet at most once per window.
  def dedup_key(name, queue), do: "gen_durable:#{inspect(name)}:pokelock:#{queue}"

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
      case String.split(payload, "|", parts: 2) do
        # self-originated: the direct local leg already poked this node
        [token, _queue] when token == state.token -> :ok
        [_token, queue] -> GenDurable.Poke.local(state.name, queue)
        _ -> :ok
      end

      {:noreply, state}
    end

    # :subscribed / :disconnected notices — Redix logs disconnections itself,
    # and a poke gap is covered by the poll
    def handle_info(_msg, state), do: {:noreply, state}
  end
end
