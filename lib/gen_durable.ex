defmodule GenDurable do
  @moduledoc """
  Postgres-backed durable execution: an FSM whose state is committed to Postgres
  before each step proceeds, so instances survive process and node death and resume
  where they left off.

  An FSM is a database row, not a process — there is no GenServer per instance; each
  step runs as an ephemeral task. The runtime backbone (scheduler, reaper, GC) is a
  small set of GenServers that pick runnable rows and dispatch them.

  Start the engine in a host supervision tree:

      {GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5]}

  Several engine instances can coexist (different repos, disjoint queue sets):
  give each a `:name` (an atom, default `GenDurable`) and route API calls with
  `name: MyEngine` — see `GenDurable.Supervisor`.

  FSMs are resolved from the row (the `fsm` column defaults to the module name);
  pass `:fsms` only for a custom `:name` or to keep an old `:version` running.

  Then enqueue instances and deliver signals — address a signal by the returned
  internal id, or by the `:correlation_key` you set at insert:

      {:ok, _id} =
        GenDurable.insert(Checkout, state: %{order: 42}, correlation_key: "order:42")

      :ok = GenDurable.signal("order:42", "payment_confirmed", %{amount: 100}, dedup_key: "evt-7")

  ## Telemetry

  Attach to these `:telemetry` events (`[:gen_durable, …]`):

    * `[:gen_durable, :step, :stop]` — a step finished. Measurements `%{duration}`
      (native units); metadata `%{id, fsm, step, kind}` where `kind` is the outcome
      (`:next` · `:retry` · `:await` · `:schedule_childs` · `:done` · `:stop`).
    * `[:gen_durable, :pick, :stop]` — a picker batch ran. Measurements
      `%{count, demand}`; metadata `%{queue, worker}`. Watch `count` vs `demand` to
      see how full picks are.
    * `[:gen_durable, :scheduler, :saturation]` — per-poll gauge. Measurements
      `%{in_flight, buffer, concurrency, prefetch}`; metadata `%{queue}`. The signal
      for tuning the feeder knobs.
    * `[:gen_durable, :scheduler, :drain]` — graceful shutdown of a queue.
      Measurements `%{released, in_flight}`; metadata `%{queue}`.
    * `[:gen_durable, :scheduler, :reclaimed]` — at scheduler startup, claims of a
      dead predecessor (same instance+queue+VM) were released early instead of
      waiting out the lease. Measurements `%{count}`; metadata `%{queue}`.
    * `[:gen_durable, :concurrency, :contended]` — a cross-node claim race on a
      concurrency_key hit the unique arbiter and the pick retried. Measurements
      `%{count}`; metadata `%{queue}`.
    * `[:gen_durable, :concurrency, :throttled]` — a concurrency gate admitted fewer
      rows than wanted in a pick (the cap is biting). Measurements
      `%{wanted, admitted}`; metadata `%{key, queue}`.
    * `[:gen_durable, :outcome, :stale]` — a worker committed an outcome for a row
      it no longer owns (its lease expired and the row was reclaimed while the step
      ran); the outcome was dropped and the current claimant redoes the step.
      Measurements `%{count}`; metadata `%{id, fsm, step, kind}`.
    * `[:gen_durable, :reaper, :reaped]` — expired leases reclaimed. Measurements
      `%{count}`; metadata `%{}`.
    * `[:gen_durable, :await, :timeout]` — parked instances whose await deadline
      fired were woken (a wake, not a failure). Measurements `%{count}`; metadata `%{}`.
    * `[:gen_durable, :gc, :swept]` — a GC sweep deleted or repaired something.
      Measurements `%{count, buckets, gates}` (terminal rows, stale rate buckets,
      reconciled/swept concurrency-gate buckets); metadata `%{}`.
    * `[:gen_durable, :rate_limit, :throttled]` — a rate-limit bucket granted fewer rows
      than wanted in a pick. Measurements `%{wanted, granted}`; metadata
      `%{key, queue}`. The signal that a limit is biting.
    * `[:gen_durable, :rate_limit, :contended]` — two picks raced the first-ever
      grant of a rate key (cold-bucket mint) and the loser retried. Measurements
      `%{count}`; metadata `%{queue}`.
    * `[:gen_durable, :rate_limit, :unknown]` — a step named a `rate_limit` whose name has
      no configured policy (the row would stall). Measurements `%{count}`; metadata
      `%{key, name, fsm, step}`.

  See the guides for the feature documentation (jobs, state machines, signals,
  child fan-out, rate limiting, concurrency keys, identity, scheduling, operations).
  """

  alias GenDurable.{Queries, State}

  # --- engine lifecycle ------------------------------------------------------

  defdelegate start_link(opts), to: GenDurable.Supervisor

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, GenDurable),
      start: {GenDurable.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  # --- public API ------------------------------------------------------------

  @doc """
  Enqueue one FSM instance. Options: `:state` (alias `:args`, the job-form name),
  `:step` (default the FSM's `:initial`), `:queue`, `:priority`, `:concurrency_key`,
  `:correlation_key`, `:correlation_scope`, and scheduling — `:eligible_at` (a `DateTime`),
  or the sugar `:schedule_at` (a `DateTime`) / `:schedule_in` (milliseconds from now).
  `:name` routes the call to a named engine instance (default `GenDurable`);
  `insert_all/3` and `signal/4` take it too.
  Returns `{:ok, id}` or `{:error, :duplicate}`.

  `:correlation_key` is the instance's business identity — both the key you can later
  `signal/4` by (instead of the internal id) and a uniqueness guard. `:correlation_scope`
  is the list of statuses in which the key is "occupied": uniqueness is enforced and the
  signal address resolves only while the instance sits in one of them. It defaults to the
  non-terminal statuses (unique among live instances; freed on termination); pass `[]` to
  disable uniqueness, or include `:done`/`:failed` to keep the key reserved after the
  instance ends. A duplicate within the occupied scope is rejected as `{:error, :duplicate}`.
  With no `:correlation_key` the instance is neither addressable nor deduplicated.
  """
  def insert(fsm_module, opts \\ []) do
    params = build_params(fsm_module, opts)

    with {:ok, id} <- Queries.insert(repo(opts), params) do
      poke_local([params], opts)
      {:ok, id}
    end
  end

  @doc """
  Batch-enqueue instances in a single statement (dedup via the partial unique
  index). `entries` is a list of per-instance option keyword lists. Returns the
  list of inserted ids — duplicates are dropped and rows are inserted in
  `correlation_key` order, so the list has no positional mapping to `entries`.
  """
  def insert_all(fsm_module, entries, opts \\ []) do
    params = Enum.map(entries, &build_params(fsm_module, &1))
    ids = Queries.insert_all(repo(opts), params)
    if ids != [], do: poke_local(params, opts)
    ids
  end

  @doc """
  Wait for the instance to **settle** — the sync-over-async bridge: insert,
  poke does the discovery, `await/3` holds the caller until there is something
  to report or the deadline passes.

      case GenDurable.await(id, 1_000) do
        {:done, result}   -> # finished; result is the FSM's result map
        {:failed, error}  -> # terminal failure (retries exhausted)
        {:awaiting, snap} -> # parked: waiting on a signal / children
        {:busy, snap}     -> # deadline hit, still working — hand the client
                             # the id as a retry token (HTTP 202 + Retry-After)
        :not_found        -> # no such row (never existed, or GC-swept)
      end

  Calling `await/3` again with the same id is the retry protocol — a row that
  finished in the meantime answers immediately. Retry tokens live as long as
  the row: terminal rows are swept `retention` after finishing (a day by
  default), in-progress rows are never swept.

  Semantics to know:

    * **A timeout is not a failure.** `{:busy, _}` means the work continues;
      map it to "in progress", never to an error.
    * **The row is the truth.** The executor's completion push and the batched
      watcher only shorten the wait; the reply is always read back from the
      database. Same-node completions answer in ~ms; cross-node ones within
      the watcher tick (`await: [tick: 25]` engine option).
    * A **retryable error is not `failed`**: a step in backoff shows as
      `{:busy, %{status: :runnable, attempt: n}}`; `{:failed, _}` is terminal.
    * `until: :terminal` waits through parked states instead of returning
      `{:awaiting, _}` (default `until: :settled`).
    * `snap` is `%{status, step, attempt}` — enough to show progress.

  Options: `:until` (above), `:name`/`:repo` as everywhere. With no running
  instance (bare `:repo`), falls back to a plain poll loop.
  """
  def await(id, timeout \\ 5_000, opts \\ []) when is_integer(id) and timeout >= 0 do
    GenDurable.Await.await(
      repo(opts),
      Keyword.get(opts, :name, GenDurable),
      id,
      timeout,
      Keyword.get(opts, :until, :settled)
    )
  end

  @doc """
  Deliver a durable signal to an instance. Wakes the instance only on
  a name match. `:dedup_key` (default `nil`) makes redelivery idempotent.

  `target` is either the internal id (an integer) or a `:correlation_key` (a string) set
  at insert. Addressing by `correlation_key` resolves to the single instance currently
  occupying it (per its `:correlation_scope`). A target that does not resolve to a **live**
  instance — a freed or terminal key, a terminal id, an id that does not exist yet —
  returns `{:error, :no_target}`: nothing would ever read that inbox, and a signal is
  not held for an instance that does not exist. Returns `:ok` otherwise.
  """
  def signal(target, name, payload \\ %{}, opts \\ []) do
    case Queries.deliver_signal(
           repo(opts),
           target,
           to_string(name),
           Jason.encode!(payload),
           opts[:dedup_key]
         ) do
      {:ok, nil} ->
        :ok

      {:ok, woken_queue} ->
        # the wake flipped the target to runnable — announce it like an insert
        GenDurable.Poke.dispatch(Keyword.get(opts, :name, GenDurable), woken_queue)
        :ok

      {:error, _} = err ->
        err
    end
  end

  # --- helpers ---------------------------------------------------------------

  @doc false
  def build_params(fsm_module, opts) do
    state_module = fsm_module.__gd_state__()

    %{
      fsm: fsm_module.__gd_name__(),
      fsm_version: fsm_module.__gd_version__(),
      step: opts[:step] || fsm_module.__gd_initial__(),
      state_json: State.cast(state_module, opts[:state] || opts[:args] || %{}),
      queue: opts[:queue] || fsm_module.__gd_queue__(),
      priority: opts[:priority] || 0,
      # same nil | name | {name, partition} shapes as :rate_limit; a name with a
      # `concurrency_limits:` config makes the key a gate (semaphore of size
      # `limit`), an unconfigured key defaults to mutual exclusion (limit 1)
      concurrency_key: rate_limit_key(opts[:concurrency_key]),
      correlation_key: opts[:correlation_key],
      correlation_scope: correlation_scope(opts),
      rate_limit: rate_limit_key(opts[:rate_limit]),
      weight: opts[:weight] || 1,
      eligible_at: resolve_eligible_at(opts)
    }
  end

  # Resolve an insert-time `:rate_limit` key: nil | name | {name, partition}.
  defp rate_limit_key(nil), do: nil
  defp rate_limit_key(name) when is_binary(name) or is_atom(name), do: to_string(name)
  defp rate_limit_key({name, partition}), do: "#{name}:#{partition}"

  # The statuses in which a correlation_key is "occupied": the engine enforces uniqueness
  # over them and resolves the signal address within them. Supplied directly as
  # :correlation_scope; defaults to the non-terminal statuses (unique among live instances,
  # freed on termination). Pass [] for a key with no uniqueness, or include :done/:failed
  # to keep it reserved after the instance ends.
  @live_statuses ~w(runnable executing awaiting_signal awaiting_children)

  defp correlation_scope(opts),
    do: Enum.map(opts[:correlation_scope] || @live_statuses, &to_string/1)

  # Scheduling sugar. Precedence: explicit :eligible_at, then :schedule_at
  # (a DateTime), then :schedule_in (milliseconds from now). nil ⇒ now() in SQL.
  defp resolve_eligible_at(opts) do
    cond do
      opts[:eligible_at] -> opts[:eligible_at]
      opts[:schedule_at] -> opts[:schedule_at]
      opts[:schedule_in] -> DateTime.add(DateTime.utc_now(), opts[:schedule_in], :millisecond)
      true -> nil
    end
  end

  # Poke the schedulers of every queue that just received a row due NOW, so the
  # work is discovered immediately instead of on the next poll tick. Routed
  # through the instance's configured transport (local node / cluster / Redis —
  # see `GenDurable.Poke`); best-effort in every mode. A node with nobody to
  # poke (web-only topology, foreign queue, no engine at all — the Testing
  # path) is a no-op and the insert is discovered by whoever polls.
  defp poke_local(params, opts),
    do: GenDurable.Poke.dispatch_rows(Keyword.get(opts, :name, GenDurable), params)

  # Resolve the repo for an API call: an explicit `:repo` wins, else the config
  # of the `:name`d instance (default `GenDurable`).
  defp repo(opts), do: opts[:repo] || config(Keyword.get(opts, :name, GenDurable)).repo

  defp config(name) do
    :persistent_term.get({GenDurable, name}, nil) ||
      raise ArgumentError,
            "no GenDurable instance named #{inspect(name)} — is the engine started?"
  end
end
