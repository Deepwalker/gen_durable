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
    * `[:gen_durable, :concurrency, :contended]` — a concurrency_key advisory lock
      was contended (a row was handed back). Measurements `%{count}`; metadata
      `%{id, fsm, concurrency_key}`.
    * `[:gen_durable, :outcome, :stale]` — a worker committed an outcome for a row
      it no longer owns (its lease expired and the row was reclaimed while the step
      ran); the outcome was dropped and the current claimant redoes the step.
      Measurements `%{count}`; metadata `%{id, fsm, step, kind}`.
    * `[:gen_durable, :reaper, :reaped]` — expired leases reclaimed. Measurements
      `%{count}`; metadata `%{}`.
    * `[:gen_durable, :await, :timeout]` — parked instances whose await deadline
      fired were woken (a wake, not a failure). Measurements `%{count}`; metadata `%{}`.
    * `[:gen_durable, :gc, :swept]` — a GC sweep deleted something. Measurements
      `%{count, buckets}` (terminal rows and stale rate buckets); metadata `%{}`.
    * `[:gen_durable, :rate_limit, :throttled]` — a rate-limit bucket granted fewer rows
      than wanted in a pick. Measurements `%{wanted, granted}`; metadata
      `%{key, queue}`. The signal that a limit is biting.
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
    Queries.insert(repo(opts), build_params(fsm_module, opts))
  end

  @doc """
  Batch-enqueue instances in a single statement (dedup via the partial unique
  index). `entries` is a list of per-instance option keyword lists. Returns the
  list of inserted ids — duplicates are dropped and rows are inserted in
  `correlation_key` order, so the list has no positional mapping to `entries`.
  """
  def insert_all(fsm_module, entries, opts \\ []) do
    Queries.insert_all(repo(opts), Enum.map(entries, &build_params(fsm_module, &1)))
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
    Queries.deliver_signal(
      repo(opts),
      target,
      to_string(name),
      Jason.encode!(payload),
      opts[:dedup_key]
    )
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
      concurrency_key: opts[:concurrency_key],
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

  # Resolve the repo for an API call: an explicit `:repo` wins, else the config
  # of the `:name`d instance (default `GenDurable`).
  defp repo(opts), do: opts[:repo] || config(Keyword.get(opts, :name, GenDurable)).repo

  defp config(name) do
    :persistent_term.get({GenDurable, name}, nil) ||
      raise ArgumentError,
            "no GenDurable instance named #{inspect(name)} — is the engine started?"
  end
end
