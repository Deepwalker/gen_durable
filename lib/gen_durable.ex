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
    * `[:gen_durable, :partition, :contended]` — a partition advisory lock was
      contended (a row was handed back). Measurements `%{count}`; metadata
      `%{id, fsm, partition_key}`.
    * `[:gen_durable, :reaper, :reaped]` — expired leases reclaimed. Measurements
      `%{count}`; metadata `%{ids}`.
    * `[:gen_durable, :gc, :swept]` — terminal rows deleted by a GC sweep.
      Measurements `%{count}`; metadata `%{}`.

  See `gen_durable_spec.md` (normative) and `gen_durable_plan.md` (roadmap).
  """

  alias GenDurable.{Queries, State}

  # --- engine lifecycle ------------------------------------------------------

  defdelegate start_link(opts), to: GenDurable.Supervisor

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, GenDurable.Supervisor),
      start: {GenDurable.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  # --- public API ------------------------------------------------------------

  @doc """
  Enqueue one FSM instance. Options: `:state` (alias `:args`, the job-form name),
  `:step` (default the FSM's `:initial`), `:queue`, `:priority`, `:partition_key`,
  `:correlation_key`, `:correlation_scope`, and scheduling — `:eligible_at` (a `DateTime`),
  or the sugar `:schedule_at` (a `DateTime`) / `:schedule_in` (milliseconds from now).
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
    repo = opts[:repo] || config().repo
    Queries.insert(repo, build_params(fsm_module, opts))
  end

  @doc """
  Batch-enqueue instances in a single statement (dedup via the partial unique
  index). `entries` is a list of per-instance option keyword lists. Returns the
  list of inserted ids (duplicates dropped).
  """
  def insert_all(fsm_module, entries, opts \\ []) do
    repo = opts[:repo] || config().repo
    Queries.insert_all(repo, Enum.map(entries, &build_params(fsm_module, &1)))
  end

  @doc """
  Deliver a durable signal to an instance (spec §5). Wakes the instance only on
  a name match. `:dedup_key` (default `nil`) makes redelivery idempotent.

  `target` is either the internal id (an integer) or a `:correlation_key` (a string) set
  at insert. Addressing by `correlation_key` resolves to the single instance currently
  occupying it (per its `:correlation_scope`); if none exists it returns `{:error, :no_target}`
  (a freed/terminal key can no longer be woken, and a signal is not held for an instance
  that does not exist yet). Returns `:ok` otherwise.
  """
  def signal(target, name, payload \\ %{}, opts \\ []) do
    repo = opts[:repo] || config().repo

    Queries.deliver_signal(
      repo,
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
      partition_key: opts[:partition_key],
      correlation_key: opts[:correlation_key],
      correlation_scope: correlation_scope(opts),
      eligible_at: resolve_eligible_at(opts)
    }
  end

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

  defp config, do: :persistent_term.get({GenDurable, :config})
end
