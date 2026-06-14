defmodule GenDurable do
  @moduledoc """
  Durable FSM engine on top of Postgres + GenServer.

  Start the engine in a host supervision tree:

      {GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5]}

  FSMs are resolved from the row (the `fsm` column defaults to the module name);
  pass `:fsms` only for a custom `:name` or to keep an old `:version` running.

  Then enqueue instances and deliver signals:

      {:ok, id} = GenDurable.insert(Checkout, state: %{order: 42}, partition_key: "order:42")
      :ok = GenDurable.signal(id, "payment_confirmed", %{amount: 100}, dedup_key: "evt-7")

  ## Telemetry

  Attach to these `:telemetry` events (`[:gen_durable, …]`):

    * `[:gen_durable, :step, :stop]` — a step finished. Measurements `%{duration}`
      (native units); metadata `%{id, fsm, step, kind}` where `kind` is the outcome
      (`:next` · `:replay` · `:await` · `:schedule_childs` · `:done` · `:stop`).
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
  Enqueue one FSM instance. Options: `:state`, `:step` (default the FSM's
  `:initial`), `:queue`, `:priority`, `:partition_key`, `:unique_key`,
  `:unique_scope`, and scheduling — `:eligible_at` (a `DateTime`), or the sugar
  `:schedule_at` (a `DateTime`) / `:schedule_in` (milliseconds from now).
  Returns `{:ok, id}` or `{:error, :duplicate}`.
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
  """
  def signal(target_id, name, payload \\ %{}, opts \\ []) do
    repo = opts[:repo] || config().repo

    Queries.deliver_signal(
      repo,
      target_id,
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
      state_json: State.cast(state_module, opts[:state] || %{}),
      queue: opts[:queue] || fsm_module.__gd_queue__(),
      priority: opts[:priority] || 0,
      partition_key: opts[:partition_key],
      unique_key: opts[:unique_key],
      unique_scope: Enum.map(opts[:unique_scope] || [], &to_string/1),
      eligible_at: resolve_eligible_at(opts)
    }
  end

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
