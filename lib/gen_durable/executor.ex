defmodule GenDurable.Executor do
  @moduledoc """
  Runs one picked instance to a committed outcome.

  Resolves the FSM module, loads the jsonb state into the FSM's struct, and
  calls `step/2` under `try`. The signal-inbox and children snapshots ride in
  the job itself — batch-loaded by the pick, not fetched per step. A raised
  exception routes to `handle/2`, and so does an uncaught `throw` (as
  `{:throw, value}`); if `handle/2` itself raises, the instance fails. A worker
  *crash* (a bare `exit`, a kill — no return at all) is **not** handled here —
  it is the reaper's job, the at-least-once safety floor.

  Signal consumption: a step sees the awaited subset as `ctx.awaited`
  (only the signals whose name is in the set it parked on) and the whole inbox as
  `ctx.all`. On a progressing outcome the engine deletes exactly the `ctx.awaited`
  ids the step received — latecomers and never-awaited signals survive; a terminal
  outcome clears the whole inbox (cleanup); `:retry`/`:await` delete nothing.
  Deletion happens in SQL, by id.

  Outcomes commit only while this worker still owns the claim (`locked_by` +
  `status = 'executing'` guard in every `complete_*`): an orphaned task whose
  lease expired and whose row was reclaimed gets its late outcome dropped —
  observable as `[:gen_durable, :outcome, :stale]` — and the current claimant
  redoes the step (at-least-once).
  """

  alias GenDurable.{Context, Outcome, Queries, Registry, State}

  @doc """
  Execute `job` (a map returned by `Queries.pick/5`). `config` is `%{repo: ...}`.
  Returns the outcome tuple in its serialized (DB-ready) form.
  """
  def run(config, job) do
    repo = config.repo
    # Deliberately OUTSIDE the guarded region: an unresolvable fsm here is most
    # likely a rolling deploy (this node does not know the module yet — another
    # node does), so the right move is the crash path (lease floor, re-pick
    # elsewhere), not a terminal :stop that would lose the instance.
    module = Registry.fetch!(config.registry, job.fsm, job.fsm_version)
    state_module = module.__gd_state__()

    state = State.from_db(state_module, job.state)
    all = job.signals
    awaits = job.awaits || []

    ctx = %Context{
      id: job.id,
      fsm: job.fsm,
      fsm_version: job.fsm_version,
      step: job.step,
      attempt: job.attempt,
      state: state,
      # awaited: the subset the step's await named (delivered + consumed on progress
      # by these exact ids); all: the full inbox, for the raw case.
      awaited: Enum.filter(all, &(&1.name in awaits)),
      all: all,
      childs: job.childs
    }

    started = System.monotonic_time()
    {outcome, consumed} = invoke(module, state_module, ctx)
    apply_outcome(repo, job, outcome, consumed)
    warn_unknown_rate_limit(config, job, outcome)

    :telemetry.execute(
      [:gen_durable, :step, :stop],
      %{duration: System.monotonic_time() - started},
      %{id: job.id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
    )

    outcome
  end

  # A `:next` naming a rate-limit whose name has no configured policy: the row
  # would stall (no bucket). Emit telemetry so it is observable, not silent.
  defp warn_unknown_rate_limit(config, job, {:next, _step, _state, %{rate_limit: rl}})
       when is_binary(rl) do
    name = rl |> String.split(":", parts: 2) |> hd()
    names = Map.get(config, :rate_limit_names, MapSet.new())

    unless MapSet.member?(names, name) do
      :telemetry.execute(
        [:gen_durable, :rate_limit, :unknown],
        %{count: 1},
        %{key: rl, name: name, fsm: job.fsm, step: job.step}
      )
    end
  end

  defp warn_unknown_rate_limit(_config, _job, _outcome), do: :ok

  # step/2, falling back to handle/2 on a caught exception or an uncaught
  # `throw` (delivered as `{:throw, value}` — a throw is a controlled non-local
  # return, not a crash; without the catch it would kill the Task and cost a
  # full lease_ttl through the reaper). A bare `exit` is deliberately NOT caught:
  # it is a process-level event and takes the crash path (reaper, at-least-once
  # floor). The outcome is SERIALIZED inside the guarded region too: a
  # deterministic user-data error (an unencodable `:done` result or state, a bad
  # child spec) must route to `handle/2` — outside the guard it would crash the
  # Task and loop forever through the reaper, one lease per cycle, with no
  # terminal state. Returns the serialized outcome plus the signal ids to
  # consume on a progressing outcome (the awaited snapshot the step received;
  # none on the handle path, since the step failed).
  defp invoke(module, state_module, ctx) do
    outcome =
      module.step(ctx.step, ctx)
      |> Outcome.validate!()
      |> serialize(state_module)

    {outcome, Enum.map(ctx.awaited, & &1.id)}
  rescue
    reason -> {handle(module, state_module, ctx, reason), []}
  catch
    :throw, value -> {handle(module, state_module, ctx, {:throw, value}), []}
  end

  defp handle(module, state_module, ctx, reason) do
    handle_ctx = %{ctx | awaited: [], all: [], childs: []}

    try do
      module.handle(reason, handle_ctx)
      |> Outcome.validate!()
      |> serialize(state_module)
    rescue
      e2 -> {:stop, format_reason(e2)}
    end
  end

  # A validated outcome in its DB-ready form: state/result encoded to JSON text,
  # child specs expanded to insert params, stop reasons formatted. Raises on
  # unserializable user data — deliberately called inside invoke's guarded
  # region, so the error reaches `handle/2` like any other step failure.
  defp serialize(outcome, state_module) do
    case outcome do
      {:next, step, state, opts} ->
        {:next, step, State.to_db(state_module, state), opts}

      {:retry, state, delay} ->
        {:retry, State.to_db(state_module, state), delay}

      {:await, names, next_step, state, opts} ->
        {:await, names, next_step, State.to_db(state_module, state), opts}

      {:schedule_childs, next_step, children, state} ->
        child_params = Enum.map(children, &child_to_params/1)
        {:schedule_childs, next_step, child_params, State.to_db(state_module, state)}

      {:done, result} ->
        {:done, Jason.encode!(result)}

      {:stop, reason} ->
        {:stop, format_reason(reason)}
    end
  end

  # `consumed` is the awaited-signal ids to delete on a progressing outcome; terminal
  # outcomes delete the whole inbox regardless (cleanup), :retry/:await delete nothing.
  # An :await passes them as the PRESENTED set instead: the park's recheck must not
  # re-wake on a signal the step already saw and chose to re-await (see
  # Queries.complete_await). Every outcome carries the claim's worker id (ownership
  # guard): a `:stale` return means the lease expired and the row was reclaimed
  # while this step ran — the outcome is dropped (the new claimant redoes the
  # work) and the drop is made observable via telemetry.
  defp apply_outcome(repo, job, outcome, consumed) do
    %{id: id, worker: worker} = job

    result =
      case outcome do
        {:next, step, state_json, opts} ->
          Queries.complete_next(
            repo,
            id,
            worker,
            step,
            state_json,
            consumed,
            opts.rate_limit,
            opts.weight,
            opts.concurrency_key
          )

        {:retry, state_json, delay} ->
          Queries.complete_retry(repo, id, worker, state_json, delay)

        {:await, names, next_step, state_json, opts} ->
          Queries.complete_await(
            repo,
            id,
            worker,
            state_json,
            names,
            next_step,
            consumed,
            opts.timeout
          )

        {:schedule_childs, next_step, child_params, state_json} ->
          Queries.complete_schedule_childs(
            repo,
            id,
            worker,
            next_step,
            state_json,
            child_params,
            consumed
          )

        {:done, result_json} ->
          Queries.complete_done(repo, id, worker, result_json)

        {:stop, reason_text} ->
          Queries.complete_stop(repo, id, worker, reason_text)
      end

    if result == :stale do
      :telemetry.execute(
        [:gen_durable, :outcome, :stale],
        %{count: 1},
        %{id: id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
      )
    end

    :ok
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(reason), do: inspect(reason)

  # A child spec is `{FsmModule, insert_opts}` or a bare `FsmModule`.
  defp child_to_params({module, opts}), do: GenDurable.build_params(module, opts)
  defp child_to_params(module) when is_atom(module), do: GenDurable.build_params(module, [])
end
