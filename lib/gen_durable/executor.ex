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
  Returns the validated outcome tuple.
  """
  def run(config, job) do
    repo = config.repo
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
    {outcome, consumed} = invoke(module, ctx)
    apply_outcome(repo, state_module, job, outcome, consumed)
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
  # floor). Returns the validated outcome plus the signal ids to consume on a
  # progressing outcome (the awaited snapshot the step received; none on the
  # handle path, since the step failed).
  defp invoke(module, ctx) do
    outcome = Outcome.validate!(module.step(ctx.step, ctx))
    {outcome, Enum.map(ctx.awaited, & &1.id)}
  rescue
    reason -> {handle(module, ctx, reason), []}
  catch
    :throw, value -> {handle(module, ctx, {:throw, value}), []}
  end

  defp handle(module, ctx, reason) do
    handle_ctx = %{ctx | awaited: [], all: [], childs: []}

    try do
      Outcome.validate!(module.handle(reason, handle_ctx))
    rescue
      e2 -> {:stop, e2}
    end
  end

  # `consumed` is the awaited-signal ids to delete on a progressing outcome; terminal
  # outcomes delete the whole inbox regardless (cleanup), :retry/:await delete nothing.
  # Every outcome carries the claim's worker id (ownership guard): a `:stale`
  # return means the lease expired and the row was reclaimed while this step ran —
  # the outcome is dropped (the new claimant redoes the work) and the drop is
  # made observable via telemetry.
  defp apply_outcome(repo, state_module, job, outcome, consumed) do
    %{id: id, worker: worker} = job

    result =
      case outcome do
        {:next, step, state, opts} ->
          Queries.complete_next(
            repo,
            id,
            worker,
            step,
            State.to_db(state_module, state),
            consumed,
            opts.rate_limit,
            opts.weight
          )

        {:retry, state, delay} ->
          Queries.complete_retry(repo, id, worker, State.to_db(state_module, state), delay)

        {:await, names, next_step, state} ->
          Queries.complete_await(
            repo,
            id,
            worker,
            State.to_db(state_module, state),
            names,
            next_step
          )

        {:schedule_childs, next_step, children, state} ->
          child_params = Enum.map(children, &child_to_params/1)

          Queries.complete_schedule_childs(
            repo,
            id,
            worker,
            next_step,
            State.to_db(state_module, state),
            child_params,
            consumed
          )

        {:done, result} ->
          Queries.complete_done(repo, id, worker, Jason.encode!(result))

        {:stop, reason} ->
          Queries.complete_stop(repo, id, worker, format_reason(reason))
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
