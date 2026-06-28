defmodule GenDurable.Executor do
  @moduledoc """
  Runs one picked instance to a committed outcome (spec §3/§4).

  Resolves the FSM module, loads the jsonb state into the FSM's struct, snapshots
  the signal inbox, calls `step/2` under `try`. A raised exception routes to
  `handle/2` (spec §4.2); if `handle/2` itself raises, the instance fails. A
  worker *crash* (no return at all) is **not** handled here — it is the reaper's
  job (spec §4.3), the at-least-once safety floor.

  Signal consumption (spec §5): a step sees the awaited subset as `ctx.awaited`
  (only the signals whose name is in the set it parked on) and the whole inbox as
  `ctx.all`. On a progressing outcome the engine deletes exactly the `ctx.awaited`
  ids the step received — latecomers and never-awaited signals survive; a terminal
  outcome clears the whole inbox (cleanup); `:retry`/`:await` delete nothing.
  Deletion happens in SQL, by id.
  """

  alias GenDurable.{Context, Outcome, Queries, Registry, State}

  @doc """
  Execute `job` (a map returned by `Queries.pick/5`). `config` is `%{repo: ...}`.
  Returns the validated outcome tuple.
  """
  def run(config, job) do
    repo = config.repo
    module = Registry.fetch!(job.fsm, job.fsm_version)
    state_module = module.__gd_state__()

    state = State.from_db(state_module, job.state)
    all = Queries.load_signals(repo, job.id)
    childs = Queries.load_childs(repo, job.id)

    awaits = job.awaits || []

    ctx = %Context{
      id: job.id,
      fsm: job.fsm,
      fsm_version: job.fsm_version,
      step: job.step,
      attempt: job.attempt,
      state: state,
      # awaited: the subset the step's await named (delivered + consumed on progress
      # by these exact ids, spec §5); all: the full inbox, for the raw case.
      awaited: Enum.filter(all, &(&1.name in awaits)),
      all: all,
      childs: childs
    }

    started = System.monotonic_time()
    {outcome, consumed} = invoke(module, ctx)
    apply_outcome(repo, state_module, job.id, outcome, consumed)
    warn_unknown_rate_limit(config, job, outcome)

    :telemetry.execute(
      [:gen_durable, :step, :stop],
      %{duration: System.monotonic_time() - started},
      %{id: job.id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
    )

    outcome
  end

  # A `:next` naming a rate-limit whose name has no configured policy (spec §12): the row
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

  # step/2, falling back to handle/2 on a caught exception. Returns the validated
  # outcome plus the signal ids to consume on a progressing outcome (the awaited
  # snapshot the step received; none on the handle path, since the step failed).
  defp invoke(module, ctx) do
    outcome = Outcome.validate!(module.step(ctx.step, ctx))
    {outcome, Enum.map(ctx.awaited, & &1.id)}
  rescue
    reason ->
      handle_ctx = %{ctx | awaited: [], all: [], childs: []}

      outcome =
        try do
          Outcome.validate!(module.handle(reason, handle_ctx))
        rescue
          e2 -> {:stop, e2}
        end

      {outcome, []}
  end

  # `consumed` is the awaited-signal ids to delete on a progressing outcome; terminal
  # outcomes delete the whole inbox regardless (cleanup), :retry/:await delete nothing.
  defp apply_outcome(repo, state_module, id, outcome, consumed) do
    case outcome do
      {:next, step, state, opts} ->
        Queries.complete_next(
          repo,
          id,
          step,
          State.to_db(state_module, state),
          consumed,
          opts.rate_limit,
          opts.weight
        )

      {:retry, state, delay} ->
        Queries.complete_retry(repo, id, State.to_db(state_module, state), delay)

      {:await, names, next_step, state} ->
        Queries.complete_await(repo, id, State.to_db(state_module, state), names, next_step)

      {:schedule_childs, next_step, children, state} ->
        child_params = Enum.map(children, &child_to_params/1)

        Queries.complete_schedule_childs(
          repo,
          id,
          next_step,
          State.to_db(state_module, state),
          child_params,
          consumed
        )

      {:done, result} ->
        Queries.complete_done(repo, id, Jason.encode!(result))

      {:stop, reason} ->
        Queries.complete_stop(repo, id, format_reason(reason))
    end
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(reason), do: inspect(reason)

  # A child spec is `{FsmModule, insert_opts}` or a bare `FsmModule`.
  defp child_to_params({module, opts}), do: GenDurable.build_params(module, opts)
  defp child_to_params(module) when is_atom(module), do: GenDurable.build_params(module, [])
end
