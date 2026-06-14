defmodule GenDurable.Executor do
  @moduledoc """
  Runs one picked instance to a committed outcome (spec §3/§4).

  Resolves the FSM module, loads the jsonb state into the FSM's struct, snapshots
  the signal inbox, calls `step/2` under `try`. A raised exception routes to
  `handle/2` (spec §4.2); if `handle/2` itself raises, the instance fails. A
  worker *crash* (no return at all) is **not** handled here — it is the reaper's
  job (spec §4.3), the at-least-once safety floor.

  Signal consumption is name-scoped (spec §5): on a progressing outcome the
  outcome transaction deletes exactly the inbox signals whose name matches the
  row's `awaits` (the name the step was parked on), and clears `awaits`. Signals
  for other names — and signals on a never-awaited instance — survive. The
  snapshot in `ctx.signals` is read-only; deletion happens in SQL, not by id.
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
    signals = Queries.load_signals(repo, job.id)
    childs = Queries.load_childs(repo, job.id)

    ctx = %Context{
      id: job.id,
      fsm: job.fsm,
      fsm_version: job.fsm_version,
      step: job.step,
      attempt: job.attempt,
      state: state,
      signals: signals,
      childs: childs
    }

    started = System.monotonic_time()
    outcome = invoke(module, ctx)
    apply_outcome(repo, state_module, job.id, outcome)

    :telemetry.execute(
      [:gen_durable, :step, :stop],
      %{duration: System.monotonic_time() - started},
      %{id: job.id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
    )

    outcome
  end

  # step/2, falling back to handle/2 on a caught exception.
  defp invoke(module, ctx) do
    Outcome.validate!(module.step(ctx.step, ctx))
  rescue
    reason ->
      handle_ctx = %{ctx | signals: [], childs: []}

      try do
        Outcome.validate!(module.handle(reason, handle_ctx))
      rescue
        e2 -> {:stop, e2}
      end
  end

  defp apply_outcome(repo, state_module, id, outcome) do
    case outcome do
      {:next, step, state} ->
        Queries.complete_next(repo, id, step, State.to_db(state_module, state))

      {:replay, state, delay} ->
        Queries.complete_replay(repo, id, State.to_db(state_module, state), delay)

      {:await, name, state} ->
        Queries.complete_await(repo, id, State.to_db(state_module, state), name)

      {:schedule_childs, next_step, children, state} ->
        child_params = Enum.map(children, &child_to_params/1)

        Queries.complete_schedule_childs(
          repo,
          id,
          next_step,
          State.to_db(state_module, state),
          child_params
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
