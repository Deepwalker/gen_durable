defmodule GenDurable.Executor do
  @moduledoc """
  Runs one picked instance to a committed outcome (spec §3/§4).

  Resolves the FSM module, loads the jsonb state into the FSM's struct, snapshots
  the signal inbox, calls `step/2` under `try`. A raised exception routes to
  `handle/2` (spec §4.2); if `handle/2` itself raises, the instance fails. A
  worker *crash* (no return at all) is **not** handled here — it is the reaper's
  job (spec §4.3), the at-least-once safety floor.

  Consumed signals are deleted in the outcome transaction. Per the agreed
  semantics, the engine deletes exactly the snapshotted inbox **except** on an
  `:await` outcome, where the instance stays parked and the inbox is preserved
  (so a non-matching signal survives until its own await, spec §5).
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

    ctx = %Context{
      id: job.id,
      fsm: job.fsm,
      fsm_version: job.fsm_version,
      step: job.step,
      attempt: job.attempt,
      state: state,
      signals: signals
    }

    started = System.monotonic_time()
    outcome = invoke(module, ctx)
    apply_outcome(repo, state_module, job.id, signals, outcome)

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
      handle_ctx = %{ctx | signals: []}

      try do
        Outcome.validate!(module.handle(reason, handle_ctx))
      rescue
        e2 -> {:stop, e2}
      end
  end

  defp apply_outcome(repo, state_module, id, signals, outcome) do
    consumed = Enum.map(signals, & &1.id)

    case outcome do
      {:next, step, state} ->
        Queries.complete_next(repo, id, step, State.to_db(state_module, state), consumed)

      {:replay, state, delay} ->
        Queries.complete_replay(repo, id, State.to_db(state_module, state), delay, consumed)

      {:await, name, state} ->
        # Stay parked: keep the inbox, delete nothing.
        Queries.complete_await(repo, id, State.to_db(state_module, state), name, [])

      {:done, result} ->
        Queries.complete_done(repo, id, Jason.encode!(result), consumed)

      {:stop, reason} ->
        Queries.complete_stop(repo, id, format_reason(reason), consumed)
    end
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(reason), do: inspect(reason)
end
