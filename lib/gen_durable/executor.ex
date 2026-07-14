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

  Inline execution (run-ahead): an `inline_execution:` FSM's `:next` runs the
  next step in THIS task on the same claim instead of requeuing — a guarded
  `continue_next` commit (keeps the row `executing`; durability unchanged) then
  an out-of-band `Limiter.admit` for the next step's tokens; denied ⇒ the row
  requeues and the picker admits it. The chained step gets a fresh inbox/children
  snapshot (re-enriched per step), so its `ctx` matches what a re-pick would hand
  it. Disabled when `run/3`'s `sched` is `nil` (the `Testing.drain/1` path).

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
  Returns the last committed outcome tuple in its serialized (DB-ready) form.

  `sched` is the scheduler pid (for the inline-continuation slot handoff); `nil` — the
  `Testing.drain/1` path — disables inline chaining, so each `run/2` commits exactly one
  step, as before.
  """
  def run(config, job), do: run(config, job, nil)

  def run(config, job, sched) do
    # Deliberately OUTSIDE the guarded region: an unresolvable fsm here is most
    # likely a rolling deploy (this node does not know the module yet — another
    # node does), so the right move is the crash path (lease floor, re-pick
    # elsewhere), not a terminal :stop that would lose the instance.
    module = Registry.fetch!(config.registry, job.fsm, job.fsm_version)
    state_module = module.__gd_state__()
    run_step(config, job, module, state_module, sched)
  end

  # Run one step to a committed outcome; on an inline `:next` continuation, loop into the
  # next step in THIS worker (no requeue, no re-pick) with the same claim held.
  defp run_step(config, job, module, state_module, sched) do
    ctx = build_ctx(job, state_module)

    started = System.monotonic_time()
    {outcome, consumed} = invoke(module, state_module, ctx)
    warn_unknown_rate_limit(config, job, outcome)

    :telemetry.execute(
      [:gen_durable, :step, :stop],
      %{duration: System.monotonic_time() - started},
      %{id: job.id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
    )

    case maybe_continue(config, job, outcome, consumed, module, sched) do
      {:continue, next_job} -> run_step(config, next_job, module, state_module, sched)
      :done -> outcome
    end
  end

  defp build_ctx(job, state_module) do
    all = job.signals
    awaits = job.awaits || []

    %Context{
      id: job.id,
      fsm: job.fsm,
      fsm_version: job.fsm_version,
      step: job.step,
      attempt: job.attempt,
      state: State.from_db(state_module, job.state),
      # awaited: the subset the step's await named (delivered + consumed on progress
      # by these exact ids); all: the full inbox, for the raw case.
      awaited: Enum.filter(all, &(&1.name in awaits)),
      all: all,
      childs: job.childs
    }
  end

  # --- inline continuation (run-ahead) ---------------------------------------
  #
  # A `:next` whose FSM (or this transition's opts) enables `inline_execution` tries to run
  # the next step in place. The commit that keeps the row `executing` (`continue_next`) is
  # GUARDED and runs FIRST — it doubles as the ownership proof, so the subsequent (unguarded,
  # out-of-band) `Limiter.admit` can never fire from an orphaned task onto a row a new
  # claimant owns. If the next step's rate/concurrency tokens can't be secured, the row is
  # requeued and the picker takes it — the same admission arithmetic, just not inline.
  # Everything else (terminal, await, retry, schedule_childs, or a `:next` with inlining off)
  # commits through the normal `apply_outcome` path. `sched == nil` (Testing) never inlines.

  defp maybe_continue(
         config,
         job,
         {:next, step, state_json, opts} = outcome,
         consumed,
         module,
         sched
       )
       when is_pid(sched) do
    if inline?(opts, module) do
      attempt_inline(config, job, step, state_json, opts, consumed, sched, outcome)
    else
      apply_outcome(config, config.repo, job, outcome, consumed)
      :done
    end
  end

  defp maybe_continue(config, job, outcome, consumed, _module, _sched) do
    apply_outcome(config, config.repo, job, outcome, consumed)
    :done
  end

  defp inline?(%{inline_execution: nil}, module), do: module.__gd_inline_execution__()
  defp inline?(%{inline_execution: flag}, _module), do: flag

  defp attempt_inline(config, job, step, state_json, opts, consumed, sched, outcome) do
    plan = conc_plan(opts.concurrency_key, job, config)

    # Phase 1: guarded commit that KEEPS the row executing (durability + ownership proof).
    case Queries.continue_next(
           config.repo,
           job.id,
           job.worker,
           step,
           state_json,
           consumed,
           opts.rate_limit,
           opts.weight,
           plan.set_key,
           plan.key_value,
           plan.set_shard,
           plan.shard_value,
           config.lease_ttl_ms
         ) do
      :stale ->
        # Lease expired mid-step and the row was reclaimed — drop, the new claimant redoes
        # it (at-least-once), exactly like a stale outcome. Nothing admitted yet, nothing leaks.
        emit_stale(job, outcome)
        :done

      :contended ->
        # An unconfigured new concurrency_key is already held — requeue and let the picker's
        # K=1 arbiter serialize it. Nothing committed by continue_next (it rolled back).
        emit_yield(job, outcome, :contended)
        apply_outcome(config, config.repo, job, outcome, consumed)
        :done

      :ok ->
        finish_inline(config, job, step, state_json, opts, consumed, plan, sched, outcome)
    end
  end

  # Phase 2: secure the next step's tokens out-of-band. The guarded commit already landed, so
  # `Limiter.admit` is safe (we still own the row). Denied ⇒ requeue (executing → runnable via
  # the normal outcome path, which also credits the old slot). Admitted ⇒ hand off and loop.
  defp finish_inline(config, job, step, state_json, opts, consumed, plan, sched, outcome) do
    case admit_inline(config, job, plan, opts) do
      :denied ->
        emit_yield(job, outcome, :throttled)
        apply_outcome(config, config.repo, job, outcome, consumed)
        :done

      {:admitted, new_slot} ->
        # Return the old concurrency slot iff the key actually changed (a `:keep` chain holds
        # the same slot across steps — crediting it would double-count).
        if plan.credit_old and not is_nil(job.slot),
          do: GenDurable.Limiter.credit(config.limiter, [job.slot])

        # Keep the scheduler's in-flight slot current so its heartbeat renews the RIGHT slot
        # (a lease-native backend prunes an unrenewed one). Only when it changed.
        if new_slot != job.slot, do: send(sched, {:slot_swap, job.id, new_slot})

        emit_continue(job, outcome)
        {:continue, build_next_job(config.repo, job, step, state_json, plan, new_slot)}
    end
  end

  # Call the limiter only when the next step actually needs a token: a fresh rate token, or a
  # slot for a CONFIGURED new concurrency key. `:keep`, a key release, and an unconfigured new
  # key (K=1, handled in-band by continue_next) need no admission.
  defp admit_inline(config, job, plan, opts) do
    rate = if opts.rate_limit, do: {opts.rate_limit, opts.weight}

    if is_nil(rate) and is_nil(plan.admit_conc) do
      {:admitted, resolve_slot(plan, job, nil)}
    else
      entry = %{id: job.id, conc: plan.admit_conc, rate: rate}
      %{admitted: admitted, denied: denied} = GenDurable.Limiter.admit(config.limiter, [entry])

      if denied == [] do
        {:admitted, resolve_slot(plan, job, admitted |> Map.new() |> Map.get(job.id))}
      else
        :denied
      end
    end
  end

  # The slot the NEXT step will hold: keep the current one, none, or the one admit just drew.
  defp resolve_slot(%{new_slot: :keep}, job, _admitted), do: job.slot
  defp resolve_slot(%{new_slot: :none}, _job, _admitted), do: nil
  defp resolve_slot(%{new_slot: :from_admit}, _job, admitted), do: admitted

  # Directives for `continue_next` + admission, from the next step's `concurrency_key`:
  #   :keep — hold the current key/shard/slot, admit nothing.
  #   nil   — release the key (clear key + shard), credit the old slot, admit nothing.
  #   configured new key — set key + PROVISIONAL shard (out of K=1); admit stamps the real
  #                        shard and draws the slot; credit the old one.
  #   unconfigured new key — set key + NULL shard (re-enters the K=1 arbiter, enforced by
  #                          continue_next); no slot, credit the old one.
  defp conc_plan(:keep, _job, _config),
    do: %{
      set_key: false,
      key_value: nil,
      set_shard: false,
      shard_value: nil,
      admit_conc: nil,
      credit_old: false,
      new_slot: :keep
    }

  defp conc_plan(nil, _job, _config),
    do: %{
      set_key: true,
      key_value: nil,
      set_shard: true,
      shard_value: nil,
      admit_conc: nil,
      credit_old: true,
      new_slot: :none
    }

  defp conc_plan(key, _job, config) when is_binary(key) do
    name = key |> String.split(":", parts: 2) |> hd()

    if MapSet.member?(config.concurrency_limit_names, name) do
      %{
        set_key: true,
        key_value: key,
        set_shard: true,
        shard_value: 0,
        admit_conc: key,
        credit_old: true,
        new_slot: :from_admit
      }
    else
      %{
        set_key: true,
        key_value: key,
        set_shard: true,
        shard_value: nil,
        admit_conc: nil,
        credit_old: true,
        new_slot: :none
      }
    end
  end

  # The next step's job: same claim (id/worker), new step/state/slot/key, attempt reset, and a
  # FRESH inbox/children snapshot — identical to what a re-pick would have handed the step.
  defp build_next_job(repo, job, step, state_json, plan, new_slot) do
    next_key = if plan.set_key, do: plan.key_value, else: job.concurrency_key

    base = %{
      job
      | step: step,
        state: state_json,
        attempt: 0,
        awaits: [],
        slot: new_slot,
        concurrency_key: next_key
    }

    [enriched] = Queries.enrich_jobs(repo, [base])
    enriched
  end

  defp emit_continue(job, outcome) do
    :telemetry.execute(
      [:gen_durable, :run_ahead, :continue],
      %{count: 1},
      %{id: job.id, fsm: job.fsm, from: job.step, to: elem(outcome, 1)}
    )
  end

  defp emit_yield(job, outcome, reason) do
    :telemetry.execute(
      [:gen_durable, :run_ahead, :yield],
      %{count: 1},
      %{id: job.id, fsm: job.fsm, step: job.step, to: elem(outcome, 1), reason: reason}
    )
  end

  defp emit_stale(job, outcome) do
    :telemetry.execute(
      [:gen_durable, :outcome, :stale],
      %{count: 1},
      %{id: job.id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
    )
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
  defp apply_outcome(config, repo, job, outcome, consumed) do
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

    case result do
      :stale ->
        :telemetry.execute(
          [:gen_durable, :outcome, :stale],
          %{count: 1},
          %{id: id, fsm: job.fsm, step: job.step, kind: Outcome.kind(outcome)}
        )

      committed ->
        # The row left `executing` — credit its concurrency slot back, out-of-band
        # (replaces the old `credit_gate` rider). Only for a CONFIGURED gate (a slot was
        # drawn); plain / unconfigured K=1 rows carry none. Skipped on `:stale` above: the
        # row was reclaimed and its slot belongs to the new claimant.
        credit_slot(config, job)

        # The committed outcome may have settled the instance (terminal or
        # parked) — nudge this node's awaiters. A hint only: awaiters re-check
        # the row, so over-nudging (e.g. an empty schedule_childs that left the
        # row runnable) costs one read, never correctness.
        if Outcome.kind(outcome) in [:await, :done, :stop, :schedule_childs] do
          GenDurable.Await.notify_local(config.name, id)
        end

        # Cross-queue wakes ride the poke transport like inserts do. Same-queue
        # runnable rows need none of this — the scheduler that ran this step
        # refills on its completion.
        case outcome do
          # freshly-inserted children may live in other queues
          {:schedule_childs, _next, child_params, _state} ->
            GenDurable.Poke.dispatch_rows(config.name, child_params)

          _ ->
            :ok
        end

        # a parent whose join this terminal completion satisfied (its queue
        # rides back in the outcome's result — see Queries.complete_done)
        case committed do
          {:ok, parent_queue} when is_binary(parent_queue) ->
            GenDurable.Poke.dispatch(config.name, parent_queue)

          _ ->
            :ok
        end
    end

    :ok
  end

  # Credit the concurrency slot the job held (the opaque handle the limiter drew at pick time)
  # back to its backend. A configured gate carries a slot; plain / unconfigured K=1 rows carry
  # none (`nil`).
  defp credit_slot(config, %{slot: slot}) when not is_nil(slot),
    do: GenDurable.Limiter.credit(config.limiter, [slot])

  defp credit_slot(_config, _job), do: :ok

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(reason), do: inspect(reason)

  # A child spec is `{FsmModule, insert_opts}` or a bare `FsmModule`.
  defp child_to_params({module, opts}), do: GenDurable.build_params(module, opts)
  defp child_to_params(module) when is_atom(module), do: GenDurable.build_params(module, [])
end
