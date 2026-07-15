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
  continue commit through the flush that KEEPS the row `executing` (durability
  unchanged; the Task blocks until it lands) then an out-of-band `Limiter.admit`
  for the next step's tokens; denied ⇒ the row requeues and the picker admits it.
  The chained step gets a fresh inbox/children snapshot (re-enriched per step), so
  its `ctx` matches what a re-pick would hand it. Disabled when `run/3`'s `sched`
  is `nil` (the `Testing.drain/1` path).

  Signal consumption: a step sees the awaited subset as `ctx.awaited`
  (only the signals whose name is in the set it parked on) and the whole inbox as
  `ctx.all`. On a progressing outcome the engine deletes exactly the `ctx.awaited`
  ids the step received — latecomers and never-awaited signals survive; a terminal
  outcome clears the whole inbox (cleanup); `:retry`/`:await` delete nothing.
  Deletion happens in SQL, by id.

  Outcomes commit only while this worker still owns the claim (`locked_by` +
  `status = 'executing'` guard in the flush): an orphaned task whose
  lease expired and whose row was reclaimed gets its late outcome dropped —
  observable as `[:gen_durable, :outcome, :stale]` — and the current claimant
  redoes the step (at-least-once).
  """

  alias GenDurable.{Context, Flusher, Outcome, Queries, Registry, State}

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
  # the next step in place. The commit that keeps the row `executing` (a continue flush entry) is
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

    if plan.inline_ok do
      # Phase 1: guarded commit that KEEPS the row executing (durability + ownership proof),
      # through the SAME batched flush as every other commit — the Task blocks until it lands.
      case Flusher.commit(
             job.flusher,
             continue_entry(job, step, state_json, opts, plan, consumed, config.lease_ttl_ms)
           ) do
        :stale ->
          # Lease expired mid-step and the row was reclaimed — drop, the new claimant redoes
          # it (at-least-once). Nothing admitted yet, nothing leaks.
          emit_stale(job, outcome)
          :done

        {:error, _reason} ->
          # The flush transaction failed; the row stays executing for the reaper.
          :done

        :committed ->
          finish_inline(config, job, step, state_json, opts, consumed, plan, sched, outcome)
      end
    else
      # An unconfigured new concurrency_key can collide on the K=1 arbiter, which in a batched
      # flush would abort the WHOLE batch — so never inline it; requeue and let the picker's
      # arbiter serialize the key (as a fused pick would).
      emit_yield(job, outcome, :contended)
      apply_outcome(config, config.repo, job, outcome, consumed)
      :done
    end
  end

  # The inline-continue flush entry: like a :next, but KEEPS the row executing
  # (keep_lock, lease extended) so the same Task runs the next step in place. Shard/key
  # follow the plan (provisional 0 for a configured new key; the subsequent admit stamps
  # the real shard).
  defp continue_entry(job, step, state_json, opts, plan, consumed, lease_ttl_ms) do
    %{
      base_entry(job)
      | status: "executing",
        keep_lock: true,
        # The row KEEPS its concurrency slot across the inline step (it stays executing) — so the
        # flush must NOT credit it. Slot lifecycle on an inline chain is handled explicitly in
        # `finish_inline` (credit the old slot only on a key change). base_entry set slot: job.slot.
        slot: nil,
        lease_ttl_ms: lease_ttl_ms,
        set_step: true,
        step: step,
        set_state: true,
        state: state_json,
        set_attempt: true,
        attempt: 0,
        set_eligible: true,
        delay_ms: 0,
        clear_awaits: true,
        set_rate: true,
        rate_limit: opts.rate_limit,
        weight: opts.weight * 1.0,
        set_ck: plan.set_key,
        ck_value: plan.key_value,
        set_shard: plan.set_shard,
        shard_value: plan.shard_value,
        consumed_ids: consumed
    }
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
  # key (K=1, handled in-band by the flush continue) need no admission.
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

  # Directives for the continue flush entry + admission, from the next step's `concurrency_key`:
  #   :keep — hold the current key/shard/slot, admit nothing.
  #   nil   — release the key (clear key + shard), credit the old slot, admit nothing.
  #   configured new key — set key + PROVISIONAL shard (out of K=1); admit stamps the real
  #                        shard and draws the slot; credit the old one.
  #   unconfigured new key — set key + NULL shard (re-enters the K=1 arbiter, enforced by
  #                          the flush continue); no slot, credit the old one.
  defp conc_plan(:keep, _job, _config),
    do: %{
      set_key: false,
      key_value: nil,
      set_shard: false,
      shard_value: nil,
      admit_conc: nil,
      credit_old: false,
      new_slot: :keep,
      inline_ok: true
    }

  defp conc_plan(nil, _job, _config),
    do: %{
      set_key: true,
      key_value: nil,
      set_shard: true,
      shard_value: nil,
      admit_conc: nil,
      credit_old: true,
      new_slot: :none,
      inline_ok: true
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
        new_slot: :from_admit,
        inline_ok: true
      }
    else
      # A NEW unconfigured key would go NULL-shard into the K=1 arbiter — unsafe to inline
      # through a shared flush batch (a collision aborts the batch). Route it through a requeue.
      %{
        set_key: true,
        key_value: key,
        set_shard: true,
        shard_value: nil,
        admit_conc: nil,
        credit_old: true,
        new_slot: :none,
        inline_ok: false
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
  # the flush await recheck). Every outcome carries the claim's worker id (ownership
  # guard): a `:stale` return means the lease expired and the row was reclaimed
  # while this step ran — the outcome is dropped (the new claimant redoes the
  # work) and the drop is made observable via telemetry.
  # Router: the four kinds that LEAVE `executing` (:next, :retry, :done, :stop)
  # commit through the batched flush (group commit); :await and :schedule_childs
  # carry per-row riders (park+recheck, children insert) and stay on their own
  # single-statement path for now.
  # Every outcome now commits through the batched flush.
  defp apply_outcome(config, _repo, job, outcome, consumed) do
    commit_batched(config, job, outcome, consumed)
  end

  # Hand the row's entry to the queue's flusher and block until the group commit
  # lands (`commit_before_proceed` is preserved — the Task waits for the write).
  # With no flusher — the `Testing.drain/1` path — flush synchronously. The flush
  # runs every side effect (credit, notify_local, parent poke) in one place, so
  # there is nothing to do here on success. `:stale` ⇒ the row was reclaimed; the
  # new claimant redoes the step (at-least-once), the drop is telemetry.
  defp commit_batched(config, job, outcome, consumed) do
    entry = build_entry(job, outcome, consumed)

    result =
      case Map.get(job, :flusher) do
        nil ->
          committed = Flusher.commit_sync(config, [entry])
          if MapSet.member?(committed, entry.id), do: :committed, else: :stale

        flusher ->
          Flusher.commit(flusher, entry)
      end

    case result do
      :committed -> :ok
      :stale -> emit_stale(job, outcome)
      {:error, _reason} -> :ok
    end
  end

  # A flush entry mirrors a `complete_*` statement, one per outcome. Every column
  # the batched UPDATE can touch is present; `set_*` flags say which the CASE
  # applies (unset columns keep their current value). `slot`/`notify` drive the
  # post-flush side effects. Terminal outcomes leave `consumed_ids` empty — the
  # flush drops their whole inbox by status; :retry keeps its awaited signals.
  defp base_entry(job) do
    %{
      kind: :state,
      id: job.id,
      worker: job.worker,
      slot: job.slot,
      notify: false,
      status: nil,
      attempt: 0,
      delay_ms: 0,
      # :done/:stop keep attempt and eligible_at (terminal rows are never repicked);
      # :next/:retry flip these on.
      set_attempt: false,
      set_eligible: false,
      # inline-continue keeps the claim (keep_lock) and sets a provisional/kept shard;
      # every other kind leaves executing (set_shard true → NULL, keep_lock false).
      keep_lock: false,
      set_shard: true,
      shard_value: nil,
      lease_ttl_ms: 0,
      set_step: false,
      step: nil,
      set_state: false,
      state: nil,
      set_result: false,
      result: nil,
      set_error: false,
      error: nil,
      clear_awaits: false,
      set_rate: false,
      rate_limit: nil,
      weight: 1.0,
      set_ck: false,
      ck_value: nil,
      consumed_ids: []
    }
  end

  defp build_entry(job, {:next, step, state_json, opts}, consumed) do
    {set_ck, ck_value} =
      if opts.concurrency_key == :keep, do: {false, nil}, else: {true, opts.concurrency_key}

    %{
      base_entry(job)
      | status: "runnable",
        set_attempt: true,
        attempt: 0,
        set_eligible: true,
        delay_ms: 0,
        set_step: true,
        step: step,
        set_state: true,
        state: state_json,
        clear_awaits: true,
        set_rate: true,
        rate_limit: opts.rate_limit,
        weight: opts.weight * 1.0,
        set_ck: set_ck,
        ck_value: ck_value,
        consumed_ids: consumed
    }
  end

  defp build_entry(job, {:retry, state_json, delay}, _consumed) do
    # Same step, keeps awaits and rate_limit; attempt bumps, eligible_at delays.
    %{
      base_entry(job)
      | status: "runnable",
        set_state: true,
        state: state_json,
        set_attempt: true,
        attempt: job.attempt + 1,
        set_eligible: true,
        delay_ms: delay
    }
  end

  defp build_entry(job, {:done, result_json}, _consumed) do
    %{base_entry(job) | status: "done", set_result: true, result: result_json, clear_awaits: true, notify: true}
  end

  defp build_entry(job, {:stop, reason_text}, _consumed) do
    %{base_entry(job) | status: "failed", set_error: true, error: reason_text, clear_awaits: true, notify: true}
  end

  # :await parks on `names` and transitions to `next_step`. `consumed` here is the
  # PRESENTED set (the awaited ids the step already saw) — the recheck must not
  # re-wake on those. Releases the slot (park leaves executing) and notifies
  # awaiters (the instance settled).
  defp build_entry(job, {:await, names, next_step, state_json, opts}, presented) do
    Map.merge(base_entry(job), %{
      kind: :await,
      notify: true,
      step: next_step,
      state: state_json,
      awaits_json: Jason.encode!(names),
      timeout_ms: opts.timeout,
      presented_ids: presented
    })
  end

  # :schedule_childs spawns a child batch and parks the parent on the join barrier.
  # `children` (child insert params) drive the batched insert in the flush and the
  # cross-queue child poke in the side effects; `consumed` is the parent's awaited ids.
  defp build_entry(job, {:schedule_childs, next_step, child_params, state_json}, consumed) do
    Map.merge(base_entry(job), %{
      kind: :schedule_childs,
      notify: true,
      step: next_step,
      state: state_json,
      children: child_params,
      consumed_ids: consumed
    })
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(reason), do: inspect(reason)

  # A child spec is `{FsmModule, insert_opts}` or a bare `FsmModule`.
  defp child_to_params({module, opts}), do: GenDurable.build_params(module, opts)
  defp child_to_params(module) when is_atom(module), do: GenDurable.build_params(module, [])
end
