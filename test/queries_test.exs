defmodule GenDurable.QueriesTest do
  use ExUnit.Case, async: false

  alias GenDurable.Queries
  alias GenDurable.Test.Repo

  @worker "test-worker"
  @ttl 60_000
  @live_scope ~w(runnable executing awaiting_signal awaiting_children)

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp params(overrides \\ %{}) do
    Map.merge(
      %{
        fsm: "counter",
        fsm_version: 1,
        step: "tick",
        state_json: ~s({"n":0}),
        queue: "default",
        priority: 0,
        concurrency_key: nil,
        correlation_key: nil,
        correlation_scope: [],
        rate_limit: nil,
        weight: 1,
        eligible_at: nil
      },
      overrides
    )
  end

  defp exists?(id) do
    %{rows: [[n]]} = Repo.query!("SELECT count(*) FROM gen_durable WHERE id = $1", [id])
    n == 1
  end

  # Put a row into the claimed state (executing + locked_by @worker) without going
  # through pick — the outcome queries commit only for the claim's owner.
  defp claim(id) do
    Repo.query!(
      "UPDATE gen_durable SET status = 'executing', locked_by = $2 WHERE id = $1",
      [id, @worker]
    )

    :ok
  end

  defp age_past_retention(ids) do
    Repo.query!(
      "UPDATE gen_durable SET updated_at = now() - interval '1 hour' WHERE id = ANY($1)",
      [List.wrap(ids)]
    )
  end

  defp seed(rate, burst),
    do: Queries.upsert_rate_configs(Repo, [%{name: "api", rate: rate * 1.0, burst: burst * 1.0}])

  defp tokens(key) do
    %{rows: [[t]]} =
      Repo.query!("SELECT tokens FROM gen_durable_rate_buckets WHERE key = $1", [key])

    t
  end

  @doc false
  def __fwd__(_event, measure, meta, pid), do: send(pid, {:telemetry, measure, meta})

  test "insert then pick flips to executing and returns the job" do
    {:ok, id} = Queries.insert(Repo, params())

    [job] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    assert job.id == id
    assert job.fsm == "counter"
    assert job.step == "tick"
    assert job.attempt == 0

    %{rows: [[status, locked_by]]} =
      Repo.query!("SELECT status::text, locked_by FROM gen_durable WHERE id = $1", [id])

    assert status == "executing"
    assert locked_by == @worker
  end

  test "pick honors queue filter and SKIP LOCKED batch limit" do
    {:ok, _} = Queries.insert(Repo, params(%{queue: "a"}))
    {:ok, _} = Queries.insert(Repo, params(%{queue: "b"}))

    assert [%{}] = Queries.pick(Repo, "a", 10, @worker, @ttl)
    assert [] = Queries.pick(Repo, "a", 10, @worker, @ttl)
    assert [%{}] = Queries.pick(Repo, "b", 10, @worker, @ttl)
  end

  describe "concurrency_key dedup in the picker (spec §6)" do
    test "claims at most one runnable row per concurrency_key in a batch" do
      for _ <- 1..3, do: {:ok, _} = Queries.insert(Repo, params(%{concurrency_key: "k"}))
      {:ok, other} = Queries.insert(Repo, params(%{concurrency_key: "k2"}))

      jobs = Queries.pick(Repo, "default", 10, @worker, @ttl)
      keys = Enum.map(jobs, & &1.concurrency_key)

      assert Enum.count(keys, &(&1 == "k")) == 1
      assert Enum.count(keys, &(&1 == "k2")) == 1
      assert length(jobs) == 2
      assert other in Enum.map(jobs, & &1.id)
    end

    test "skips a runnable row whose concurrency_key is already executing" do
      {:ok, a} = Queries.insert(Repo, params(%{concurrency_key: "k"}))
      {:ok, _b} = Queries.insert(Repo, params(%{concurrency_key: "k"}))

      # Claim one row for "k"; it becomes executing and holds the key.
      assert [%{id: ^a}] = Queries.pick(Repo, "default", 10, @worker, @ttl)

      # The sibling is runnable, but "k" is executing => not picked (no bounce).
      assert [] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    end

    test "NULL concurrency_key rows are never deduped against each other" do
      for _ <- 1..3, do: {:ok, _} = Queries.insert(Repo, params())

      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 3
    end
  end

  test "complete_next resets attempt and returns to runnable" do
    {:ok, id} = Queries.insert(Repo, params())
    [_job] = Queries.pick(Repo, "default", 10, @worker, @ttl)

    :ok = Queries.complete_next(Repo, id, @worker, "tick", ~s({"n":1}), [], nil, 1)

    %{rows: [[status, step, attempt, state]]} =
      Repo.query!("SELECT status::text, step, attempt, state FROM gen_durable WHERE id = $1", [id])

    assert status == "runnable"
    assert step == "tick"
    assert attempt == 0
    assert state == %{"n" => 1}
  end

  test "complete_retry bumps attempt and delays eligibility" do
    {:ok, id} = Queries.insert(Repo, params())
    [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)

    :ok = Queries.complete_retry(Repo, id, @worker, ~s({"n":0}), 50_000)

    %{rows: [[status, attempt, future]]} =
      Repo.query!(
        "SELECT status::text, attempt, eligible_at > now() FROM gen_durable WHERE id = $1",
        [id]
      )

    assert status == "runnable"
    assert attempt == 1
    assert future == true
  end

  test "complete_retry keeps awaits and consumes nothing (redo sees the same inputs)" do
    {:ok, id} = Queries.insert(Repo, params())
    [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go"], "woke", [], nil)
    :ok = Queries.deliver_signal(Repo, id, "go", ~s({"v":1}), nil)
    [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)

    :ok = Queries.complete_retry(Repo, id, @worker, ~s({}), 0)

    %{rows: [[status, awaits]]} =
      Repo.query!("SELECT status::text, awaits FROM gen_durable WHERE id = $1", [id])

    assert status == "runnable"
    # awaits is KEPT (not cleared) and the signal survives — the redo re-sees it
    assert awaits == ["go"]
    assert [%{name: "go"}] = Queries.load_signals(Repo, id)
  end

  test "complete_done and complete_stop are terminal" do
    {:ok, d} = Queries.insert(Repo, params())
    :ok = claim(d)
    :ok = Queries.complete_done(Repo, d, @worker, ~s({"ok":true}))

    %{rows: [[status, result]]} =
      Repo.query!("SELECT status::text, result FROM gen_durable WHERE id = $1", [d])

    assert status == "done"
    assert result == %{"ok" => true}

    {:ok, s} = Queries.insert(Repo, params())
    :ok = claim(s)
    :ok = Queries.complete_stop(Repo, s, @worker, "boom")

    %{rows: [[status, err]]} =
      Repo.query!("SELECT status::text, last_error FROM gen_durable WHERE id = $1", [s])

    assert status == "failed"
    assert err == "boom"
  end

  test "reaper returns expired-lease executing rows to runnable with attempt+1" do
    {:ok, id} = Queries.insert(Repo, params())
    # Pick with a negative TTL so the lease is already expired.
    [_] = Queries.pick(Repo, "default", 10, @worker, -1000)

    assert Queries.reap(Repo) == [id]

    %{rows: [[status, attempt, locked_by]]} =
      Repo.query!("SELECT status::text, attempt, locked_by FROM gen_durable WHERE id = $1", [id])

    assert status == "runnable"
    assert attempt == 1
    assert locked_by == nil
  end

  describe "signals" do
    test "deliver wakes a matching await and keeps the awaited set" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go", "stop"], "woke", [], nil)

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({"v":1}), nil)

      %{rows: [[status, awaits]]} =
        Repo.query!("SELECT status::text, awaits FROM gen_durable WHERE id = $1", [id])

      assert status == "runnable"
      # awaits (the whole set) is kept until the woken step progresses
      assert awaits == ["go", "stop"]

      assert [%{name: "go", payload: %{"v" => 1}}] = Queries.load_signals(Repo, id)
    end

    test "a signal outside the awaited set does not wake the instance" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go", "stop"], "woke", [], nil)

      :ok = Queries.deliver_signal(Repo, id, "other", ~s({}), nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      assert status == "awaiting_signal"
    end

    test "a signal that arrived before the await unparks the instance (no lost wakeup)" do
      {:ok, id} = Queries.insert(Repo, params())
      # signal arrives while the instance is still runnable (not yet awaiting)
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)

      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go", "stop"], "woke", [], nil)

      %{rows: [[status, step]]} =
        Repo.query!("SELECT status::text, step FROM gen_durable WHERE id = $1", [id])

      # EXISTS race-fix: a matching signal already present => straight to runnable,
      # parked at next_step
      assert status == "runnable"
      assert step == "woke"
    end

    test "re-awaiting with already-presented signals parks cleanly (no spin); a new one wakes" do
      {:ok, id} = Queries.insert(Repo, params())
      :ok = Queries.deliver_signal(Repo, id, "a", ~s({}), nil)
      [sig] = Queries.load_signals(Repo, id)

      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)

      # The accumulate pattern: the step was HANDED sig ("a") and re-awaits the
      # full set. The recheck must NOT re-wake on it — that would spin
      # park → flip → re-pick → re-await at full speed until the pack completes.
      :ok =
        Queries.complete_await(Repo, id, @worker, ~s({}), ["a", "b"], "collect", [sig.id], nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      assert status == "awaiting_signal"

      # a NEW matching signal still wakes the park
      :ok = Queries.deliver_signal(Repo, id, "b", ~s({}), nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      assert status == "runnable"
    end

    test "a terminal or missing target refuses the signal as :no_target" do
      {:ok, id} = Queries.insert(Repo, params())
      :ok = claim(id)
      :ok = Queries.complete_done(Repo, id, @worker, ~s({}))

      # nothing will ever read a done row's inbox — refuse instead of storing garbage
      assert {:error, :no_target} = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)
      assert Queries.load_signals(Repo, id) == []

      # a missing id is :no_target too (previously an FK violation)
      assert {:error, :no_target} = Queries.deliver_signal(Repo, id + 1_000, "go", ~s({}), nil)
    end

    test "dedup_key makes redelivery idempotent; nil dedup allows duplicates" do
      {:ok, id} = Queries.insert(Repo, params())

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), "k1")
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), "k1")
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)

      assert length(Queries.load_signals(Repo, id)) == 3
    end

    test "progress consumes exactly the passed ids; other signals survive" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go"], "woke", [], nil)

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)
      :ok = Queries.deliver_signal(Repo, id, "other", ~s({}), nil)

      go = Enum.find(Queries.load_signals(Repo, id), &(&1.name == "go"))
      # progress, consuming exactly the awaited "go" id
      :ok = claim(id)
      :ok = Queries.complete_next(Repo, id, @worker, "tick", ~s({}), [go.id], nil, 1)

      assert [%{name: "other"}] = Queries.load_signals(Repo, id)

      %{rows: [[awaits]]} = Repo.query!("SELECT awaits FROM gen_durable WHERE id = $1", [id])
      assert awaits == nil
    end

    test "a terminal outcome deletes the whole inbox (cleanup)" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.deliver_signal(Repo, id, "a", ~s({}), nil)
      :ok = Queries.deliver_signal(Repo, id, "b", ~s({}), nil)

      :ok = Queries.complete_done(Repo, id, @worker, ~s({}))

      assert Queries.load_signals(Repo, id) == []
    end
  end

  test "state, result, and signal payloads are stored as jsonb objects (no double encoding)" do
    # Regression guard: binding a JSON string to a bare ::jsonb parameter makes the
    # driver re-encode it, storing a jsonb scalar string — invisible to ->> and
    # jsonb indexes. Every write path must produce a real object.
    {:ok, id} = Queries.insert(Repo, params())

    %{rows: [[t]]} =
      Repo.query!("SELECT jsonb_typeof(state) FROM gen_durable WHERE id = $1", [id])

    assert t == "object"

    [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    :ok = Queries.complete_next(Repo, id, @worker, "tick", ~s({"n":1}), [], nil, 1)

    %{rows: [[t, n]]} =
      Repo.query!("SELECT jsonb_typeof(state), state->>'n' FROM gen_durable WHERE id = $1", [id])

    assert t == "object"
    assert n == "1"

    :ok = Queries.deliver_signal(Repo, id, "go", ~s({"v":7}), nil)

    %{rows: [[t, v]]} =
      Repo.query!(
        "SELECT jsonb_typeof(payload), payload->>'v' FROM signals WHERE target_id = $1",
        [
          id
        ]
      )

    assert t == "object"
    assert v == "7"

    [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    :ok = Queries.complete_done(Repo, id, @worker, ~s({"ok":true}))

    %{rows: [[t, ok]]} =
      Repo.query!("SELECT jsonb_typeof(result), result->>'ok' FROM gen_durable WHERE id = $1", [
        id
      ])

    assert t == "object"
    assert ok == "true"

    # batch path (unnest): same guarantee
    [bid] = Queries.insert_all(Repo, [params()])

    %{rows: [[t]]} =
      Repo.query!("SELECT jsonb_typeof(state) FROM gen_durable WHERE id = $1", [bid])

    assert t == "object"
  end

  describe "await timeout" do
    test "expire_awaits wakes only parks past their deadline, keeping awaits" do
      # expired: deadline armed at ~now
      {:ok, expired} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, expired, @worker, ~s({}), ["go"], "woke", [], 1)

      # future deadline: not to be touched
      {:ok, later} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, later, @worker, ~s({}), ["go"], "woke", [], 60_000)

      # no timeout at all: never swept
      {:ok, forever} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, forever, @worker, ~s({}), ["go"], "woke", [], nil)

      Process.sleep(5)
      assert Queries.expire_awaits(Repo) == 1

      %{rows: [[status, awaits, attempt, deadline]]} =
        Repo.query!(
          "SELECT status::text, awaits, attempt, await_deadline FROM gen_durable WHERE id = $1",
          [expired]
        )

      # a wake, not a failure: awaits kept (the woken step reads the inbox through
      # it as usual), attempt untouched, deadline cleared
      assert status == "runnable"
      assert awaits == ["go"]
      assert attempt == 0
      assert deadline == nil

      for id <- [later, forever] do
        %{rows: [[status]]} =
          Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

        assert status == "awaiting_signal"
      end
    end
  end

  describe "stale outcomes (ownership guard)" do
    test "a reclaimed row rejects every outcome from its old worker" do
      {:ok, id} = Queries.insert(Repo, params())
      # claim with an already-expired lease, then let the reaper hand it over
      [_] = Queries.pick(Repo, "default", 10, @worker, -1000)
      [^id] = Queries.reap(Repo)

      assert :stale = Queries.complete_next(Repo, id, @worker, "next", ~s({"n":9}), [], nil, 1)
      assert :stale = Queries.complete_retry(Repo, id, @worker, ~s({"n":9}), 0)

      assert :stale =
               Queries.complete_await(Repo, id, @worker, ~s({"n":9}), ["go"], "woke", [], nil)

      assert :stale = Queries.complete_stop(Repo, id, @worker, "boom")

      # the row is untouched: still runnable at the original step and state
      %{rows: [[status, step, state]]} =
        Repo.query!(
          "SELECT status::text, step, state FROM gen_durable WHERE id = $1",
          [id]
        )

      assert status == "runnable"
      assert step == "tick"
      assert state == %{"n" => 0}
    end

    test "a stale terminal outcome touches neither the inbox nor the parent join barrier" do
      {:ok, parent} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)

      :ok =
        Queries.complete_schedule_childs(
          Repo,
          parent,
          @worker,
          "join",
          ~s({}),
          [params(%{fsm: "child"})],
          []
        )

      # claim the child with an expired lease, give it a signal, let the reaper reclaim
      [%{id: child}] = Queries.pick(Repo, "default", 10, "old-worker", -1000)
      :ok = Queries.deliver_signal(Repo, child, "keep", ~s({}), nil)
      [^child] = Queries.reap(Repo)

      assert :stale = Queries.complete_done(Repo, child, "old-worker", ~s({}))

      # inbox intact, parent still parked on the join, barrier not decremented
      assert [%{name: "keep"}] = Queries.load_signals(Repo, child)

      %{rows: [[p_status, pending]]} =
        Repo.query!(
          "SELECT status::text, children_pending FROM gen_durable WHERE id = $1",
          [parent]
        )

      assert p_status == "awaiting_children"
      assert pending == 1
    end

    test "a stale schedule_childs spawns no children and consumes nothing" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, -1000)
      :ok = Queries.deliver_signal(Repo, id, "keep", ~s({}), nil)
      [sig] = Queries.load_signals(Repo, id)
      [^id] = Queries.reap(Repo)

      assert :stale =
               Queries.complete_schedule_childs(
                 Repo,
                 id,
                 @worker,
                 "join",
                 ~s({}),
                 [params(%{fsm: "child"})],
                 [sig.id]
               )

      %{rows: [[children]]} =
        Repo.query!("SELECT count(*) FROM gen_durable WHERE parent_id = $1", [id])

      assert children == 0
      assert [%{name: "keep"}] = Queries.load_signals(Repo, id)
    end
  end

  describe "correlation_key addressing" do
    test "signal by correlation_key wakes the instance occupying it" do
      {:ok, id} =
        Queries.insert(
          Repo,
          params(%{correlation_key: "order:7", correlation_scope: @live_scope})
        )

      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, @worker, ~s({}), ["go"], "woke", [], nil)

      assert :ok = Queries.deliver_signal(Repo, "order:7", "go", ~s({"v":1}), nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      assert status == "runnable"
      assert [%{name: "go", payload: %{"v" => 1}}] = Queries.load_signals(Repo, id)
    end

    test "signal by an unknown correlation_key returns {:error, :no_target}" do
      assert {:error, :no_target} = Queries.deliver_signal(Repo, "nope", "go", ~s({}), nil)
    end

    test "a key is freed on termination; not a target, reusable for a new instance" do
      {:ok, old} =
        Queries.insert(
          Repo,
          params(%{correlation_key: "order:9", correlation_scope: @live_scope})
        )

      :ok = claim(old)
      :ok = Queries.complete_done(Repo, old, @worker, ~s({}))

      # terminal ⇒ key no longer occupied ⇒ no target
      assert {:error, :no_target} = Queries.deliver_signal(Repo, "order:9", "go", ~s({}), nil)

      # the key is free again for a new live instance
      {:ok, fresh} =
        Queries.insert(
          Repo,
          params(%{correlation_key: "order:9", correlation_scope: @live_scope})
        )

      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, fresh, @worker, ~s({}), ["go"], "woke", [], nil)
      assert :ok = Queries.deliver_signal(Repo, "order:9", "go", ~s({}), nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [fresh])

      assert status == "runnable"
    end

    test "a duplicate correlation_key under the active policy is rejected as :duplicate" do
      assert {:ok, _} =
               Queries.insert(
                 Repo,
                 params(%{correlation_key: "order:1", correlation_scope: @live_scope})
               )

      assert {:error, :duplicate} =
               Queries.insert(
                 Repo,
                 params(%{correlation_key: "order:1", correlation_scope: @live_scope})
               )
    end
  end

  describe "gc" do
    test "deletes terminal rows past retention; keeps fresh terminal and non-terminal" do
      {:ok, old_done} = Queries.insert(Repo, params())
      {:ok, old_failed} = Queries.insert(Repo, params())
      {:ok, fresh_done} = Queries.insert(Repo, params())
      {:ok, runnable} = Queries.insert(Repo, params())

      for id <- [old_done, old_failed, fresh_done], do: :ok = claim(id)
      :ok = Queries.complete_done(Repo, old_done, @worker, ~s({}))
      :ok = Queries.complete_stop(Repo, old_failed, @worker, "x")
      :ok = Queries.complete_done(Repo, fresh_done, @worker, ~s({}))
      age_past_retention([old_done, old_failed])

      assert Queries.gc(Repo, 60_000, 100) == 2

      assert exists?(runnable)
      assert exists?(fresh_done)
      refute exists?(old_done)
      refute exists?(old_failed)
    end

    test "spares a terminal child whose parent is still mid-join" do
      {:ok, parent} = Queries.insert(Repo, params())

      Repo.query!(
        "UPDATE gen_durable SET status = 'awaiting_children', children_pending = 1 WHERE id = $1",
        [parent]
      )

      {:ok, child} = Queries.insert(Repo, params())
      :ok = claim(child)
      :ok = Queries.complete_done(Repo, child, @worker, ~s({}))
      Repo.query!("UPDATE gen_durable SET parent_id = $1 WHERE id = $2", [parent, child])
      age_past_retention(child)

      # parent active ⇒ child kept (it may still be read on the join)
      assert Queries.gc(Repo, 60_000, 100) == 0
      assert exists?(child)

      # parent terminal ⇒ both collectible
      Repo.query!(
        "UPDATE gen_durable SET status = 'done', updated_at = now() - interval '1 hour' WHERE id = $1",
        [parent]
      )

      assert Queries.gc(Repo, 60_000, 100) == 2
      refute exists?(child)
      refute exists?(parent)
    end

    test "batch bounds the number deleted per sweep" do
      ids =
        for _ <- 1..3 do
          {:ok, id} = Queries.insert(Repo, params())
          :ok = claim(id)
          :ok = Queries.complete_done(Repo, id, @worker, ~s({}))
          id
        end

      age_past_retention(ids)

      assert Queries.gc(Repo, 60_000, 2) == 2
      assert Queries.gc(Repo, 60_000, 2) == 1
      assert Queries.gc(Repo, 60_000, 2) == 0
    end
  end

  describe "uniqueness" do
    test "duplicate within occupied scope is rejected; NULL key never conflicts" do
      key = "k1"
      scope = ["runnable", "executing"]

      assert {:ok, _} =
               Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))

      assert {:error, :duplicate} =
               Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))

      # No correlation_key => no dedup.
      assert {:ok, _} = Queries.insert(Repo, params())
      assert {:ok, _} = Queries.insert(Repo, params())
    end

    test "leaving the occupied scope frees the key for re-insertion" do
      key = "k9"
      scope = ["runnable", "executing"]

      {:ok, id} = Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))
      # Move to a status outside the scope.
      :ok = claim(id)
      :ok = Queries.complete_done(Repo, id, @worker, ~s({}))

      assert {:ok, _} =
               Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))
    end

    test "a scope including terminal statuses reserves the key after termination (no reuse)" do
      key = "g1"
      scope = ~w(runnable executing awaiting_signal awaiting_children done failed)

      {:ok, id} = Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))
      :ok = claim(id)
      :ok = Queries.complete_done(Repo, id, @worker, ~s({}))

      # terminal, but 'done' is still in scope ⇒ key stays occupied ⇒ reuse rejected
      assert {:error, :duplicate} =
               Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))
    end

    test "batch insert dedups within the batch and against existing rows" do
      key = "k7"
      scope = ["runnable"]

      {:ok, _} = Queries.insert(Repo, params(%{correlation_key: key, correlation_scope: scope}))

      rows = [
        params(%{correlation_key: key, correlation_scope: scope}),
        params(%{correlation_key: "k8", correlation_scope: scope}),
        params(%{correlation_key: "k8", correlation_scope: scope})
      ]

      # "k7" collides with the existing row; "k8" collides within the batch.
      assert length(Queries.insert_all(Repo, rows)) == 1
    end

    test "batch insert clears the wire-protocol parameter ceiling (unnest form)" do
      # 6000 rows × 12 params would be 72000 placeholders — past the protocol's
      # 65535-parameter cap that the old per-row-placeholder form hit at ~5400
      # rows. The unnest form passes 12 arrays + bucket keys, batch size be damned.
      rows =
        for i <- 1..6_000 do
          params(%{correlation_key: "bulk:#{i}", correlation_scope: @live_scope})
        end

      ids = Queries.insert_all(Repo, rows)
      assert length(ids) == 6_000

      # the comma-joined correlation_scope survived the unnest round trip
      %{rows: [[scope]]} =
        Repo.query!("SELECT correlation_scope::text[] FROM gen_durable WHERE id = $1", [
          hd(ids)
        ])

      assert scope == @live_scope
    end
  end

  describe "rate limiting (spec §12)" do
    setup do
      Repo.query!("TRUNCATE gen_durable_rate_buckets, gen_durable_rate_configs")
      :ok
    end

    test "insert ensures a full bucket; pick grants up to budget, debits, then throttles" do
      :ok = seed(0, 5)
      for _ <- 1..10, do: {:ok, _} = Queries.insert(Repo, params(%{rate_limit: "api"}))
      # the ensure CTE created the bucket full at burst
      assert tokens("api") == 5.0

      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 5
      assert tokens("api") == 0.0
      # rate 0 ⇒ no refill ⇒ the rest stay parked
      assert Queries.pick(Repo, "default", 10, @worker, @ttl) == []
    end

    test "weight: a step consuming N units takes N from the budget" do
      :ok = seed(0, 5)
      for _ <- 1..4, do: {:ok, _} = Queries.insert(Repo, params(%{rate_limit: "api", weight: 2}))
      # cumulative weight 2,4,6,8 vs avail 5 ⇒ only the first two fit
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 2
      assert tokens("api") == 1.0
    end

    test "NULL rate_limit bypasses the limiter entirely" do
      :ok = seed(0, 0)
      for _ <- 1..3, do: {:ok, _} = Queries.insert(Repo, params())
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 3
    end

    test "refill restores budget over elapsed time" do
      :ok = seed(100, 1)
      for _ <- 1..5, do: {:ok, _} = Queries.insert(Repo, params(%{rate_limit: "api"}))

      # burst 1 ⇒ first pick grants 1, bucket ~0
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 1

      # rate 100/s ⇒ ~30 ms later ≥1 token refilled ⇒ another grant
      Process.sleep(30)
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) >= 1
    end

    test "insert_all ensures buckets and rate-limits the whole batch" do
      :ok = seed(0, 2)

      ids =
        Queries.insert_all(Repo, [
          params(%{rate_limit: "api"}),
          params(%{rate_limit: "api"}),
          params(%{rate_limit: "api"})
        ])

      assert length(ids) == 3
      assert tokens("api") == 2.0
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 2
    end

    test "schedule_childs ensures buckets for rate-limited children" do
      :ok = seed(0, 1)
      {:ok, parent} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, "default", 10, @worker, @ttl)

      children = [
        params(%{fsm: "child", rate_limit: "api"}),
        params(%{fsm: "child", rate_limit: "api"})
      ]

      :ok = Queries.complete_schedule_childs(Repo, parent, @worker, "join", ~s({}), children, [])

      assert tokens("api") == 1.0
      # both children are runnable; the bucket (burst 1) lets one through
      assert length(Queries.pick(Repo, "default", 10, @worker, @ttl)) == 1
    end

    test "the pick self-heals a swept bucket; the row is grantable on the next pick" do
      :ok = seed(0, 5)
      {:ok, id} = Queries.insert(Repo, params(%{rate_limit: "api"}))
      # simulate gc_buckets having swept the bucket while the row slept
      Repo.query!("DELETE FROM gen_durable_rate_buckets WHERE key = 'api'")

      # first pick cannot grant (no bucket row) but the heal CTE recreates it full…
      assert Queries.pick(Repo, "default", 10, @worker, @ttl) == []
      assert tokens("api") == 5.0

      # …so the next pick grants — no permanent stall
      assert [%{id: ^id}] = Queries.pick(Repo, "default", 10, @worker, @ttl)
    end

    test "gc_buckets sweeps refilled-idle and orphaned buckets, keeps the rest" do
      # rate 10/s, burst 5 ⇒ fully refilled after 0.5s idle
      :ok = seed(10, 5)
      # zero-rate: never refills, so deleting would grant a fresh burst — never swept
      :ok = Queries.upsert_rate_configs(Repo, [%{name: "frozen", rate: 0.0, burst: 5.0}])

      Repo.query!("""
      INSERT INTO gen_durable_rate_buckets (key, tokens, last_refill) VALUES
        ('api:idle',  0, now() - interval '10 seconds'),
        ('api:fresh', 0, now()),
        ('ghost:1',   0, now() - interval '10 seconds'),
        ('frozen:1',  0, now() - interval '1 hour')
      """)

      # api:idle (refilled by now) and ghost:1 (config removed) go; the fresh
      # and the zero-rate buckets stay.
      assert Queries.gc_buckets(Repo) == 2

      %{rows: rows} = Repo.query!("SELECT key FROM gen_durable_rate_buckets ORDER BY key")
      assert List.flatten(rows) == ["api:fresh", "frozen:1"]
    end

    test "a throttled bucket emits [:gen_durable, :rate_limit, :throttled]" do
      :ok = seed(0, 2)
      for _ <- 1..5, do: {:ok, _} = Queries.insert(Repo, params(%{rate_limit: "api"}))

      handler = "throttle-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:gen_durable, :rate_limit, :throttled],
        &__MODULE__.__fwd__/4,
        self()
      )

      Queries.pick(Repo, "default", 10, @worker, @ttl)
      :telemetry.detach(handler)

      assert_received {:telemetry, %{wanted: 5, granted: 2}, %{key: "api", queue: "default"}}
    end
  end
end
