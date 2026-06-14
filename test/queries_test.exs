defmodule GenDurable.QueriesTest do
  use ExUnit.Case, async: false

  alias GenDurable.Queries
  alias GenDurable.Test.Repo

  @worker "test-worker"
  @ttl 60_000

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
        partition_key: nil,
        unique_key: nil,
        unique_scope: [],
        eligible_at: nil
      },
      overrides
    )
  end

  test "insert then pick flips to executing and returns the job" do
    {:ok, id} = Queries.insert(Repo, params())

    [job] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
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

    assert [%{}] = Queries.pick(Repo, ["a"], 10, @worker, @ttl)
    assert [] = Queries.pick(Repo, ["a"], 10, @worker, @ttl)
    assert [%{}] = Queries.pick(Repo, ["b"], 10, @worker, @ttl)
  end

  describe "partition_key dedup in the picker (spec §6)" do
    test "claims at most one runnable row per partition_key in a batch" do
      for _ <- 1..3, do: {:ok, _} = Queries.insert(Repo, params(%{partition_key: "k"}))
      {:ok, other} = Queries.insert(Repo, params(%{partition_key: "k2"}))

      jobs = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
      keys = Enum.map(jobs, & &1.partition_key)

      assert Enum.count(keys, &(&1 == "k")) == 1
      assert Enum.count(keys, &(&1 == "k2")) == 1
      assert length(jobs) == 2
      assert other in Enum.map(jobs, & &1.id)
    end

    test "skips a runnable row whose partition_key is already executing" do
      {:ok, a} = Queries.insert(Repo, params(%{partition_key: "k"}))
      {:ok, _b} = Queries.insert(Repo, params(%{partition_key: "k"}))

      # Claim one row for "k"; it becomes executing and holds the key.
      assert [%{id: ^a}] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)

      # The sibling is runnable, but "k" is executing => not picked (no bounce).
      assert [] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
    end

    test "NULL partition_key rows are never deduped against each other" do
      for _ <- 1..3, do: {:ok, _} = Queries.insert(Repo, params())

      assert length(Queries.pick(Repo, ["default"], 10, @worker, @ttl)) == 3
    end
  end

  test "complete_next resets attempt and returns to runnable" do
    {:ok, id} = Queries.insert(Repo, params())
    [_job] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)

    :ok = Queries.complete_next(Repo, id, "tick", ~s({"n":1}))

    %{rows: [[status, step, attempt, state]]} =
      Repo.query!("SELECT status::text, step, attempt, state FROM gen_durable WHERE id = $1", [id])

    assert status == "runnable"
    assert step == "tick"
    assert attempt == 0
    assert Jason.decode!(state) == %{"n" => 1}
  end

  test "complete_replay bumps attempt and delays eligibility" do
    {:ok, id} = Queries.insert(Repo, params())
    [_] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)

    :ok = Queries.complete_replay(Repo, id, ~s({"n":0}), 50_000)

    %{rows: [[status, attempt, future]]} =
      Repo.query!(
        "SELECT status::text, attempt, eligible_at > now() FROM gen_durable WHERE id = $1",
        [id]
      )

    assert status == "runnable"
    assert attempt == 1
    assert future == true
  end

  test "complete_done and complete_stop are terminal" do
    {:ok, d} = Queries.insert(Repo, params())
    :ok = Queries.complete_done(Repo, d, ~s({"ok":true}))

    %{rows: [[status, result]]} =
      Repo.query!("SELECT status::text, result FROM gen_durable WHERE id = $1", [d])

    assert status == "done"
    assert Jason.decode!(result) == %{"ok" => true}

    {:ok, s} = Queries.insert(Repo, params())
    :ok = Queries.complete_stop(Repo, s, "boom")

    %{rows: [[status, err]]} =
      Repo.query!("SELECT status::text, last_error FROM gen_durable WHERE id = $1", [s])

    assert status == "failed"
    assert err == "boom"
  end

  test "reaper returns expired-lease executing rows to runnable with attempt+1" do
    {:ok, id} = Queries.insert(Repo, params())
    # Pick with a negative TTL so the lease is already expired.
    [_] = Queries.pick(Repo, ["default"], 10, @worker, -1000)

    assert Queries.reap(Repo) == [id]

    %{rows: [[status, attempt, locked_by]]} =
      Repo.query!("SELECT status::text, attempt, locked_by FROM gen_durable WHERE id = $1", [id])

    assert status == "runnable"
    assert attempt == 1
    assert locked_by == nil
  end

  describe "signals" do
    test "deliver wakes a matching await and keeps awaits (name-scoped consumption)" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, ~s({}), "go")

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({"v":1}), nil)

      %{rows: [[status, awaits]]} =
        Repo.query!("SELECT status::text, awaits FROM gen_durable WHERE id = $1", [id])

      assert status == "runnable"
      # awaits is kept until the woken step progresses (spec §5)
      assert awaits == "go"

      assert [%{name: "go", payload: %{"v" => 1}}] = Queries.load_signals(Repo, id)
    end

    test "non-matching signal does not wake the instance" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, ~s({}), "go")

      :ok = Queries.deliver_signal(Repo, id, "other", ~s({}), nil)

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      assert status == "awaiting_signal"
    end

    test "a signal that arrived before the await unparks the instance (no lost wakeup)" do
      {:ok, id} = Queries.insert(Repo, params())
      # signal arrives while the instance is still runnable (not yet awaiting)
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)

      [_] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, ~s({}), "go")

      %{rows: [[status]]} =
        Repo.query!("SELECT status::text FROM gen_durable WHERE id = $1", [id])

      # EXISTS race-fix: matching signal already present => straight back to runnable
      assert status == "runnable"
    end

    test "dedup_key makes redelivery idempotent; nil dedup allows duplicates" do
      {:ok, id} = Queries.insert(Repo, params())

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), "k1")
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), "k1")
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)
      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)

      assert length(Queries.load_signals(Repo, id)) == 3
    end

    test "progress consumes only the awaited name; other signals survive (§5)" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = Queries.pick(Repo, ["default"], 10, @worker, @ttl)
      :ok = Queries.complete_await(Repo, id, ~s({}), "go")

      :ok = Queries.deliver_signal(Repo, id, "go", ~s({}), nil)
      :ok = Queries.deliver_signal(Repo, id, "other", ~s({}), nil)

      # the woken step progresses
      :ok = Queries.complete_next(Repo, id, "tick", ~s({}))

      assert [%{name: "other"}] = Queries.load_signals(Repo, id)

      %{rows: [[awaits]]} = Repo.query!("SELECT awaits FROM gen_durable WHERE id = $1", [id])
      assert awaits == nil
    end
  end

  describe "uniqueness" do
    test "duplicate within occupied scope is rejected; NULL key never conflicts" do
      key = <<1, 2, 3>>
      scope = ["runnable", "executing"]

      assert {:ok, _} = Queries.insert(Repo, params(%{unique_key: key, unique_scope: scope}))

      assert {:error, :duplicate} =
               Queries.insert(Repo, params(%{unique_key: key, unique_scope: scope}))

      # No unique_key => no dedup.
      assert {:ok, _} = Queries.insert(Repo, params())
      assert {:ok, _} = Queries.insert(Repo, params())
    end

    test "leaving the occupied scope frees the key for re-insertion" do
      key = <<9>>
      scope = ["runnable", "executing"]

      {:ok, id} = Queries.insert(Repo, params(%{unique_key: key, unique_scope: scope}))
      # Move to a status outside the scope.
      :ok = Queries.complete_done(Repo, id, ~s({}))

      assert {:ok, _} = Queries.insert(Repo, params(%{unique_key: key, unique_scope: scope}))
    end

    test "batch insert dedups within the batch and against existing rows" do
      key = <<7>>
      scope = ["runnable"]

      {:ok, _} = Queries.insert(Repo, params(%{unique_key: key, unique_scope: scope}))

      rows = [
        params(%{unique_key: key, unique_scope: scope}),
        params(%{unique_key: <<8>>, unique_scope: scope}),
        params(%{unique_key: <<8>>, unique_scope: scope})
      ]

      # <<7>> collides with the existing row; <<8>> collides within the batch.
      assert length(Queries.insert_all(Repo, rows)) == 1
    end
  end
end
