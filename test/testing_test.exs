defmodule GenDurable.TestingTest do
  use ExUnit.Case, async: false
  use GenDurable.Testing, repo: GenDurable.Test.Repo, fsms: GenDurable.Test.FSMs.all()

  alias GenDurable.Test.Repo

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  test "drain runs a plain FSM to done, inline" do
    {:ok, id} = GenDurable.insert(GenDurable.Test.Counter, state: %{target: 3}, repo: Repo)

    assert %{done: 1, next: 3, steps: 4} = drain()
    assert %{"count" => 3} = assert_done(id)
  end

  test "await flow: drain parks, a signal resumes, drain finishes" do
    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter, repo: Repo)

    assert %{await: 1, done: 0} = drain()
    assert_awaiting(id, "go")

    :ok = GenDurable.signal(id, "go", %{v: 7}, repo: Repo)

    assert %{done: 1} = drain()
    assert_done(id, %{"got" => %{"v" => 7}})
  end

  test "child fan-out joins inline in one drain" do
    kids = for i <- 1..2, do: %{"x" => i}
    {:ok, id} = GenDurable.insert(GenDurable.Test.Parent, state: %{"kids" => kids}, repo: Repo)

    summary = drain()
    assert summary.schedule_childs == 1
    # parent + 2 children reach :done
    assert summary.done == 3

    assert_done(id, %{"children" => 2, "done" => 2, "failed" => 0})
  end

  test "retry backoffs collapse by default; the failure asserts by message" do
    {:ok, id} = GenDurable.insert(GenDurable.Test.Crasher, repo: Repo)

    assert %{retry: 2, stop: 1} = drain()
    assert_failed(id, "gave up")
    assert_failed(id, ~r/gave/)
  end

  test "with_scheduled: false leaves future work alone" do
    {:ok, id} =
      GenDurable.insert(GenDurable.Test.Plain, schedule_in: 60_000, repo: Repo)

    assert %{steps: 0} = drain(with_scheduled: false)
    assert_status(id, :runnable)

    assert %{done: 1} = drain()
    assert_done(id, %{})
  end

  test "queue: drains only the given queue" do
    {:ok, a} = GenDurable.insert(GenDurable.Test.Plain, queue: "a", repo: Repo)
    {:ok, b} = GenDurable.insert(GenDurable.Test.Plain, queue: "b", repo: Repo)

    assert %{done: 1} = drain(queue: "a")
    assert_done(a)
    assert_status(b, :runnable)
  end

  test "fire_timeouts force-fires an armed await deadline" do
    {:ok, id} = GenDurable.insert(GenDurable.Test.AwaitTimeout, repo: Repo)

    assert %{await: 1} = drain()
    assert_awaiting(id, "go")

    # the deadline (300ms) has NOT passed — fire it anyway
    assert fire_timeouts() == 1
    assert %{done: 1} = drain()
    assert_done(id, %{"timed_out" => true})
  end

  test "max_steps caps a runaway FSM with a clear error" do
    {:ok, _} = GenDurable.insert(GenDurable.Test.Counter, state: %{target: 50}, repo: Repo)

    assert_raise RuntimeError, ~r/max_steps/, fn -> drain(max_steps: 5) end
  end

  test "durable/assert helpers resolve a finished instance by correlation key" do
    {:ok, id} =
      GenDurable.insert(GenDurable.Test.Plain, correlation_key: "order:7", repo: Repo)

    drain()

    # default scope freed the key on termination; the test helper still finds
    # the latest row carrying it
    assert_done("order:7", %{})
    assert %{id: ^id, status: :done} = durable("order:7")
  end

  test "a concurrency gate admits, credits, and re-admits through drain" do
    Repo.query!("TRUNCATE gen_durable_concurrency_buckets, gen_durable_concurrency_configs")

    :ok =
      GenDurable.Queries.upsert_concurrency_configs(Repo, [%{name: "gate", cap: 1, shards: 1}])

    {:ok, a} = GenDurable.insert(GenDurable.Test.Plain, concurrency_key: {:gate, 7}, repo: Repo)
    {:ok, b} = GenDurable.insert(GenDurable.Test.Plain, concurrency_key: {:gate, 7}, repo: Repo)

    # the gate is COLD (no bucket rows exist yet) — the first pick must
    # mint-and-admit in the same statement, or drain would see an empty pick
    # and wrongly stop at "quiescence" with runnable work left
    %{rows: [[buckets]]} =
      Repo.query!("SELECT count(*) FROM gen_durable_concurrency_buckets WHERE key = 'gate:7'")

    assert buckets == 0

    # cap 1: the drain loop admits one, its completion credits the slot back,
    # the next iteration admits the other — all through the production pick
    assert %{done: 2} = drain()
    assert_done(a)
    assert_done(b)
  end

  test "a rate limit admits through a cold bucket in one drain" do
    Repo.query!("TRUNCATE gen_durable_rate_buckets, gen_durable_rate_configs")

    :ok = GenDurable.Queries.upsert_rate_configs(Repo, [%{name: "api", rate: 0.0, burst: 2.0}])

    {:ok, a} = GenDurable.insert(GenDurable.Test.Plain, rate_limit: {:api, 7}, repo: Repo)
    {:ok, b} = GenDurable.insert(GenDurable.Test.Plain, rate_limit: {:api, 7}, repo: Repo)

    # the bucket is COLD (nothing minted at insert) — the first pick must
    # mint-and-grant in the same statement, or drain would see an empty pick
    # and wrongly stop at "quiescence" with runnable work left
    %{rows: [[buckets]]} =
      Repo.query!("SELECT count(*) FROM gen_durable_rate_buckets WHERE key = 'api:7'")

    assert buckets == 0

    # burst 2 covers both jobs on the cold pick (rate 0: no refill afterwards)
    assert %{done: 2} = drain()
    assert_done(a)
    assert_done(b)
  end

  test "assert_status flunks helpfully on a missing instance" do
    assert_raise ExUnit.AssertionError, ~r/no gen_durable instance/, fn ->
      assert_status(999_999, :done)
    end
  end
end
