defmodule GenDurable.EngineTest do
  use ExUnit.Case, async: false

  alias GenDurable.Test.Repo

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    :ok
  end

  defp start_engine(opts \\ []) do
    defaults = [
      repo: Repo,
      fsms: GenDurable.Test.FSMs.all(),
      queues: [default: 5],
      poll_interval: 25,
      lease_ttl: 1_000,
      heartbeat_interval: 300,
      reap_interval: 150
    ]

    start_supervised!({GenDurable, Keyword.merge(defaults, opts)})
  end

  defp agent_spec(name, init), do: %{id: name, start: {Agent, :start_link, [init, [name: name]]}}

  @doc false
  def __forward_telemetry__(event, measurements, metadata, pid),
    do: send(pid, {:telemetry, event, measurements, metadata})

  defp attach_telemetry(events) do
    handler = "test-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(handler, events, &__MODULE__.__forward_telemetry__/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp status(id) do
    %{rows: [[s, result, err, attempt]]} =
      Repo.query!(
        "SELECT status::text, result, last_error, attempt FROM gen_durable WHERE id = $1",
        [id]
      )

    %{status: s, result: decode(result), last_error: err, attempt: attempt}
  end

  # jsonb objects come back as maps; scalar-string rows (the pre-fix double-encoded
  # format) as binaries.
  defp decode(nil), do: nil
  defp decode(value) when is_binary(value), do: Jason.decode!(value)
  defp decode(value), do: value

  defp eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    run = fn run ->
      case fun.() do
        {:ok, v} ->
          v

        :retry ->
          if System.monotonic_time(:millisecond) > deadline do
            flunk("eventually/2 timed out")
          else
            Process.sleep(20)
            run.(run)
          end
      end
    end

    run.(run)
  end

  defp wait_status(id, target) do
    eventually(fn ->
      row = status(id)
      if row.status == target, do: {:ok, row}, else: :retry
    end)
  end

  test "typed-state FSM drives :next loop to :done, committing each step" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Counter, state: %{target: 3})

    row = wait_status(id, "done")
    assert row.result == %{"count" => 3}
  end

  test "plain-map FSM also reaches :done" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.MapCounter, state: %{"target" => 2})

    row = wait_status(id, "done")
    assert row.result == %{"count" => 2}
  end

  test "await parks, signal wakes next_step which reads ctx.awaited and consumes it" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter)

    wait_status(id, "awaiting_signal")
    :ok = GenDurable.signal(id, "go", %{v: 7})

    row = wait_status(id, "done")
    assert row.result == %{"got" => %{"v" => 7}}

    # The instance is done, so its inbox was cleaned up.
    assert GenDurable.Queries.load_signals(Repo, id) == []
  end

  test "await: a signal that arrived before parking still wakes (no lost wake-up)" do
    # Deliver before the engine runs the step: the row is still runnable, so delivery
    # only inserts the signal (no flip). When "wait" parks, complete_await must see it
    # already in the inbox and go straight to "woke".
    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter, repo: Repo)
    :ok = GenDurable.signal(id, "go", %{v: 9}, repo: Repo)

    start_engine()

    row = wait_status(id, "done")
    assert row.result == %{"got" => %{"v" => 9}}
  end

  test "an await timeout wakes the step with empty awaited (not a failure)" do
    attach_telemetry([[:gen_durable, :await, :timeout]])
    start_engine(reap_interval: 50)
    {:ok, id} = GenDurable.insert(GenDurable.Test.AwaitTimeout)

    row = wait_status(id, "done")
    assert row.result == %{"timed_out" => true}
    # a timeout is a wake, not a failure
    assert row.attempt == 0
    assert_received {:telemetry, [:gen_durable, :await, :timeout], %{count: 1}, _}
  end

  test "a signal delivered before the deadline beats the await timeout" do
    start_engine(reap_interval: 50)
    {:ok, id} = GenDurable.insert(GenDurable.Test.AwaitTimeout)

    wait_status(id, "awaiting_signal")
    :ok = GenDurable.signal(id, "go", %{v: 1})

    row = wait_status(id, "done")
    assert row.result == %{"got" => %{"v" => 1}}
  end

  test "a retry on an await step re-sees ctx.awaited across the redo" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.AwaitRetry)

    wait_status(id, "awaiting_signal")
    :ok = GenDurable.signal(id, "go", %{"v" => 5})

    # "woke" retries once (attempt 0), then finishes (attempt 1). It would crash on
    # hd([]) if the retry had cleared awaits / consumed the signal.
    row = wait_status(id, "done")
    assert row.result == %{"v" => 5, "attempt" => 1}
  end

  test "await on a set wakes on any one and branches on which arrived" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Selector)

    wait_status(id, "awaiting_signal")
    :ok = GenDurable.signal(id, "reject", %{"why" => "nope"})

    row = wait_status(id, "done")
    assert row.result == %{"decision" => "reject", "by" => %{"why" => "nope"}}
  end

  test "a signal addressed by correlation_key wakes the instance" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter, correlation_key: "cust:42")

    wait_status(id, "awaiting_signal")
    # deliver by the business key, never having seen the internal id
    :ok = GenDurable.signal("cust:42", "go", %{"v" => 11})

    row = wait_status(id, "done")
    assert row.result == %{"got" => %{"v" => 11}}
  end

  test "await accumulates a pack across re-awaits, then processes all of it" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Collector)

    # Deliver the three out of order; each wakes "wait", which re-awaits until all here.
    :ok = GenDurable.signal(id, "b", %{"v" => 2})
    :ok = GenDurable.signal(id, "a", %{"v" => 1})
    :ok = GenDurable.signal(id, "c", %{"v" => 4})

    row = wait_status(id, "done")
    assert row.result == %{"sum" => 7, "count" => 3}
    # Terminal cleanup removed the whole pack.
    assert GenDurable.Queries.load_signals(Repo, id) == []
  end

  test "terminal cleanup deletes leftover (never-awaited) signals" do
    # Plain reaches :done immediately and never awaits, so a stray signal would
    # otherwise linger on the terminal row. complete_done must clear the inbox.
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain, repo: Repo)
    :ok = GenDurable.signal(id, "noise", %{}, repo: Repo)

    start_engine()

    wait_status(id, "done")
    assert GenDurable.Queries.load_signals(Repo, id) == []
  end

  test "caught exception routes to handle/2, which retries then stops" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Crasher)

    row = wait_status(id, "failed")
    assert row.last_error == "gave up"
    assert row.attempt == 2
  end

  test "an uncaught throw routes to handle/2 as {:throw, value} (no lease wait)" do
    # lease_ttl 60s: if the throw took the crash path, the test would sit out
    # the full lease — reaching "failed" promptly proves handle/2 caught it.
    start_engine(lease_ttl: 60_000)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Thrower)

    row = wait_status(id, "failed")
    assert row.last_error == "threw :bail"
    assert row.attempt == 0
  end

  test "an unserializable outcome routes to handle/2, not an infinite crash loop" do
    # A :done result Jason cannot encode is a deterministic user error: it must
    # reach handle/2 (default {:stop, reason}) promptly — on the crash path it
    # would loop through the reaper forever, one lease per cycle.
    start_engine(lease_ttl: 60_000)
    {:ok, id} = GenDurable.insert(GenDurable.Test.BadResult)

    row = wait_status(id, "failed")
    assert row.attempt == 0
    assert row.last_error =~ "Jason.Encoder"
  end

  test "startup reclaim frees a dead incarnation's claims without waiting out the lease" do
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain, repo: Repo)

    # Simulate a claim left by a dead scheduler of the same instance+queue+VM:
    # same claim prefix, different incarnation suffix. The lease is decayed past
    # the staleness margin (lease_ttl 1000 − 2×heartbeat 300 = 400ms of
    # remaining lease at most) but not yet expired — a freshly-heartbeated claim
    # (a LIVE owner) would sit above the margin and must never be reclaimed.
    stale = GenDurable.Scheduler.claim_prefix(GenDurable, "default") <> "0"

    Repo.query!(
      """
      UPDATE gen_durable
      SET status = 'executing', locked_by = $2,
          lease_expires_at = now() + interval '200 milliseconds'
      WHERE id = $1
      """,
      [id, stale]
    )

    attach_telemetry([[:gen_durable, :scheduler, :reclaimed]])
    start_engine()

    assert_receive {:telemetry, [:gen_durable, :scheduler, :reclaimed], %{count: 1},
                    %{queue: "default"}},
                   2_000

    row = wait_status(id, "done")
    assert row.result == %{}
  end

  test "worker crash with no outcome is recovered by the reaper (attempt + 1)" do
    start_engine(lease_ttl: 500, heartbeat_interval: 250, reap_interval: 100)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Reborn)

    row = wait_status(id, "done")
    assert row.result == %{"recovered" => true}
    assert row.attempt >= 1
  end

  test "concurrency_key serializes steps sharing a key (no lost updates)" do
    start_supervised!(%{
      id: :concurrency_agent,
      start: {Agent, :start_link, [fn -> 0 end, [name: GenDurable.Test.ConcurrencyAgent]]}
    })

    start_engine()

    ids =
      for _ <- 1..4 do
        {:ok, id} = GenDurable.insert(GenDurable.Test.ConcurrencyInc, concurrency_key: "k")
        id
      end

    for id <- ids, do: wait_status(id, "done")

    assert Agent.get(GenDurable.Test.ConcurrencyAgent, & &1) == 4
  end

  test "schedule_childs fans out and joins on all children (spec §11)" do
    start_engine()
    kids = for i <- 1..4, do: %{"x" => i}
    {:ok, id} = GenDurable.insert(GenDurable.Test.Parent, state: %{"kids" => kids})

    row = wait_status(id, "done")
    assert row.result == %{"children" => 4, "done" => 4, "failed" => 0}
  end

  test "a failed child still releases the join barrier" do
    start_engine()
    kids = [%{"x" => 1}, %{"x" => 2, "fail" => true}]
    {:ok, id} = GenDurable.insert(GenDurable.Test.Parent, state: %{"kids" => kids})

    row = wait_status(id, "done")
    assert row.result == %{"children" => 2, "done" => 1, "failed" => 1}
  end

  test "zero children means the parent proceeds to next_step immediately" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Parent, state: %{"kids" => []})

    row = wait_status(id, "done")
    assert row.result == %{"children" => 0, "done" => 0, "failed" => 0}
  end

  test "picker honors priority (lower number runs first)" do
    start_supervised!(agent_spec(GenDurable.Test.RecorderAgent, fn -> [] end))
    # Insert before the engine starts so both are runnable at the first poll.
    {:ok, _} =
      GenDurable.insert(GenDurable.Test.Recorder,
        repo: Repo,
        state: %{"tag" => "low"},
        priority: 5
      )

    {:ok, _} =
      GenDurable.insert(GenDurable.Test.Recorder,
        repo: Repo,
        state: %{"tag" => "high"},
        priority: 0
      )

    start_engine(queues: [default: 1])

    eventually(fn ->
      if length(Agent.get(GenDurable.Test.RecorderAgent, & &1)) == 2, do: {:ok, :ok}, else: :retry
    end)

    assert Agent.get(GenDurable.Test.RecorderAgent, & &1) == ["high", "low"]
  end

  test "per-queue schedulers route by queue" do
    start_engine(queues: [a: 1, b: 1])
    {:ok, ia} = GenDurable.insert(GenDurable.Test.Plain, queue: "a")
    {:ok, ib} = GenDurable.insert(GenDurable.Test.Plain, queue: "b")

    wait_status(ia, "done")
    wait_status(ib, "done")
  end

  test "schedule_in delays eligibility (scheduling sugar)" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain, schedule_in: 400)

    Process.sleep(120)
    assert status(id).status == "runnable"

    assert wait_status(id, "done").status == "done"
  end

  test "heartbeat keeps a long step's lease alive (no spurious reap)" do
    start_engine(lease_ttl: 400, heartbeat_interval: 100, reap_interval: 100)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Sleeper, state: %{"ms" => 900})

    row = wait_status(id, "done")
    # attempt stays 0 => the lease never expired, so the step was never re-run
    assert row.attempt == 0
    assert row.result == %{"slept" => 900}
  end

  test "prefetched (buffered) rows are heartbeated and never spuriously reaped" do
    # concurrency 1 + prefetch 5 ⇒ the picker claims all rows up front and holds
    # the tail in the in-memory buffer. Each step sleeps 200ms, so a buffered row
    # waits well past the 300ms lease before it runs. If the buffer weren't
    # heartbeated, the reaper would expire those leases and bump `attempt`.
    start_engine(
      queues: [default: 1],
      prefetch: 5,
      lease_ttl: 300,
      heartbeat_interval: 100,
      reap_interval: 100,
      poll_interval: 25
    )

    ids =
      for _ <- 1..4 do
        {:ok, id} = GenDurable.insert(GenDurable.Test.Sleeper, state: %{"ms" => 200})
        id
      end

    rows = for id <- ids, do: wait_status(id, "done")

    assert Enum.all?(rows, &(&1.attempt == 0))
    assert Enum.all?(rows, &(&1.result == %{"slept" => 200}))
  end

  test "different concurrency_keys run in parallel (overlapping steps)" do
    start_supervised!(agent_spec(GenDurable.Test.OverlapAgent, fn -> %{} end))
    start_engine(queues: [default: 4])

    {:ok, a} = GenDurable.insert(GenDurable.Test.Overlap, concurrency_key: "ka")
    {:ok, b} = GenDurable.insert(GenDurable.Test.Overlap, concurrency_key: "kb")

    wait_status(a, "done")
    wait_status(b, "done")

    m = Agent.get(GenDurable.Test.OverlapAgent, & &1)
    # The two steps overlapped: each started before the other stopped.
    assert m[{a, :start}] < m[{b, :stop}]
    assert m[{b, :start}] < m[{a, :stop}]
  end

  test "an FSM not listed in :fsms resolves dynamically from its module name" do
    # FSMs.all() does NOT include Auto — it must resolve via its default name.
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.Auto)

    row = wait_status(id, "done")
    assert row.result == %{"auto" => true}
  end

  test "job form: a perform/1 job runs and finishes" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.JobOk, args: %{"x" => 1})

    row = wait_status(id, "done")
    assert row.result == %{}
    assert row.attempt == 0
  end

  test "job form: perform/2 returns a result and sees ctx" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.JobResult, args: %{"n" => 21})

    row = wait_status(id, "done")
    assert row.result == %{"doubled" => 42, "attempt" => 0}
  end

  test "job form: {:error, _} retries with backoff then succeeds" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.JobRetry)

    row = wait_status(id, "done")
    assert row.result == %{"ok_at" => 2}
    assert row.attempt == 2
  end

  test "job form: errors exhaust max_attempts then fail" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.JobGiveUp)

    row = wait_status(id, "failed")
    assert row.last_error == "always"
    assert row.attempt == 2
  end

  test "job form: {:cancel, _} fails immediately, no retry" do
    start_engine()
    {:ok, id} = GenDurable.insert(GenDurable.Test.JobCancel)

    row = wait_status(id, "failed")
    assert row.last_error == "nope"
    assert row.attempt == 0
  end

  test "graceful shutdown releases buffered work and drains in-flight (no lease wait)" do
    # Insert before the engine starts so the first poll claims all four at once.
    ids =
      for _ <- 1..4 do
        {:ok, id} = GenDurable.insert(GenDurable.Test.Sleeper, repo: Repo, state: %{"ms" => 400})
        id
      end

    attach_telemetry([[:gen_durable, :scheduler, :drain]])
    # concurrency 1 + prefetch 5 ⇒ one in-flight, three buffered. A 60s lease means
    # the reaper cannot help within the test: only the drain can free them.
    start_engine(queues: [default: 1], prefetch: 5, lease_ttl: 60_000, drain_timeout: 2_000)

    Process.sleep(120)
    # the supervised child id is the instance name (default GenDurable)
    :ok = stop_supervised(GenDurable)

    assert_received {:telemetry, [:gen_durable, :scheduler, :drain], %{released: 3, in_flight: 1},
                     _}

    statuses = Enum.map(ids, &status(&1).status)
    refute "executing" in statuses
    assert Enum.count(statuses, &(&1 == "done")) == 1
    assert Enum.count(statuses, &(&1 == "runnable")) == 3
  end

  test "emits pick and saturation telemetry" do
    attach_telemetry([[:gen_durable, :pick, :stop], [:gen_durable, :scheduler, :saturation]])
    # Insert before the engine starts so the first pick claims it (count >= 1).
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain, repo: Repo)
    start_engine()
    wait_status(id, "done")

    assert_receive {:telemetry, [:gen_durable, :pick, :stop], %{count: count},
                    %{queue: "default"}},
                   2_000

    assert count >= 1

    assert_receive {:telemetry, [:gen_durable, :scheduler, :saturation], %{concurrency: 5}, _},
                   2_000
  end

  defp row_count(id) do
    %{rows: [[n]]} = Repo.query!("SELECT count(*) FROM gen_durable WHERE id = $1", [id])
    n
  end

  test "GC deletes a completed instance after retention and emits :swept" do
    attach_telemetry([[:gen_durable, :gc, :swept]])
    # retention 0 ⇒ a terminated row is collectible at once; sweep often.
    start_engine(gc_interval: 30, gc_retention: 0)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)

    eventually(fn -> if row_count(id) == 0, do: {:ok, :gone}, else: :retry end)

    assert_receive {:telemetry, [:gen_durable, :gc, :swept], %{count: c}, _}, 2_000
    assert c >= 1
  end

  test "gc_interval: nil disables the GC process; terminal rows persist" do
    start_engine(gc_interval: nil, gc_retention: 0)
    refute Process.whereis(GenDurable.GC)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    wait_status(id, "done")

    Process.sleep(100)
    assert row_count(id) == 1
  end

  test "a :next naming an unconfigured rate_limit emits :rate_limit :unknown (spec §12)" do
    attach_telemetry([[:gen_durable, :rate_limit, :unknown]])
    start_engine()
    {:ok, _id} = GenDurable.insert(GenDurable.Test.RateUnknown)

    assert_receive {:telemetry, [:gen_durable, :rate_limit, :unknown], %{count: 1},
                    %{name: "ghost", step: "go"}},
                   2_000
  end

  test "two named instances coexist; API calls route by :name" do
    # Default-named instance polling "default" + a second instance polling
    # "second". The mere start of the second one exercises the isolation: named
    # supervisor, task supervisor, config entry, and registry are all per-name.
    start_engine()

    start_supervised!(
      {GenDurable,
       [
         name: GenDurable.EngineTwo,
         repo: Repo,
         fsms: GenDurable.Test.FSMs.all(),
         queues: [second: 5],
         poll_interval: 25,
         lease_ttl: 1_000,
         heartbeat_interval: 300,
         reap_interval: 150
       ]}
    )

    {:ok, id} =
      GenDurable.insert(GenDurable.Test.Counter,
        state: %{target: 2},
        queue: "second",
        name: GenDurable.EngineTwo
      )

    # only the second instance polls "second" — done means it ran there
    row = wait_status(id, "done")
    assert row.result == %{"count" => 2}

    # an unknown instance name is a loud error, not a silent misroute
    assert_raise ArgumentError, ~r/no GenDurable instance named/, fn ->
      GenDurable.insert(GenDurable.Test.Counter, state: %{target: 1}, name: Nope)
    end
  end
end
