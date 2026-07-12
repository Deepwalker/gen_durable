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
      reaper: [interval: 150]
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
    start_engine(reaper: [interval: 50])
    {:ok, id} = GenDurable.insert(GenDurable.Test.AwaitTimeout)

    row = wait_status(id, "done")
    assert row.result == %{"timed_out" => true}
    # a timeout is a wake, not a failure
    assert row.attempt == 0
    assert_received {:telemetry, [:gen_durable, :await, :timeout], %{count: 1}, _}
  end

  test "a signal delivered before the deadline beats the await timeout" do
    start_engine(reaper: [interval: 50])
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
    start_engine(lease_ttl: 500, heartbeat_interval: 250, reaper: [interval: 100])
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
    start_engine(lease_ttl: 400, heartbeat_interval: 100, reaper: [interval: 100])
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
      reaper: [interval: 100],
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
    start_engine(gc: [interval: 30, retention: 0])
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)

    eventually(fn -> if row_count(id) == 0, do: {:ok, :gone}, else: :retry end)

    assert_receive {:telemetry, [:gen_durable, :gc, :swept], %{count: c}, _}, 2_000
    assert c >= 1
  end

  test "gc: false runs no GC on this node; terminal rows persist" do
    sup = start_engine(gc: false)
    ids = for {id, _, _, _} <- Supervisor.which_children(sup), do: id
    refute GenDurable.GC in ids
    assert GenDurable.Reaper in ids

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    wait_status(id, "done")

    Process.sleep(100)
    assert row_count(id) == 1
  end

  test "reaper: false runs no reaper on this node" do
    sup = start_engine(reaper: false)
    ids = for {id, _, _, _} <- Supervisor.which_children(sup), do: id
    refute GenDurable.Reaper in ids
    assert GenDurable.GC in ids
  end

  test "a local insert pokes the queue's scheduler — no waiting for the poll" do
    # the startup poll fires once at 0; after that the next poll is a minute
    # away, so only the poke can get this job executed within the 5s deadline
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    assert %{status: "done"} = wait_status(id, "done")
  end

  test "a future-scheduled insert pokes nobody" do
    attach_telemetry([[:gen_durable, :pick, :stop]])
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)

    # swallow the startup poll's pick event
    assert_receive {:telemetry, [:gen_durable, :pick, :stop], _, _}, 1_000

    {:ok, _} = GenDurable.insert(GenDurable.Test.Plain, schedule_in: 60_000)
    refute_receive {:telemetry, [:gen_durable, :pick, :stop], _, _}, 300
  end

  defp redis_url, do: System.get_env("REDIS_URL", "redis://localhost:6379")

  test "poke: :cluster fans out to every member of the queue's pg group" do
    start_engine(poke: :cluster, poll_interval: 60_000, max_poll_interval: 60_000)

    # a stand-in for a remote node's scheduler: a real one differs only in
    # living on another node, and delivery over distribution is OTP's contract
    :ok = :pg.join(GenDurable.Poke.scope(GenDurable), "default", self())

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    assert_receive :poke, 1_000
    assert %{status: "done"} = wait_status(id, "done")
  end

  test "poke: {:redis, _} executes a local insert immediately (the direct leg)" do
    start_engine(poke: {:redis, redis_url()}, poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    assert %{status: "done"} = wait_status(id, "done")
  end

  test "a foreign-origin redis publish pokes this node's schedulers" do
    start_engine(poke: {:redis, redis_url()}, poll_interval: 60_000, max_poll_interval: 60_000)
    # let the startup poll (scheduled at 0) pass — only the publish may
    # deliver this row within the deadline
    Process.sleep(100)

    # a row inserted "on another node": straight through Queries, no poke here
    {:ok, id} =
      GenDurable.Queries.insert(Repo, GenDurable.build_params(GenDurable.Test.Plain, []))

    # publish as another VM would; retried so the test doesn't race the
    # listener's subscribe handshake
    {:ok, redix} = Redix.start_link(redis_url())

    eventually(fn ->
      Redix.command!(redix, [
        "PUBLISH",
        GenDurable.Poke.channel(GenDurable),
        "other-vm#1|default"
      ])

      if status(id).status == "done", do: {:ok, :done}, else: :retry
    end)
  end

  test "a self-originated redis publish is dropped (the direct leg already ran)" do
    start_engine(poke: {:redis, redis_url()}, poll_interval: 60_000, max_poll_interval: 60_000)
    # let the startup poll (scheduled at 0) pass before the row exists
    Process.sleep(100)

    {:ok, id} =
      GenDurable.Queries.insert(Repo, GenDurable.build_params(GenDurable.Test.Plain, []))

    token = GenDurable.Scheduler.vm_id()
    {:ok, redix} = Redix.start_link(redis_url())

    # the later publishes certainly hit a live subscription; a broken origin
    # check would run the job and fail the final assert
    for _ <- 1..10 do
      Redix.command!(redix, ["PUBLISH", GenDurable.Poke.channel(GenDurable), token <> "|default"])
      Process.sleep(50)
    end

    assert %{status: "runnable"} = status(id)
  end

  test "await: a fresh insert answers {:done, result} within the call (local push)" do
    # poll 60s: only poke discovers the row, only the executor's nudge (or the
    # 25ms watcher) can answer the await this fast
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    assert {:done, %{}} = GenDurable.await(id, 3_000)
  end

  test "await: deadline hit returns {:busy, snap}; a later await is the retry protocol" do
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Sleeper, state: %{ms: 400})

    assert {:busy, %{status: status}} = GenDurable.await(id, 100)
    assert status in [:runnable, :executing]

    # the client came back with the id token — the work finished meanwhile
    assert {:done, %{"slept" => 400}} = GenDurable.await(id, 3_000)
  end

  test "await: a parked instance settles as {:awaiting, snap}; :terminal waits through it" do
    # poll 60s end to end: the insert reaches "parked" via the insert poke, and
    # the resume after the signal is reachable only via the signal-wake poke
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Awaiter)
    assert {:awaiting, %{status: :awaiting_signal}} = GenDurable.await(id, 3_000)

    # until: :terminal treats the park as still-pending
    assert {:busy, %{status: :awaiting_signal}} = GenDurable.await(id, 200, until: :terminal)

    :ok = GenDurable.signal(id, "go", %{n: 1})
    assert {:done, %{"got" => %{"n" => 1}}} = GenDurable.await(id, 3_000)
  end

  test "a cross-queue fan-out completes with 60s polls: children and the join wake are poked" do
    # parent on "default", children on "kids": the insert poke starts the
    # parent, the schedule_childs poke starts the children in THEIR queue, and
    # the last child's completion pokes the woken parent's queue — three
    # different poke paths, no poll assistance
    start_engine(
      queues: [default: 5, kids: 5],
      poll_interval: 60_000,
      max_poll_interval: 60_000
    )

    Process.sleep(100)

    {:ok, id} = GenDurable.insert(GenDurable.Test.CrossQueueParent, state: %{n: 3})
    # until: :terminal — the fan-out parks the parent (awaiting_children is a
    # settled state), and we want the whole round trip
    assert {:done, %{"done" => 3}} = GenDurable.await(id, 5_000, until: :terminal)
  end

  test "await: unknown id is :not_found immediately" do
    start_engine()
    assert :not_found = GenDurable.await(999_999_999, 5_000)
  end

  test "await: a result committed by 'another node' arrives via the watcher tick" do
    # queues: [] — nothing executes locally, so no executor nudge can fire;
    # the batched watcher is the only wake-up source
    start_engine(queues: [], poll_interval: 60_000, max_poll_interval: 60_000)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    waiter = Task.async(fn -> GenDurable.await(id, 3_000) end)
    Process.sleep(100)

    # complete the row as a foreign worker would
    Repo.query!(
      "UPDATE gen_durable SET status = 'executing', locked_by = 'foreign' WHERE id = $1",
      [id]
    )

    {:ok, _} = GenDurable.Queries.complete_done(Repo, id, "foreign", ~s({"via": "watcher"}))

    assert {:done, %{"via" => "watcher"}} = Task.await(waiter)
  end

  test "await: works with a bare repo and no running engine (poll loop)" do
    {:ok, id} =
      GenDurable.Queries.insert(Repo, GenDurable.build_params(GenDurable.Test.Plain, []))

    waiter = Task.async(fn -> GenDurable.await(id, 3_000, repo: Repo) end)
    Process.sleep(80)

    Repo.query!(
      "UPDATE gen_durable SET status = 'executing', locked_by = 'bare' WHERE id = $1",
      [id]
    )

    {:ok, _} = GenDurable.Queries.complete_done(Repo, id, "bare", ~s({}))

    assert {:done, %{}} = Task.await(waiter)
  end

  test "await: the engine stopping mid-wait returns {:busy}, never crashes the caller" do
    # queues: [] — the row stays runnable, so the waiter is parked in its
    # receive when the whole engine (Watcher, waiter table) goes down; the reply
    # and the after-block cleanup must both survive that
    start_engine(queues: [], gc: false, reaper: false)
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)

    waiter = Task.async(fn -> GenDurable.await(id, 600) end)
    Process.sleep(150)
    :ok = stop_supervised(GenDurable)

    assert {:busy, %{status: :runnable}} = Task.await(waiter)
  end

  test "await: a Watcher restart mid-wait costs the re-arm cadence, not the deadline" do
    # queues: [] — no executor nudge; the kill empties the Watcher's waiters
    # map, so only the waiter's own re-arm re-check can see the completion
    start_engine(queues: [], poll_interval: 60_000, max_poll_interval: 60_000)

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    started = System.monotonic_time(:millisecond)
    waiter = Task.async(fn -> GenDurable.await(id, 10_000) end)
    Process.sleep(100)

    Process.exit(Process.whereis(GenDurable.Await.watcher(GenDurable)), :kill)
    Process.sleep(50)

    Repo.query!(
      "UPDATE gen_durable SET status = 'executing', locked_by = 'foreign' WHERE id = $1",
      [id]
    )

    {:ok, _} = GenDurable.Queries.complete_done(Repo, id, "foreign", ~s({"via": "rearm"}))

    assert {:done, %{"via" => "rearm"}} = Task.await(waiter, 11_000)
    # well under the deadline: the re-arm pass (≤1s) picked it up
    assert System.monotonic_time(:millisecond) - started < 5_000
  end

  test "poke survives a :pg scope restart: schedulers re-join the restarted scope" do
    start_engine(poll_interval: 60_000, max_poll_interval: 60_000)
    Process.sleep(100)

    scope = GenDurable.Poke.scope(GenDurable)
    old = Process.whereis(scope)
    Process.exit(old, :kill)

    # the RESTARTED scope starts empty; wait for the schedulers' monitors to
    # re-join it (the old pid's ETS can linger for a beat — don't trust it)
    eventually(fn ->
      try do
        new = Process.whereis(scope)

        if new && new != old && :pg.get_local_members(scope, "default") != [],
          do: {:ok, :joined},
          else: :retry
      catch
        # the membership table vanished mid-read (old scope mid-death)
        _, _ -> :retry
      end
    end)

    # with 60s polls, only a working poke can run this within the await window
    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain)
    assert {:done, %{}} = GenDurable.await(id, 3_000)
  end

  test "await: [tick: 0] fails the boot loudly" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: msg}, _stack}} =
             GenDurable.Supervisor.start_link(name: BadTick, repo: Repo, await: [tick: 0])

    assert msg =~ ":await tick"
  end

  test "a bad :poke option fails the boot loudly" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: msg}, _stack}} =
             GenDurable.Supervisor.start_link(name: BadPoke, repo: Repo, poke: :multicast)

    assert msg =~ ":poke must be"
  end

  test "a web-only node (queues: [], gc/reaper off) still inserts and signals" do
    sup = start_engine(queues: [], gc: false, reaper: false)
    ids = for {id, _, _, _} <- Supervisor.which_children(sup), do: id
    refute Enum.any?(ids, &match?({GenDurable.Scheduler, _}, &1))

    {:ok, id} = GenDurable.insert(GenDurable.Test.Plain, correlation_key: "web:1")
    assert :ok = GenDurable.signal("web:1", "ping", %{})

    # nothing on this node executes it - the row waits for a worker node
    Process.sleep(150)
    assert %{status: "runnable"} = status(id)
  end

  test "a bad component option fails the boot loudly" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: msg}, _stack}} =
             GenDurable.Supervisor.start_link(name: BadBoot, repo: Repo, gc: [foo: 1])

    assert msg =~ "unknown :gc option"

    assert {:error, {%ArgumentError{}, _stack}} =
             GenDurable.Supervisor.start_link(name: BadBoot, repo: Repo, reaper: true)
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
         reaper: [interval: 150]
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
