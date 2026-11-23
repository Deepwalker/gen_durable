defmodule GenDurable.FormalFindingsTest do
  @moduledoc """
  Regression tests for the findings of the formal models (`formal/`, ISSUES #31–#34). Each
  `describe` pins the code-level property that the model-checked fix relies on; the model
  instance that reproduces the original bug is named in it.
  """
  use ExUnit.Case, async: false
  use GenDurable.Testing, repo: GenDurable.Test.Repo, fsms: GenDurable.Test.FSMs.all()

  alias GenDurable.{Limiter, Queries, Scheduler}
  alias GenDurable.Test.Repo

  @ttl 60_000

  setup do
    Repo.query!("TRUNCATE gen_durable, signals RESTART IDENTITY CASCADE")
    Repo.query!("TRUNCATE gen_durable_buckets, gen_durable_bucket_configs")
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

  defp pg, do: {Limiter.Postgres, %{repo: Repo}}

  defp pick(worker, ttl \\ @ttl, gates \\ []),
    do: Queries.pick(Repo, "default", 10, worker, ttl, pg(), gates)

  defp done_entry(job) do
    %{
      kind: :state,
      id: job.id,
      worker: job.worker,
      slot: job.slot,
      notify: false,
      status: "done",
      attempt: 0,
      delay_ms: 0,
      set_attempt: false,
      set_eligible: false,
      keep_lock: false,
      set_shard: true,
      shard_value: nil,
      lease_ttl_ms: 0,
      set_step: false,
      step: nil,
      set_state: false,
      state: nil,
      set_result: true,
      result: ~s({}),
      set_error: false,
      error: nil,
      clear_awaits: true,
      set_rate: false,
      rate_limit: nil,
      weight: 1.0,
      set_ck: false,
      ck_value: nil,
      consumed_ids: []
    }
  end

  defp available(key) do
    %{rows: [[n]]} =
      Repo.query!(
        "SELECT available::int FROM gen_durable_buckets WHERE kind = 'conc' AND key = $1",
        [key]
      )

    n
  end

  defp row(id) do
    %{rows: [[status, locked_by, attempt, last_error]]} =
      Repo.query!(
        "SELECT status::text, locked_by, attempt, last_error FROM gen_durable WHERE id = $1",
        [id]
      )

    %{status: status, locked_by: locked_by, attempt: attempt, last_error: last_error}
  end

  defp credit_fun, do: fn slots -> Limiter.credit(pg(), slots) end

  # --- #31 -----------------------------------------------------------------------------

  describe "#31 PG gate credit is atomic with the row leaving executing (model: shipped)" do
    setup do
      :ok =
        Queries.upsert_concurrency_configs(Repo, [%{name: "api", capacity: 1, shards: 1}])

      :ok
    end

    test "the Postgres limiter credits inside the flush transaction" do
      assert Limiter.credit_in_txn?(pg())
    end

    test "the slot comes back in the flush; a reconcile afterwards cannot free it twice" do
      for _ <- 1..2, do: {:ok, _} = Queries.insert(Repo, params(%{concurrency_key: "api"}))

      [j1] = pick("w#1", @ttl, ["api"])
      assert available("api") == 0

      assert %{committed: [_]} = Queries.flush(Repo, [done_entry(j1)], credit_fun())
      # returned in the same transaction — nothing left "in flight" for reconcile to race
      assert available("api") == 1

      # the race the model found: reconcile, then an admit, then (formerly) the late credit
      Queries.reconcile_concurrency(Repo)
      [_j2] = pick("w#2", @ttl, ["api"])
      assert available("api") == 0
      Queries.reconcile_concurrency(Repo)
      assert available("api") == 0
      assert pick("w#3", @ttl, ["api"]) == []
    end

    test "a failing credit rolls the outcome back (one transaction)" do
      {:ok, id} = Queries.insert(Repo, params(%{concurrency_key: "api"}))
      [j] = pick("w#1", @ttl, ["api"])

      assert_raise RuntimeError, fn ->
        Queries.flush(Repo, [done_entry(j)], fn _ -> raise "credit failed" end)
      end

      assert %{status: "executing", locked_by: "w#1"} = row(id)
      assert available("api") == 0
    end
  end

  # --- #32 -----------------------------------------------------------------------------

  describe "#32 per-claim fencing token (model: shipped_pauses)" do
    test "the scheduler claims under a per-pick token prefixed by its incarnation" do
      assert Scheduler.claim_token("inst:q@vm-7", 3) == "inst:q@vm-7#3"
    end

    test "a zombie of the SAME incarnation cannot commit after its row was re-claimed" do
      {:ok, id} = Queries.insert(Repo, params())
      # pick #1 with an already-expired lease (a pause longer than lease_ttl)
      [old] = pick("inc#1", -1000)
      [^id] = Queries.reap(Repo)
      # the same incarnation re-claims it under pick #2
      [new] = pick("inc#2")

      assert %{stale: [^id], committed: []} = Queries.flush(Repo, [done_entry(old)])
      assert %{status: "executing", locked_by: "inc#2"} = row(id)
      assert %{committed: [^id]} = Queries.flush(Repo, [done_entry(new)])
    end

    test "incarnation-wide heartbeat and release still match every token of the incarnation" do
      {:ok, a} = Queries.insert(Repo, params())
      [_] = pick("inc#1", 1_000)
      {:ok, b} = Queries.insert(Repo, params())
      [_] = pick("inc#2", 1_000)

      :ok = Queries.heartbeat(Repo, [a, b], "inc", 120_000)

      %{rows: [[n]]} =
        Repo.query!(
          "SELECT count(*) FROM gen_durable WHERE lease_expires_at > now() + interval '60 seconds'"
        )

      assert n == 2

      # a different incarnation whose id is a string prefix of ours must not match
      :ok = Queries.release(Repo, [a, b], "in")
      assert row(a).status == "executing"

      :ok = Queries.release(Repo, [a, b], "inc")
      assert row(a).status == "runnable" and row(b).status == "runnable"
    end

    test "an admission for a claim that was taken back neither debits nor stamps" do
      :ok = Queries.upsert_concurrency_configs(Repo, [%{name: "api", capacity: 1, shards: 1}])
      {:ok, id} = Queries.insert(Repo, params(%{concurrency_key: "api"}))

      # claim, lose it to the reaper, someone else re-claims (gated rows: provisional shard)
      Repo.query!(
        "UPDATE gen_durable SET status = 'executing', locked_by = 'inc#2', concurrency_shard = 0 WHERE id = $1",
        [id]
      )

      %{admitted: [], denied: [^id]} =
        Limiter.admit(pg(), [%{id: id, worker: "inc#1", conc: "api", rate: nil}])

      %{rows: [[n]]} =
        Repo.query!(
          "SELECT count(*) FROM gen_durable_buckets WHERE kind = 'conc' AND key = 'api'"
        )

      assert n == 0
      assert row(id).locked_by == "inc#2"
    end
  end

  # --- #34 -----------------------------------------------------------------------------

  describe "#34 max_attempts bounds crash loops (model: retries_shipped)" do
    test "startup reclaim of a dead predecessor's claim counts as an attempt" do
      {:ok, id} = Queries.insert(Repo, params())
      [_] = pick("inst:default@vm-1#1", 1_000)

      assert Queries.reclaim_orphans(Repo, "default", "inst:default@vm-", 30_000) == 1
      assert %{status: "runnable", attempt: 1} = row(id)
    end

    test "a step whose attempt reached max_attempts fails without running user code" do
      {:ok, id} = GenDurable.insert(GenDurable.Test.JobResult, args: %{"n" => 1}, repo: Repo)
      # 20 runs already happened (reaped crashes) — the cap of a job defaults to 20
      Repo.query!("UPDATE gen_durable SET attempt = 20 WHERE id = $1", [id])

      drain()

      assert %{status: "failed", last_error: err} = row(id)
      assert err =~ "max_attempts (20) exceeded"
    end

    test "below the cap the step runs normally" do
      {:ok, id} = GenDurable.insert(GenDurable.Test.JobResult, args: %{"n" => 1}, repo: Repo)
      Repo.query!("UPDATE gen_durable SET attempt = 19 WHERE id = $1", [id])

      drain()

      assert_done(id, %{"doubled" => 2, "attempt" => 19})
    end

    test "an FSM without max_attempts stays uncapped" do
      assert GenDurable.Test.Counter.__gd_max_attempts__() == nil
      assert GenDurable.Test.JobResult.__gd_max_attempts__() == 20
      assert GenDurable.Test.JobGiveUp.__gd_max_attempts__() == 3
    end
  end
end
