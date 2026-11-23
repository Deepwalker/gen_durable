# A/B bench for the 0.2.16 hot-path changes (fencing token, admit fence join, in-txn
# credit). Version-agnostic: uses only APIs with the same shape on 0.2.15 and 0.2.16
# (`Queries.pick/7`, `Flusher.commit_sync/2`), so the SAME script runs on both checkouts:
#
#   docker compose -p gen_durable -f .devcontainer/docker-compose.yml exec -T app \
#     sh -lc "MIX_ENV=test mix run bench/gate_bench.exs"
#
# Uses a dedicated database (gen_durable_bench), never the test DB.
alias GenDurable.{Flusher, Queries}
alias GenDurable.Test.Repo

n_jobs = String.to_integer(System.get_env("JOBS", "20000"))
pickers = String.to_integer(System.get_env("PICKERS", "8"))
batch = String.to_integer(System.get_env("BATCH", "50"))
rounds = String.to_integer(System.get_env("ROUNDS", "3"))

cfg = Keyword.merge(Repo.config(), database: "gen_durable_bench", pool_size: pickers + 2)
Application.put_env(:gen_durable, Repo, cfg)
_ = Repo.__adapter__().storage_down(cfg)
:ok = Repo.__adapter__().storage_up(cfg)
{:ok, _} = Repo.start_link(cfg)
Ecto.Migrator.run(Repo, [{1, GenDurable.Test.Migrations.Setup}], :up, all: true, log: false)

limiter = {GenDurable.Limiter.Postgres, %{repo: Repo}}
config = %{repo: Repo, limiter: limiter, name: GenDurable.Bench}

params = fn key ->
  %{
    fsm: "bench", fsm_version: 1, step: "s", state_json: ~s({}), queue: "default",
    priority: 0, concurrency_key: key, correlation_key: nil, correlation_scope: [],
    rate_limit: nil, weight: 1, eligible_at: nil
  }
end

done_entry = fn job ->
  %{
    kind: :state, id: job.id, worker: job.worker, slot: job.slot, notify: false,
    status: "done", attempt: 0, delay_ms: 0, set_attempt: false, set_eligible: false,
    keep_lock: false, set_shard: true, shard_value: nil, lease_ttl_ms: 0, set_step: false,
    step: nil, set_state: false, state: nil, set_result: true, result: ~s({}),
    set_error: false, error: nil, clear_awaits: true, set_rate: false, rate_limit: nil,
    weight: 1.0, set_ck: false, ck_value: nil, consumed_ids: []
  }
end

reset = fn key, shards ->
  Repo.query!("TRUNCATE gen_durable, signals, gen_durable_buckets, gen_durable_bucket_configs RESTART IDENTITY CASCADE")

  if key do
    :ok = Queries.upsert_concurrency_configs(Repo, [%{name: key, capacity: 1_000_000, shards: shards}])
  end

  rows = for _ <- 1..n_jobs, do: params.(key)
  rows |> Enum.chunk_every(5_000) |> Enum.each(&Queries.insert_all(Repo, &1))
  Repo.query!("VACUUM ANALYZE gen_durable")
end

gates = fn key -> if key, do: [key], else: [] end

# One picker: pick → flush(done) until the queue is empty. Returns {picks_us, flushes_us}.
picker = fn i, key ->
  Stream.iterate(1, &(&1 + 1))
  |> Enum.reduce_while({[], []}, fn seq, {pt, ft} ->
    worker = "bench-#{i}##{seq}"
    {t1, jobs} = :timer.tc(fn -> Queries.pick(Repo, "default", batch, worker, 60_000, limiter, gates.(key)) end)

    case jobs do
      [] ->
        {:halt, {pt, ft}}

      jobs ->
        {t2, _} = :timer.tc(fn -> Flusher.commit_sync(config, Enum.map(jobs, done_entry)) end)
        {:cont, {[t1 | pt], [t2 | ft]}}
    end
  end)
end

pct = fn list, p ->
  s = Enum.sort(list)
  Enum.at(s, min(length(s) - 1, trunc(length(s) * p)))
end

run = fn label, key, shards ->
  results =
    for _ <- 1..rounds do
      reset.(key, shards)
      {us, per} =
        :timer.tc(fn ->
          1..pickers
          |> Task.async_stream(fn i -> picker.(i, key) end, timeout: :infinity, max_concurrency: pickers)
          |> Enum.map(fn {:ok, r} -> r end)
        end)

      %{rows: [[done]]} = Repo.query!("SELECT count(*) FROM gen_durable WHERE status = 'done'")
      true = done == n_jobs
      picks = per |> Enum.flat_map(&elem(&1, 0))
      flushes = per |> Enum.flat_map(&elem(&1, 1))
      %{jps: n_jobs / (us / 1_000_000), pick_p50: pct.(picks, 0.5), pick_p95: pct.(picks, 0.95),
        flush_p50: pct.(flushes, 0.5), flush_p95: pct.(flushes, 0.95)}
    end

  med = fn k -> results |> Enum.map(& &1[k]) |> Enum.sort() |> Enum.at(div(rounds, 2)) end

  num = fn v, w -> v |> round() |> Integer.to_string() |> String.pad_leading(w) end

  IO.puts(
    String.pad_trailing(label, 22) <> num.(med.(:jps), 7) <> " jobs/s  |  pick p50 " <>
      num.(med.(:pick_p50), 6) <> " p95 " <> num.(med.(:pick_p95), 6) <> " us  |  flush p50 " <>
      num.(med.(:flush_p50), 6) <> " p95 " <> num.(med.(:flush_p95), 6) <> " us"
  )
end

{:ok, vsn} = :application.get_key(:gen_durable, :vsn)
IO.puts("gen_durable #{vsn} — #{n_jobs} zero-length jobs, #{pickers} pickers, batch #{batch}, median of #{rounds}")
scenarios = String.split(System.get_env("SCENARIOS", "base,g1,g8"), ",")
if "base" in scenarios, do: run.("no key (baseline)", nil, 1)
if "g1" in scenarios, do: run.("gate, 1 shard", "api", 1)
if "g8" in scenarios, do: run.("gate, 8 shards", "api", 8)
