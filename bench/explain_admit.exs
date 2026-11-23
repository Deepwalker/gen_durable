# EXPLAIN (ANALYZE, BUFFERS) of the PG limiter's admit statement on a realistic table:
# ROWS terminal/runnable rows + a 50-row claimed gated batch. Runs in BEGIN/ROLLBACK.
#   MIX_ENV=test mix run bench/explain_admit.exs
alias GenDurable.Queries
alias GenDurable.Test.Repo

total = String.to_integer(System.get_env("ROWS", "1000000"))
cfg = Keyword.merge(Repo.config(), database: "gen_durable_bench", pool_size: 2)
{:ok, _} = Repo.start_link(cfg)

src = File.read!("lib/gen_durable/limiter/postgres.ex")
[_, sql] = Regex.run(~r/@admit_sql """\n(.*?)\n  """/s, src)

%{rows: [[n]]} = Repo.query!("SELECT count(*) FROM gen_durable")

if n < total do
  Repo.query!("TRUNCATE gen_durable, signals, gen_durable_buckets, gen_durable_bucket_configs RESTART IDENTITY CASCADE")
  Repo.query!("""
  INSERT INTO gen_durable (fsm, step, status, queue, concurrency_key)
  SELECT 'bench', 's', (CASE WHEN g % 10 = 0 THEN 'runnable' ELSE 'done' END)::durable_status,
         'default', NULL FROM generate_series(1, $1) g
  """, [total])
  :ok = Queries.upsert_concurrency_configs(Repo, [%{name: "api", capacity: 1_000_000, shards: 8}])
  Repo.query!("VACUUM ANALYZE gen_durable")
end

Repo.query!("UPDATE gen_durable SET status = 'runnable', locked_by = NULL, concurrency_key = NULL WHERE status = 'executing'")
%{rows: rows} =
  Repo.query!("""
  UPDATE gen_durable SET status = 'executing', locked_by = 'w#1', concurrency_key = 'api', concurrency_shard = 0
  WHERE id IN (SELECT id FROM gen_durable WHERE status = 'runnable' ORDER BY id LIMIT 50) RETURNING id
  """)

ids = List.flatten(rows)
k = length(ids)
params = [ids, List.duplicate("api", k), List.duplicate(nil, k), List.duplicate(1.0, k), List.duplicate("w#1", k)]
params = if String.contains?(sql, "$5"), do: params, else: Enum.take(params, 4)

for i <- 1..2 do
  Repo.transaction(fn ->
    %{rows: plan} = Repo.query!("EXPLAIN (ANALYZE, BUFFERS) " <> sql, params)
    if i == 2, do: plan |> List.flatten() |> Enum.each(&IO.puts/1)
    Repo.rollback(:done)
  end)
end
