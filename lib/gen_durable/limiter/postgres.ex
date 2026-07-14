defmodule GenDurable.Limiter.Postgres do
  @moduledoc """
  The default `GenDurable.Limiter` backend: the sharded `gen_durable_buckets` table.

  Same admission math as the old fused pick (the `c_*`/`r_*` CTEs), but run as standalone
  statements over an already-claimed batch instead of welded into the claim:

    * `admit/2` — sourced from the claimed batch (passed as arrays) instead of `cand`;
      RETURNS the admit decision per id (and the concurrency shard it drew) instead of
      claiming rows. Debits the taken rate tokens / concurrency slots.
    * `credit/2` — a standalone `available = LEAST(capacity, available + n)` per slot,
      replacing the outcome's `credit_gate` rider. `LEAST` (not the rider's bare `+ 1`)
      guards the out-of-band case: a credit racing a `reconcile` that already repaired the
      counter to full must not trip the `available <= capacity` CHECK.
    * `sync_config/2` / `reconcile/2` — delegate to the existing `Queries` machinery
      (`upsert_*_configs`, `gc_buckets` + `reconcile_concurrency`).

  The handle is `%{repo: repo}`.

  Retry discipline: the K=1 arbiter violation now aborts the CLAIM (handled in
  `Queries.pick`); `admit/2` can still trip the buckets CHECK (a residual gate
  over-admission, see `c_ranges`) or the buckets PK (two picks minting the same cold rate
  key), so it retries itself warm — the same three-strike, constraint-resolved discipline.
  """

  @behaviour GenDurable.Limiter

  alias GenDurable.Queries

  @impl true
  def sync_config(%{repo: repo}, configs) do
    rate =
      for {:rate, name, rate, burst, shards} <- configs,
          do: %{name: name, rate: rate, capacity: burst, shards: shards}

    conc =
      for {:conc, name, capacity, shards} <- configs,
          do: %{name: name, capacity: capacity, shards: shards}

    :ok = Queries.upsert_rate_configs(repo, rate)
    :ok = Queries.upsert_concurrency_configs(repo, conc)
    :ok
  end

  @impl true
  def reconcile(%{repo: repo}) do
    %{buckets: Queries.gc_buckets(repo), gates: Queries.reconcile_concurrency(repo)}
  end

  # Nothing to renew: a held slot IS the executing row, which the reconciler counts from the
  # `executing` truth. The row's own lease is renewed by the heartbeat.
  @impl true
  def renew(_handle, _slots), do: :ok

  @credit_sql """
  WITH d AS (
    SELECT key, shard, count(*) AS cnt
    FROM unnest($1::text[], $2::int[]) AS t(key, shard)
    GROUP BY key, shard
  )
  UPDATE gen_durable_buckets b
  SET available = LEAST(b.capacity, b.available + d.cnt)
  FROM d
  WHERE b.kind = 'conc' AND b.key = d.key AND b.shard = d.shard
  """

  @impl true
  def credit(_handle, []), do: :ok

  def credit(%{repo: repo}, slots) do
    {keys, shards} = Enum.unzip(slots)
    repo.query!(@credit_sql, [keys, shards])
    :ok
  end

  # Admission over a claimed batch. `$1..$4` are parallel arrays (id, conc_key, rkey,
  # weight); their ORDINALITY is the `(priority, eligible_at)` order the caller sorted by,
  # so `winners.rn` (per-key gate rank) and `rw.cw` (per-key cumulative rate weight) match
  # what the fused pick computed from `cand`. Returns one row per input id: its drawn
  # concurrency shard (NULL if not gated) and whether it was admitted. Debits only the
  # FINALLY-admitted (gate ∧ rate) — a gate slot reserved by a row that then fails rate is
  # left undebited, exactly as the fused pick's `c_writeback FROM claimed` did.
  @admit_sql """
  WITH input AS (
    SELECT id, conc_key, rkey, weight, ord
    FROM unnest($1::bigint[], $2::text[], $3::text[], $4::float8[])
         WITH ORDINALITY AS t(id, conc_key, rkey, weight, ord)
  ),
  winners AS (
    SELECT id, conc_key, ord,
           row_number() OVER (PARTITION BY conc_key ORDER BY ord) AS rn
    FROM input WHERE conc_key IS NOT NULL
  ),
  c_keys AS (SELECT DISTINCT conc_key AS key FROM winners),
  c_cold AS (
    SELECT k.key, s.shard,
           floor(cc.capacity / cc.shards)
             + CASE WHEN s.shard < (cc.capacity::bigint % cc.shards) THEN 1 ELSE 0 END AS cap
    FROM c_keys k
    JOIN gen_durable_bucket_configs cc ON cc.kind = 'conc' AND cc.name = split_part(k.key, ':', 1)
    CROSS JOIN LATERAL generate_series(0, cc.shards - 1) AS s(shard)
    WHERE NOT EXISTS (SELECT 1 FROM gen_durable_buckets b
                      WHERE b.kind = 'conc' AND b.key = k.key)
  ),
  c_locked AS (
    SELECT b.key, b.shard, b.available
    FROM gen_durable_buckets b
    JOIN c_keys k ON k.key = b.key
    WHERE b.kind = 'conc'
    ORDER BY b.key, b.shard
    FOR UPDATE OF b SKIP LOCKED
  ),
  c_ranges AS (
    SELECT key, shard, available,
           sum(available) OVER (PARTITION BY key ORDER BY available DESC, shard
                                ROWS UNBOUNDED PRECEDING) AS hi
    FROM (
      SELECT key, shard, min(available) AS available FROM c_locked GROUP BY key, shard
      UNION ALL
      SELECT key, shard, cap AS available FROM c_cold
    ) d
  ),
  c_admit AS (
    SELECT w.id, r.shard
    FROM winners w
    JOIN c_ranges r ON r.key = w.conc_key AND w.rn > r.hi - r.available AND w.rn <= r.hi
  ),
  elig AS (
    SELECT i.id, i.rkey, i.weight, i.ord
    FROM input i
    WHERE i.rkey IS NOT NULL
      AND (i.conc_key IS NULL OR i.id IN (SELECT id FROM c_admit))
  ),
  rw AS (
    SELECT id, rkey, weight,
           sum(weight) OVER (PARTITION BY rkey ORDER BY ord ROWS UNBOUNDED PRECEDING) AS cw
    FROM elig
  ),
  r_keys AS (SELECT DISTINCT rkey AS key FROM rw),
  r_cold AS (
    SELECT k.key, s.shard, cfg.capacity / cfg.shards AS shard_cap
    FROM r_keys k
    JOIN gen_durable_bucket_configs cfg ON cfg.kind = 'rate' AND cfg.name = split_part(k.key, ':', 1)
    CROSS JOIN LATERAL generate_series(0, cfg.shards - 1) AS s(shard)
    WHERE NOT EXISTS (SELECT 1 FROM gen_durable_buckets b WHERE b.kind = 'rate' AND b.key = k.key)
  ),
  r_locked AS (
    SELECT b.key, b.shard, b.available, b.last_refill,
           cfg.capacity / cfg.shards AS shard_cap, cfg.rate / cfg.shards AS shard_rate
    FROM gen_durable_buckets b
    JOIN r_keys k ON k.key = b.key
    JOIN gen_durable_bucket_configs cfg ON cfg.kind = 'rate' AND cfg.name = split_part(b.key, ':', 1)
    WHERE b.kind = 'rate'
    ORDER BY b.key, b.shard
    FOR UPDATE OF b SKIP LOCKED
  ),
  r_shards AS (
    SELECT key, shard,
           LEAST(shard_cap,
                 available + extract(epoch from clock_timestamp() - last_refill) * shard_rate) AS avail,
           shard_cap, false AS cold
    FROM r_locked
    UNION ALL
    SELECT key, shard, shard_cap AS avail, shard_cap, true AS cold
    FROM r_cold
  ),
  r_key_avail AS (SELECT key, sum(avail) AS total FROM r_shards GROUP BY key),
  granted AS (
    SELECT w.id, w.rkey, w.cw
    FROM rw w JOIN r_key_avail a ON a.key = w.rkey
    WHERE w.cw <= a.total
  ),
  admitted AS (
    SELECT i.id, ca.shard AS c_shard
    FROM input i
    LEFT JOIN c_admit ca ON ca.id = i.id
    LEFT JOIN granted g ON g.id = i.id
    WHERE (i.conc_key IS NULL OR ca.id IS NOT NULL)
      AND (i.rkey IS NULL OR g.id IS NOT NULL)
  ),
  c_debit AS (
    SELECT i.conc_key AS key, a.c_shard AS shard, count(*) AS cnt
    FROM admitted a JOIN input i ON i.id = a.id
    WHERE a.c_shard IS NOT NULL
    GROUP BY 1, 2
  ),
  c_writeback AS (
    UPDATE gen_durable_buckets b
    SET available = b.available - d.cnt
    FROM c_debit d
    WHERE b.kind = 'conc' AND b.key = d.key AND b.shard = d.shard
  ),
  c_mint AS (
    INSERT INTO gen_durable_buckets (kind, key, shard, capacity, available)
    SELECT 'conc', c.key, c.shard, c.cap, c.cap - coalesce(d.cnt, 0)
    FROM c_cold c
    LEFT JOIN c_debit d ON d.key = c.key AND d.shard = c.shard
    ORDER BY c.key, c.shard
    ON CONFLICT (kind, key, shard) DO UPDATE
      SET available = gen_durable_buckets.available - (EXCLUDED.capacity - EXCLUDED.available)
  ),
  consumed AS (
    SELECT i.rkey AS key, max(g.cw) AS consumed
    FROM admitted a JOIN input i ON i.id = a.id JOIN granted g ON g.id = a.id
    WHERE i.rkey IS NOT NULL
    GROUP BY i.rkey
  ),
  r_new AS (
    SELECT s.key, s.shard, s.shard_cap, s.cold,
           s.avail - CASE WHEN a.total > 0
                          THEN s.avail * coalesce(c.consumed, 0) / a.total
                          ELSE 0 END AS new_avail
    FROM r_shards s
    JOIN r_key_avail a ON a.key = s.key
    LEFT JOIN consumed c ON c.key = s.key
  ),
  r_writeback AS (
    UPDATE gen_durable_buckets b
    SET available = n.new_avail, last_refill = clock_timestamp(), capacity = n.shard_cap
    FROM r_new n
    WHERE b.kind = 'rate' AND b.key = n.key AND b.shard = n.shard AND NOT n.cold
  ),
  r_mint AS (
    INSERT INTO gen_durable_buckets (kind, key, shard, capacity, available, last_refill)
    SELECT 'rate', n.key, n.shard, n.shard_cap, n.new_avail, clock_timestamp()
    FROM r_new n
    WHERE n.cold
    ORDER BY n.key, n.shard
  ),
  -- Stamp the drawn shard onto the (already-executing) admitted rows, so `reconcile`
  -- can attribute each holder to its shard. PG-backend-only: the row is in this same DB.
  stamp AS (
    UPDATE gen_durable g
    SET concurrency_shard = a.c_shard
    FROM admitted a
    WHERE g.id = a.id AND a.c_shard IS NOT NULL
  )
  SELECT i.id,
         (SELECT a.c_shard FROM admitted a WHERE a.id = i.id) AS c_shard,
         EXISTS (SELECT 1 FROM admitted a WHERE a.id = i.id) AS ok
  FROM input i
  """

  @impl true
  def admit(handle, entries), do: admit(handle, entries, 3)

  defp admit(%{repo: repo} = handle, entries, attempts) do
    ids = Enum.map(entries, & &1.id)
    conc = Enum.map(entries, & &1.conc)
    rkey = Enum.map(entries, fn %{rate: {k, _w}} -> k; _ -> nil end)
    # weight is `double precision` in the schema (weighted rate limits), so the array is
    # float8[]; a non-rate entry's slot is unused (its rkey is NULL) but must still encode.
    weight = Enum.map(entries, fn %{rate: {_k, w}} -> w * 1.0; _ -> 1.0 end)

    %{rows: rows} = repo.query!(@admit_sql, [ids, conc, rkey, weight])
    by_id = Map.new(entries, &{&1.id, &1})

    Enum.reduce(rows, %{admitted: [], denied: []}, fn [id, c_shard, ok], acc ->
      if ok do
        slot = if c_shard, do: {by_id[id].conc, c_shard}, else: nil
        %{acc | admitted: [{id, slot} | acc.admitted]}
      else
        %{acc | denied: [id | acc.denied]}
      end
    end)
  rescue
    e in Postgrex.Error ->
      # The residual gate over-admission (buckets CHECK, from c_mint) or a cold rate-key
      # mint race (buckets PK, from r_mint) aborts the whole statement — retry warm, the
      # winner's row is committed by now. Same discipline the fused pick used.
      constraint = is_map(e.postgres) && e.postgres[:constraint]

      if attempts > 1 and constraint in ["gen_durable_buckets_check", "gen_durable_buckets_pkey"] do
        :telemetry.execute([:gen_durable, :concurrency, :contended], %{count: 1}, %{})
        admit(handle, entries, attempts - 1)
      else
        reraise e, __STACKTRACE__
      end
  end
end
