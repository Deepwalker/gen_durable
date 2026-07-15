defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement, one function each, as raw SQL.

  All functions take the `repo` explicitly. Step outcomes commit through the
  batched `flush/1` (group commit — see `GenDurable.Flusher`): one guarded
  `UPDATE … FROM unnest(...)` for the whole batch plus the consume / parent-join /
  await-recheck / children-insert riders, in one transaction.
  `concurrency_key` serialization is enforced by the database — the UNIQUE
  partial index `gen_durable_concurrency_active` makes a second executing row
  per key uncommittable, so the pick's claim IS the lock (held exactly for the
  step window, released by any outcome or the reaper); no advisory locks, no
  pinned connections.

  Every statement has a static SQL text and goes through the connection-level
  prepared-statement cache (`cache_statement:`), so Postgres parses and plans it
  once per connection instead of on every call — bulk inserts pass rows as
  parallel arrays via `unnest` to keep the text static (and the parameter count
  fixed, clear of the 65535-parameter protocol cap). The one exception is
  `upsert_rate_configs` (dynamic VALUES, boot-time only). Hosts behind a
  transaction-pooling proxy set `prepare: :unnamed` on the repo to bypass the
  cache.

  JSON values (state, result, signal payloads) arrive here as encoded JSON text
  and are bound as TEXT parameters cast server-side (`$n::text::jsonb`). A bare
  `$n::jsonb` parameter would make the driver JSON-encode the already-encoded
  string, storing a double-encoded jsonb *scalar* instead of an object —
  invisible to `->>`/jsonb indexes. Rows written by versions that did exactly
  that still decode fine: the read paths accept both formats.
  """

  # --- shared insert shape -----------------------------------------------------
  # The 12 insert columns, used by insert / insert_all / the schedule_childs flush.

  @insert_cols "fsm, fsm_version, step, state, queue, priority, concurrency_key, eligible_at, correlation_key, correlation_scope, rate_limit, weight"

  # SELECT list decoding one unnest-ed row into the 12 columns above.
  # correlation_scope travels comma-joined and is split back server-side: unnest
  # zips scalar arrays element-wise, but an array-per-row column would need a
  # rectangular multidim array (enum labels contain no commas, so the join is safe).
  @unnest_row_select "SELECT t.fsm, t.fsm_version, t.step, t.state::jsonb, t.queue, t.priority, " <>
                       "t.concurrency_key, COALESCE(t.eligible_at, now()), t.correlation_key, " <>
                       "string_to_array(t.scope, ',')::durable_status[], t.rate_limit, t.weight"

  # The unnest source for @unnest_row_select: 12 parallel arrays at $base+1..$base+12.
  defp unnest_from(base) do
    "FROM unnest($#{base + 1}::text[], $#{base + 2}::int[], $#{base + 3}::text[], " <>
      "$#{base + 4}::text[], $#{base + 5}::text[], $#{base + 6}::int[], $#{base + 7}::text[], " <>
      "$#{base + 8}::timestamptz[], $#{base + 9}::text[], $#{base + 10}::text[], " <>
      "$#{base + 11}::text[], $#{base + 12}::float8[]) " <>
      "AS t(fsm, fsm_version, step, state, queue, priority, concurrency_key, eligible_at, " <>
      "correlation_key, scope, rate_limit, weight)"
  end

  # One list per insert column (parallel arrays for unnest), from a list of params maps.
  defp column_arrays(rows) do
    [
      Enum.map(rows, & &1.fsm),
      Enum.map(rows, & &1.fsm_version),
      Enum.map(rows, & &1.step),
      Enum.map(rows, & &1.state_json),
      Enum.map(rows, & &1.queue),
      Enum.map(rows, & &1.priority),
      Enum.map(rows, & &1.concurrency_key),
      Enum.map(rows, & &1.eligible_at),
      Enum.map(rows, & &1.correlation_key),
      Enum.map(rows, &Enum.join(&1.correlation_scope, ",")),
      Enum.map(rows, & &1.rate_limit),
      Enum.map(rows, & &1.weight)
    ]
  end

  # Every static statement goes through the connection-level prepared-statement
  # cache: parse + plan happen once per connection, not on every call. A host
  # behind a transaction-pooling proxy sets `prepare: :unnamed` on the repo,
  # which bypasses the cache gracefully. Dynamically-shaped SQL
  # (upsert_rate_configs) stays uncached.
  defp q!(repo, name, sql, params),
    do: repo.query!(sql, params, cache_statement: "gen_durable/" <> name)

  # --- picker ----------------------------------------------------------------

  # The claim: lock top-$2 runnable rows for the queue and flip them to `executing`,
  # WITHOUT admission. Admission for CONFIGURED limits is out-of-band now
  # (`GenDurable.Limiter`); only the K=1 dedup of UNCONFIGURED concurrency keys stays here,
  # as the `cand` NOT EXISTS guard plus the `gen_durable_concurrency_active` unique arbiter.
  #
  # NO per-row string work: gate membership is `concurrency_name = ANY($5)`, where
  # `concurrency_name` is the STORED generated split of `concurrency_key` (parsed ONCE at
  # write, see Migration change(3)) and $5 is the configured-gate name array threaded from
  # the caller's `config.concurrency_limit_names` — the same set the executor already trusts.
  # This replaced a per-candidate `split_part(...)` + a join to `gen_durable_bucket_configs`
  # (measured ~1.5 µs/candidate-row; PERFORMANCE.md §3). The pick no longer reads the configs
  # table at all — it stays the source of truth for the limiter/GC, not the hot claim.
  #
  #   cand    — top-$2 runnable rows by (priority, eligible_at), locked once
  #             (FOR NO KEY UPDATE SKIP LOCKED). A row whose concurrency_key is already
  #             executing is excluded UNLESS the key is a configured gate; NULL keys
  #             short-circuit. `row_number()` over the raw `concurrency_key` marks the
  #             most-urgent row per key (all-NULL keys land in one partition but are kept
  #             wholesale by the `IS NULL` branch below — no synthetic partition string).
  #   winners — keep a row when: its key is NULL (all pass), OR it is the rn=1 row of its
  #             key (unconfigured K=1 window dedup), OR it is `gated` (a CONFIGURED gate
  #             keeps ALL its candidates; the limiter trims them by capacity).
  #   claimed — flip winners to `executing`. This over-claims a saturated gate up to $2;
  #             the limiter denies the excess and `pick_claim` releases it back to runnable.
  #             The alternative — holding the row locks across the admit round-trip — is the
  #             contention we are removing. A cross-node race on an unconfigured key trips
  #             the K=1 arbiter and aborts the whole claim; `pick_claim` retries warm.
  #
  # Returns the job fields plus the admission inputs (gated?, rate_limit, weight).
  @claim_sql """
  WITH cand AS (
    SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at, gated,
           row_number() OVER (PARTITION BY concurrency_key
                              ORDER BY priority, eligible_at) AS rn
    FROM (
      SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at,
             (concurrency_name IS NOT NULL AND concurrency_name = ANY($5::text[])) AS gated
      FROM gen_durable g
      WHERE g.status = 'runnable' AND g.eligible_at <= now() AND g.queue = $1
        AND (g.concurrency_key IS NULL
             OR g.concurrency_name = ANY($5::text[])
             OR NOT EXISTS (
               SELECT 1 FROM gen_durable e
               WHERE e.concurrency_key = g.concurrency_key AND e.status = 'executing'))
      ORDER BY g.priority, g.eligible_at
      FOR NO KEY UPDATE SKIP LOCKED
      LIMIT $2
    ) s
  ),
  winners AS (
    SELECT id, concurrency_key, rate_limit, weight, priority, eligible_at, gated
    FROM cand
    WHERE concurrency_key IS NULL OR rn = 1 OR gated
  ),
  claimed AS (
    UPDATE gen_durable g
    SET status = 'executing', locked_by = $3,
        -- A gate carries a non-null shard so it drops OUT of the K=1 arbiter index
        -- (unconfigured keys keep NULL and stay in it). This is a provisional 0 held
        -- only across the admit round-trip; the limiter stamps the real shard. Provisional
        -- 0 is a valid shard, so a reconcile racing the window drifts safe (under-admit).
        concurrency_shard = CASE WHEN w.gated THEN 0 ELSE NULL END,
        lease_expires_at = now() + $4::int * interval '1 millisecond', updated_at = now()
    FROM winners w
    WHERE g.id = w.id
    RETURNING g.id, g.fsm, g.fsm_version, g.step, g.state, g.attempt, g.concurrency_key, g.awaits
  )
  SELECT c.id, c.fsm, c.fsm_version, c.step, c.state, c.attempt, c.concurrency_key, c.awaits,
         w.gated, w.rate_limit, w.weight
  FROM claimed c JOIN winners w ON w.id = c.id
  ORDER BY w.priority, w.eligible_at, c.id
  """

  # `gate_names` — the configured concurrency-gate names (`config.concurrency_limit_names`,
  # a list), bound as $5 for the `concurrency_name = ANY($5)` membership test. `[]` (the
  # default, and the Testing/perf callers' 5-arg form) means "no gates" — every non-null key
  # is unconfigured K=1, which `= ANY('{}')` yields as false, exactly.
  def pick(repo, queue, batch, worker, lease_ttl_ms, limiter \\ nil, gate_names \\ []) do
    limiter = limiter || {GenDurable.Limiter.Postgres, %{repo: repo}}
    pick_claim(repo, limiter, queue, batch, worker, lease_ttl_ms, gate_names, 3)
  end

  # Claim → admit (out-of-band) → keep admitted, release denied. Two failure regimes, kept
  # apart because the claim autocommits before admit runs:
  #   * the CLAIM's K=1 arbiter (gen_durable_concurrency_active) aborts the claim atomically
  #     (nothing committed) under a cross-node race on an unconfigured key — retry warm.
  #   * anything AFTER the claim commits (admit's buckets CHECK/PK once its own retries are
  #     spent, or a transient DB error in admit/release/enrich) would strand the claimed batch
  #     as `executing` until the reaper. The old fused pick rolled back atomically; we mirror
  #     it by releasing the whole batch before reraising (a debited slot heals via reconcile).
  defp pick_claim(repo, limiter, queue, batch, worker, lease_ttl_ms, gate_names, attempts) do
    case claim(repo, queue, batch, worker, lease_ttl_ms, gate_names) do
      {:contended} when attempts > 1 ->
        :telemetry.execute([:gen_durable, :concurrency, :contended], %{count: 1}, %{queue: queue})
        pick_claim(repo, limiter, queue, batch, worker, lease_ttl_ms, gate_names, attempts - 1)

      {:contended} ->
        []

      {:ok, rows} ->
        admit_claimed(repo, limiter, rows, queue, worker)
    end
  end

  defp claim(repo, queue, batch, worker, lease_ttl_ms, gate_names) do
    %{rows: rows} =
      q!(repo, "claim", @claim_sql, [queue, batch, worker, lease_ttl_ms, gate_names])

    {:ok, rows}
  rescue
    e in Postgrex.Error ->
      if is_map(e.postgres) && e.postgres[:constraint] == "gen_durable_concurrency_active",
        do: {:contended},
        else: reraise(e, __STACKTRACE__)
  end

  defp admit_claimed(repo, limiter, rows, queue, worker) do
    claimed = Enum.map(rows, &claimed_row(&1, worker))
    {needs_admit, plain} = Enum.split_with(claimed, &(&1.gated or &1.rate != nil))

    entries =
      Enum.map(needs_admit, fn c ->
        %{id: c.id, conc: if(c.gated, do: c.concurrency_key), rate: c.rate}
      end)

    %{admitted: admitted, denied: denied} = GenDurable.Limiter.admit(limiter, entries)

    slot_by_id = Map.new(admitted)
    admitted_ids = MapSet.new(admitted, fn {id, _slot} -> id end)

    if denied != [] do
      release_claims(repo, denied, worker)
      throttle_telemetry(needs_admit, admitted_ids, queue)
    end

    admitted_rows =
      for c <- needs_admit, MapSet.member?(admitted_ids, c.id), do: %{c | slot: slot_by_id[c.id]}

    jobs = Enum.map(plain ++ admitted_rows, &Map.drop(&1, [:gated, :rate]))
    enrich(repo, jobs)
  rescue
    e ->
      _ = safe_release(repo, Enum.map(rows, &hd/1), worker)
      reraise e, __STACKTRACE__
  end

  # Best-effort release (a DB still down leaves the reaper as the floor, as before the split).
  defp safe_release(repo, ids, worker) do
    release_claims(repo, ids, worker)
  rescue
    _ -> :ok
  end

  # A claimed row from @claim_sql. `:gated`/`:rate` drive admission and are dropped before the
  # job reaches the executor; `:slot` is the opaque handle the limiter returns for an admitted
  # gated row (backend-defined — PG `{key, shard}`, Redis `{key, id}`), `nil` otherwise.
  defp claimed_row(
         [
           id,
           fsm,
           fsm_version,
           step,
           state,
           attempt,
           concurrency_key,
           awaits,
           gated,
           rate_limit,
           weight
         ],
         worker
       ) do
    %{
      id: id,
      fsm: fsm,
      fsm_version: fsm_version,
      step: step,
      state: state,
      attempt: attempt,
      concurrency_key: concurrency_key,
      slot: nil,
      awaits: awaits,
      worker: worker,
      gated: gated,
      rate: if(rate_limit, do: {rate_limit, weight})
    }
  end

  # Release the over-claimed rows the limiter denied: back to `runnable`, untouched
  # otherwise (attempt/eligible_at kept, so priority order and retry count survive). They
  # never got a concurrency_shard (the limiter stamps only the admitted), so nothing to
  # credit. Guarded on the worker's ownership, like every outcome.
  defp release_claims(repo, ids, worker) do
    q!(
      repo,
      "release_claims",
      """
      UPDATE gen_durable
      SET status = 'runnable', locked_by = null, lease_expires_at = null,
          concurrency_shard = null, updated_at = now()
      WHERE id = ANY($1::bigint[]) AND locked_by = $2 AND status = 'executing'
      """,
      [ids, worker]
    )

    :ok
  end

  # A biting limit is observable: per key, how many wanted vs how many got in this pick.
  # A gate and a rate limit are reported separately; a row gated AND rated that the rate
  # side denies is counted under its concurrency key too (the split can't tell the two
  # apart from the admission result alone — a minor drift from the old fused telemetry).
  defp throttle_telemetry(needs_admit, admitted_ids, queue) do
    denied = Enum.reject(needs_admit, &MapSet.member?(admitted_ids, &1.id))

    denied
    |> Enum.filter(& &1.gated)
    |> Enum.frequencies_by(& &1.concurrency_key)
    |> Enum.each(fn {key, denied_n} ->
      wanted = Enum.count(needs_admit, &(&1.gated and &1.concurrency_key == key))

      :telemetry.execute(
        [:gen_durable, :concurrency, :throttled],
        %{wanted: wanted, admitted: wanted - denied_n},
        %{key: key, queue: queue}
      )
    end)

    denied
    |> Enum.filter(&(&1.rate != nil))
    |> Enum.frequencies_by(fn %{rate: {key, _w}} -> key end)
    |> Enum.each(fn {key, denied_n} ->
      wanted = Enum.count(needs_admit, fn c -> match?({^key, _}, c.rate) end)

      :telemetry.execute(
        [:gen_durable, :rate_limit, :throttled],
        %{wanted: wanted, granted: wanted - denied_n},
        %{key: key, queue: queue}
      )
    end)
  end

  # Attach each claimed job's signal inbox and children — two statements per
  # BATCH, not two per job (the per-step loads were pure round-trip tax: the
  # probes usually return nothing, but each cost a network hop and a pool slot).
  # The inbox snapshot is therefore taken at pick time, not execution start;
  # consumption stays exact either way (a progressing outcome deletes the
  # `ctx.awaited` ids the step actually saw — a signal landing after the pick
  # just stays in the inbox for the next wake, same as one landing mid-step).
  # With `prefetch > 0`, buffered jobs hold their snapshot until they run.
  defp enrich(_repo, []), do: []

  defp enrich(repo, jobs) do
    ids = Enum.map(jobs, & &1.id)

    %{rows: sig_rows} =
      q!(
        repo,
        "batch_signals",
        "SELECT target_id, id, name, payload FROM signals WHERE target_id = ANY($1) ORDER BY id",
        [ids]
      )

    signals =
      Enum.group_by(sig_rows, &hd/1, fn [_, id, name, payload] ->
        %{id: id, name: name, payload: decode_json(payload)}
      end)

    %{rows: child_rows} =
      q!(
        repo,
        "batch_childs",
        """
        SELECT parent_id, id, fsm, status::text, state, result, last_error
        FROM gen_durable WHERE parent_id = ANY($1) ORDER BY id
        """,
        [ids]
      )

    childs =
      Enum.group_by(child_rows, &hd/1, fn [_, id, fsm, status, state, result, last_error] ->
        %{
          id: id,
          fsm: fsm,
          status: status,
          state: decode_json(state),
          result: decode_json_or_nil(result),
          last_error: last_error
        }
      end)

    Enum.map(jobs, fn job ->
      job
      |> Map.put(:signals, Map.get(signals, job.id, []))
      |> Map.put(:childs, Map.get(childs, job.id, []))
    end)
  end

  # --- lease / reaper --------------------------------------------------------
  #
  # Every multi-row maintenance statement (heartbeat, reap, release, startup
  # reclaim, bucket GC) claims its rows first via
  # `SELECT … ORDER BY … FOR [NO KEY] UPDATE SKIP LOCKED`, then updates/deletes
  # the claimed set. Never waiting (SKIP LOCKED) plus deterministic order means
  # no two maintenance statements can deadlock against each other — an unordered
  # multi-row UPDATE locks rows in plan order, and e.g. a late heartbeat
  # (id order) overlapping the reaper (lease-index order) on two expired rows
  # would cycle. A row that is locked right now is being actively worked (its
  # outcome committing, a beat extending it) — exactly when maintenance should
  # leave it alone until the next tick.
  #
  # gen_durable claims lock with FOR NO KEY UPDATE — strong enough for mutual
  # exclusion between claims and outcomes (writer-vs-writer locks conflict as
  # before), but compatible with the FOR KEY SHARE that FK checks take on the
  # target row (every `INSERT INTO signals`). With plain FOR UPDATE an
  # in-flight signal insert's KEY SHARE made the pick skip its target row
  # (SKIP LOCKED) — pure friction, no correctness. What this does NOT change:
  # signal delivery still queues behind a claim at its wake UPDATE, which
  # writes the row — no lock strength avoids that. Safe from lock-upgrade
  # deadlocks BY SCHEMA: Postgres strengthens an UPDATE's row lock only when it
  # modifies columns of a FULL unique index, our only full unique index is the
  # immutable PK (correlation/concurrency uniques are partial — excluded), so
  # no statement ever escalates past NO KEY strength. Adding a full unique
  # index over a MUTABLE column would break this — revisit these locks then.
  # Bucket-table locks stay FOR UPDATE: no FKs point at buckets, so there is
  # no KEY SHARE traffic to coexist with.

  def heartbeat(_repo, [], _worker, _ttl), do: :ok

  def heartbeat(repo, ids, worker, lease_ttl_ms) when is_list(ids) do
    q!(
      repo,
      "heartbeat",
      """
      WITH mine AS (
        SELECT id FROM gen_durable
        WHERE id = ANY($1) AND locked_by = $2
        ORDER BY id
        FOR NO KEY UPDATE SKIP LOCKED
      )
      UPDATE gen_durable g
      SET lease_expires_at = now() + $3::int * interval '1 millisecond', updated_at = now()
      FROM mine m
      WHERE g.id = m.id
      """,
      [ids, worker, lease_ttl_ms]
    )

    :ok
  end

  def reap(repo) do
    %{rows: rows} =
      q!(
        repo,
        "reap",
        """
        WITH doomed AS (
          SELECT id FROM gen_durable
          WHERE status = 'executing' AND lease_expires_at < now()
          ORDER BY id
          FOR NO KEY UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', locked_by = null, lease_expires_at = null,
            attempt = attempt + 1, updated_at = now()
        FROM doomed d
        WHERE g.id = d.id
        RETURNING g.id
        """,
        []
      )

    List.flatten(rows)
  end

  # Wake parks whose await timeout fired: expired `await_deadline` ⇒ runnable,
  # deadline cleared, `awaits`/`attempt` KEPT (the woken step reads the inbox
  # through `awaits` as usual — an empty ctx.awaited is the timeout signal; this
  # is a wake, not a failure). Ordered SKIP LOCKED per the maintenance note above.
  def expire_awaits(repo) do
    %{num_rows: n} =
      q!(
        repo,
        "expire_awaits",
        """
        WITH expired AS (
          SELECT id FROM gen_durable
          WHERE status = 'awaiting_signal' AND await_deadline < now()
          ORDER BY id
          FOR NO KEY UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', eligible_at = now(), await_deadline = null, updated_at = now()
        FROM expired e
        WHERE g.id = e.id
        """,
        []
      )

    n
  end

  # Garbage-collect terminal instances. Deletes up to `batch` `done`/`failed`
  # rows whose `updated_at` (their termination instant — terminal rows are immutable)
  # is older than `retention_ms`. The `NOT EXISTS` guard spares a terminal child whose
  # parent is still active (`awaiting_children`/runnable/executing): the parent may yet
  # read it via `ctx.childs` on the join. A deleted parent SET-NULLs its
  # children's `parent_id` (FK), and `signals` cascade-delete. Returns the count deleted.
  # Two round-trips on purpose (GC is a background sweep, not latency-critical):
  # collect ≤ `batch` doomed ids, then delete them by `id = ANY($ids)`. A single
  # `DELETE … WHERE id IN (subquery)` / `USING` makes the planner Seq Scan the whole
  # table to match the small id set — O(table) per sweep, seconds on a 100M-row table.
  # `id = ANY($ids)` is a PK Index Scan instead — O(batch) (~50ms for 10k incl. FK
  # cascades; see PERFORMANCE.md §4b). Terminal rows are immutable, so the ids stay
  # valid between the two statements.
  def gc(repo, retention_ms, batch) do
    %{rows: rows} =
      q!(
        repo,
        "gc_doomed",
        """
        SELECT g.id FROM gen_durable g
        WHERE g.status IN ('done', 'failed')
          AND g.updated_at < now() - $1::int * interval '1 millisecond'
          AND NOT EXISTS (
            SELECT 1 FROM gen_durable p
            WHERE p.id = g.parent_id AND p.status NOT IN ('done', 'failed')
          )
        LIMIT $2
        """,
        [retention_ms, batch]
      )

    case List.flatten(rows) do
      [] ->
        0

      ids ->
        %{num_rows: n} =
          q!(repo, "gc_delete", "DELETE FROM gen_durable WHERE id = ANY($1::bigint[])", [ids])

        n
    end
  end

  # Sweep stale rate buckets — the GC side of partitioned/sharded limits
  # (`{name, partition}` mints a bucket per partition ever seen, each split into
  # `shards` rows). A key's shards are swept ALL-or-NOTHING so the pick's cold
  # path (`r_cold` fires only when a key has NO rows) never faces a partial
  # shard set: a key is deletable when its config is gone, or when EVERY shard
  # is present, lockable, and idle longer than burst/rate seconds (fully
  # refilled by now — recreating it at full-minus-taken loses nothing). rate = 0
  # never qualifies (it never refills). The ordered SKIP LOCKED lock means a
  # shard a concurrent pick holds is skipped — active, not stale — which also
  # drops its key from the all-shards-present test that round.
  def gc_buckets(repo) do
    %{num_rows: n} =
      q!(
        repo,
        "gc_buckets",
        """
        WITH locked AS (
          SELECT b.key, b.last_refill,
                 (SELECT count(*) FROM gen_durable_buckets x
                  WHERE x.kind = 'rate' AND x.key = b.key) AS total_shards,
                 cfg.name AS cfg_name, cfg.rate AS rate, cfg.capacity AS capacity
          FROM gen_durable_buckets b
          LEFT JOIN gen_durable_bucket_configs cfg
                 ON cfg.kind = 'rate' AND cfg.name = split_part(b.key, ':', 1)
          WHERE b.kind = 'rate'
          ORDER BY b.key, b.shard
          FOR UPDATE OF b SKIP LOCKED
        ),
        doomed AS (
          SELECT key FROM locked
          GROUP BY key
          HAVING bool_or(cfg_name IS NULL)
              OR (count(*) = max(total_shards)
                  AND bool_and(rate > 0
                        AND last_refill < now() - make_interval(secs => capacity / rate)))
        )
        DELETE FROM gen_durable_buckets b
        USING doomed d
        WHERE b.kind = 'rate' AND b.key = d.key
        """,
        []
      )

    n
  end

  # --- enrichment (shared with the pick) -------------------------------------

  # Reload the signal inbox and children snapshot for already-claimed jobs — the same
  # batched enrichment the pick runs, exposed for the inline-continuation path (a run-ahead
  # step gets a fresh snapshot, identical to what a re-pick would hand it).
  def enrich_jobs(repo, jobs), do: enrich(repo, jobs)

  # --- batched flush (group commit) ------------------------------------------
  #
  # One transaction commits a WHOLE batch of outcomes, replacing the per-instance
  # complete_* round-trips (see GenDurable.Flusher). A single guarded
  # `UPDATE … FROM unnest(...)` applies every row's state transition — the kind is
  # encoded per row via set_* flags and precomputed values — and the two riders
  # (signal consume, parent-join decrement) follow as their own statements, scoped
  # to the rows that actually committed (the guard's RETURNING). No CTE stack: the
  # dedup/aggregation (which parent gets −cnt, which signals to drop) is resolved
  # in Elixir between the statements, in one place.
  #
  # Covers the four kinds that LEAVE `executing` — :next (requeue), :retry, :done,
  # :stop — so the multi-row UPDATE never trips the K=1 arbiter
  # (gen_durable_concurrency_active is a WHERE status='executing' partial index).
  # :await and :schedule_childs carry extra per-row riders and are folded in
  # separately; inline-continue keeps the row executing and rides its own variant.
  #
  # Entry arrays are sorted by id (parents by parent_id in `notify`) as best-effort
  # lock ordering — but a set-based `UPDATE … FROM unnest` acquires row locks in
  # PLAN order, so the sort is not the real guarantee. Deadlock-safety comes from
  # concurrent flushers owning DISJOINT queue (row) sets and executing the same
  # cached plan, so any rows they do share (parent rows in `notify`, up an acyclic
  # tree) are locked in one deterministic order. The sort just keeps that order
  # aligned with the maintenance statements' id-ordered SKIP LOCKED.
  #
  # `entries` — a list of maps, one per outcome (see GenDurable.Flusher.entry/2).
  # Returns %{committed: [id], woken_queues: [queue], stale: [id]}.
  @flush_update_sql """
  UPDATE gen_durable g SET
    status = u.status::durable_status,
    step = CASE WHEN u.set_step THEN u.step ELSE g.step END,
    state = CASE WHEN u.set_state THEN u.state::jsonb ELSE g.state END,
    result = CASE WHEN u.set_result THEN u.result::jsonb ELSE g.result END,
    last_error = CASE WHEN u.set_error THEN u.error ELSE g.last_error END,
    eligible_at = CASE WHEN u.set_eligible
                       THEN now() + u.delay_ms * interval '1 millisecond' ELSE g.eligible_at END,
    attempt = CASE WHEN u.set_attempt THEN u.attempt ELSE g.attempt END,
    awaits = CASE WHEN u.clear_awaits THEN NULL ELSE g.awaits END,
    rate_limit = CASE WHEN u.set_rate THEN u.rate_limit ELSE g.rate_limit END,
    weight = CASE WHEN u.set_rate THEN u.weight ELSE g.weight END,
    concurrency_key = CASE WHEN u.set_ck THEN u.ck_value ELSE g.concurrency_key END,
    concurrency_shard = CASE WHEN u.set_shard THEN u.shard_value ELSE g.concurrency_shard END,
    -- inline-continue (keep_lock) keeps the claim and extends the lease; every other
    -- kind leaves executing, releasing the lock.
    locked_by = CASE WHEN u.keep_lock THEN g.locked_by ELSE NULL END,
    lease_expires_at = CASE WHEN u.keep_lock
                            THEN now() + u.lease_ttl_ms * interval '1 millisecond' ELSE NULL END,
    updated_at = now()
  FROM unnest(
    $1::bigint[], $2::text[], $3::text[], $4::int[], $5::int[],
    $6::bool[], $7::text[], $8::bool[], $9::text[], $10::bool[], $11::text[],
    $12::bool[], $13::text[], $14::bool[], $15::bool[], $16::text[], $17::float8[],
    $18::bool[], $19::text[], $20::bool[], $21::bool[],
    $22::bool[], $23::bool[], $24::int[], $25::int[]
  ) AS u(id, worker, status, attempt, delay_ms,
         set_step, step, set_state, state, set_result, result,
         set_error, error, clear_awaits, set_rate, rate_limit, weight,
         set_ck, ck_value, set_attempt, set_eligible,
         keep_lock, set_shard, shard_value, lease_ttl_ms)
  WHERE g.id = u.id AND g.locked_by = u.worker AND g.status = 'executing'
  RETURNING g.id, g.parent_id, g.status::text
  """

  @flush_notify_sql """
  UPDATE gen_durable p SET
    children_pending = p.children_pending - d.cnt,
    status = CASE WHEN p.children_pending - d.cnt <= 0 AND p.status = 'awaiting_children'
                  THEN 'runnable' ELSE p.status END,
    eligible_at = CASE WHEN p.children_pending - d.cnt <= 0 AND p.status = 'awaiting_children'
                       THEN now() ELSE p.eligible_at END,
    updated_at = now()
  FROM unnest($1::bigint[], $2::int[]) AS d(parent_id, cnt)
  WHERE p.id = d.parent_id
  RETURNING CASE WHEN p.status = 'runnable' THEN p.queue ELSE NULL END
  """

  # :await park — the await park + lost-wakeup recheck, batched. `awaits` rides in
  # as a JSON array text per row (robust to commas in signal names, unlike the
  # comma-join trick) and is decoded server-side. A NULL timeout ⇒ NULL deadline.
  @flush_await_park_sql """
  UPDATE gen_durable g SET
    step = u.step, state = u.state::jsonb,
    awaits = ARRAY(SELECT jsonb_array_elements_text(u.awaits::jsonb)),
    status = 'awaiting_signal', eligible_at = now(), attempt = 0, rate_limit = NULL, weight = 1,
    concurrency_shard = NULL,
    await_deadline = now() + u.timeout_ms * interval '1 millisecond',
    locked_by = NULL, lease_expires_at = NULL, updated_at = now()
  FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::int[], $6::text[])
    AS u(id, worker, step, state, timeout_ms, awaits)
  WHERE g.id = u.id AND g.locked_by = u.worker AND g.status = 'executing'
  RETURNING g.id
  """

  # :await recheck — the delivery side of the lost-wakeup fix, batched over the
  # parked ids, in the SAME transaction as the park (park holds the row lock; a
  # racing delivery queues on it). A matching signal already in the inbox (that
  # the step did NOT already see — the per-row `presented` pairs) flips the row
  # straight back to runnable. READ COMMITTED gives the recheck a fresh snapshot.
  @flush_await_recheck_sql """
  UPDATE gen_durable g SET status = 'runnable', updated_at = now()
  FROM unnest($1::bigint[]) AS a(id)
  WHERE g.id = a.id AND g.status = 'awaiting_signal'
    AND EXISTS (
      SELECT 1 FROM signals s
      WHERE s.target_id = g.id AND s.name = ANY(g.awaits)
        AND NOT EXISTS (
          SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS p(tid, sid)
          WHERE p.tid = g.id AND p.sid = s.id
        )
    )
  """

  def flush(_repo, []), do: %{committed: [], woken_queues: [], stale: []}

  def flush(repo, entries) do
    # Lock in id order (deadlock discipline shared with the maintenance statements).
    entries = Enum.sort_by(entries, & &1.id)
    by_kind = Enum.group_by(entries, &Map.get(&1, :kind, :state))

    {:ok, result} =
      repo.transaction(fn ->
        # Each kind's state transition is its own batched statement; the riders
        # (consume, notify) then run over the rows that actually committed.
        committed =
          flush_state(repo, Map.get(by_kind, :state, [])) ++
            flush_await(repo, Map.get(by_kind, :await, [])) ++
            flush_schedule_childs(repo, Map.get(by_kind, :schedule_childs, []))

        committed_ids = MapSet.new(committed, fn {id, _, _} -> id end)

        flush_consume(repo, entries, committed, committed_ids)
        woken = flush_notify(repo, committed)

        stale = for e <- entries, not MapSet.member?(committed_ids, e.id), do: e.id

        %{committed: MapSet.to_list(committed_ids), woken_queues: woken, stale: stale}
      end)

    result
  end

  # The batched state transition for :next/:retry/:done/:stop. Returns
  # {id, parent_id, status} for every row that committed (guard held).
  defp flush_state(_repo, []), do: []

  defp flush_state(repo, entries) do
    %{rows: returned} =
      q!(repo, "flush_update", @flush_update_sql, [
        Enum.map(entries, & &1.id),
        Enum.map(entries, & &1.worker),
        Enum.map(entries, & &1.status),
        Enum.map(entries, & &1.attempt),
        Enum.map(entries, & &1.delay_ms),
        Enum.map(entries, & &1.set_step),
        Enum.map(entries, & &1.step),
        Enum.map(entries, & &1.set_state),
        Enum.map(entries, & &1.state),
        Enum.map(entries, & &1.set_result),
        Enum.map(entries, & &1.result),
        Enum.map(entries, & &1.set_error),
        Enum.map(entries, & &1.error),
        Enum.map(entries, & &1.clear_awaits),
        Enum.map(entries, & &1.set_rate),
        Enum.map(entries, & &1.rate_limit),
        Enum.map(entries, & &1.weight),
        Enum.map(entries, & &1.set_ck),
        Enum.map(entries, & &1.ck_value),
        Enum.map(entries, & &1.set_attempt),
        Enum.map(entries, & &1.set_eligible),
        Enum.map(entries, & &1.keep_lock),
        Enum.map(entries, & &1.set_shard),
        Enum.map(entries, & &1.shard_value),
        Enum.map(entries, & &1.lease_ttl_ms)
      ])

    Enum.map(returned, fn [id, parent_id, status] -> {id, parent_id, status} end)
  end

  # :await — park (all rows) then recheck (the parked ids, with per-row presented
  # exclusion). Returns {id, nil, "awaiting_signal"} for parked rows (a recheck may
  # have flipped some back to runnable — harmless, they get re-picked).
  defp flush_await(_repo, []), do: []

  defp flush_await(repo, entries) do
    %{rows: parked} =
      q!(repo, "flush_await_park", @flush_await_park_sql, [
        Enum.map(entries, & &1.id),
        Enum.map(entries, & &1.worker),
        Enum.map(entries, & &1.step),
        Enum.map(entries, & &1.state),
        Enum.map(entries, & &1.timeout_ms),
        Enum.map(entries, & &1.awaits_json)
      ])

    parked_ids = List.flatten(parked)
    parked_set = MapSet.new(parked_ids)

    {ptids, psids} =
      entries
      |> Enum.filter(&(MapSet.member?(parked_set, &1.id) and &1.presented_ids != []))
      |> Enum.flat_map(fn e -> Enum.map(e.presented_ids, &{e.id, &1}) end)
      |> Enum.unzip()

    if parked_ids != [],
      do: q!(repo, "flush_await_recheck", @flush_await_recheck_sql, [parked_ids, ptids, psids])

    for id <- parked_ids, do: {id, nil, "awaiting_signal"}
  end

  # :schedule_childs — batched across parents. First insert every child (each gated on
  # ITS parent's ownership, RETURNING parent_id), then park each parent with the count
  # of children that actually landed (post-dedup) — awaiting_children, or runnable when
  # zero. The parent's own awaited-signal consume rides the generic consume (its entry
  # carries consumed_ids). Children insert ORDER BY correlation_key (arbiter-deadlock
  # discipline, as insert_all); parents park in id order.
  @flush_childs_insert_sql "INSERT INTO gen_durable (#{@insert_cols}, parent_id) " <>
                             "SELECT t.fsm, t.fsm_version, t.step, t.state::jsonb, t.queue, t.priority, " <>
                             "t.concurrency_key, COALESCE(t.eligible_at, now()), t.correlation_key, " <>
                             "string_to_array(t.scope, ',')::durable_status[], t.rate_limit, t.weight, t.parent_id " <>
                             "FROM unnest($1::text[], $2::int[], $3::text[], $4::text[], $5::text[], $6::int[], " <>
                             "$7::text[], $8::timestamptz[], $9::text[], $10::text[], $11::text[], $12::float8[], " <>
                             "$13::bigint[], $14::text[]) AS t(fsm, fsm_version, step, state, queue, priority, " <>
                             "concurrency_key, eligible_at, correlation_key, scope, rate_limit, weight, parent_id, worker) " <>
                             "WHERE EXISTS (SELECT 1 FROM gen_durable p WHERE p.id = t.parent_id " <>
                             "AND p.locked_by = t.worker AND p.status = 'executing') " <>
                             "ORDER BY t.correlation_key " <>
                             "ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING " <>
                             "RETURNING parent_id"

  @flush_childs_park_sql """
  UPDATE gen_durable p SET
    step = u.step, state = u.state::jsonb, children_pending = u.cnt,
    status = (CASE WHEN u.cnt = 0 THEN 'runnable' ELSE 'awaiting_children' END)::durable_status,
    eligible_at = now(), attempt = 0, awaits = NULL, rate_limit = NULL, weight = 1,
    concurrency_shard = NULL, locked_by = NULL, lease_expires_at = NULL, updated_at = now()
  FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::int[])
    AS u(id, worker, step, state, cnt)
  WHERE p.id = u.id AND p.locked_by = u.worker AND p.status = 'executing'
  RETURNING p.id
  """

  # Lock the parents FIRST, for the whole flush transaction — the old
  # complete_schedule_childs was ONE statement (a `claim … FOR NO KEY UPDATE`
  # gated the child insert and the parent park), so the reaper's ordered
  # `FOR NO KEY UPDATE SKIP LOCKED` skipped a parent mid-spawn. Splitting the
  # insert (whose FK takes only `FOR KEY SHARE` — compatible with the reaper's
  # `NO KEY UPDATE`) from the park would otherwise let the reaper reclaim a
  # parent between them, committing orphan children whose `parent_id` decrements a
  # join barrier the reclaimed parent will re-arm — a join-barrier violation. This
  # NO KEY UPDATE lock (id order, matching the reaper) makes the reaper SKIP the
  # parent, restoring the all-or-nothing property.
  @flush_childs_lock_sql "SELECT id FROM gen_durable WHERE id = ANY($1::bigint[]) " <>
                           "AND status = 'executing' ORDER BY id FOR NO KEY UPDATE"

  defp flush_schedule_childs(_repo, []), do: []

  defp flush_schedule_childs(repo, entries) do
    q!(repo, "flush_childs_lock", @flush_childs_lock_sql, [Enum.map(entries, & &1.id)])

    flat = for e <- entries, child <- e.children, do: {child, e.id, e.worker}

    counts =
      case flat do
        [] ->
          %{}

        _ ->
          children = Enum.map(flat, fn {c, _, _} -> c end)
          parent_ids = Enum.map(flat, fn {_, pid, _} -> pid end)
          workers = Enum.map(flat, fn {_, _, w} -> w end)

          %{rows: rows} =
            q!(repo, "flush_childs_insert", @flush_childs_insert_sql, column_arrays(children) ++ [parent_ids, workers])

          rows |> List.flatten() |> Enum.frequencies()
      end

    %{rows: parked} =
      q!(repo, "flush_childs_park", @flush_childs_park_sql, [
        Enum.map(entries, & &1.id),
        Enum.map(entries, & &1.worker),
        Enum.map(entries, & &1.step),
        Enum.map(entries, & &1.state),
        Enum.map(entries, fn e -> Map.get(counts, e.id, 0) end)
      ])

    for [id] <- parked,
        do: {id, nil, if(Map.get(counts, id, 0) == 0, do: "runnable", else: "awaiting_children")}
  end

  # consume — terminal rows drop their whole inbox; progressing rows drop the exact
  # awaited ids they saw. Both scoped to committed (a stale row's signals belong to
  # its new claimant; an :await/:retry consumes nothing — empty consumed_ids).
  defp flush_consume(repo, entries, committed, committed_ids) do
    terminal_ids = for {id, _p, s} <- committed, s in ["done", "failed"], do: id

    if terminal_ids != [],
      do:
        q!(
          repo,
          "flush_consume_terminal",
          "DELETE FROM signals WHERE target_id = ANY($1::bigint[])",
          [terminal_ids]
        )

    {tids, sids} =
      entries
      |> Enum.filter(&(MapSet.member?(committed_ids, &1.id) and &1.consumed_ids != []))
      |> Enum.flat_map(fn e -> Enum.map(e.consumed_ids, &{e.id, &1}) end)
      |> Enum.unzip()

    if tids != [],
      do:
        q!(
          repo,
          "flush_consume_pairs",
          "DELETE FROM signals s USING unnest($1::bigint[], $2::bigint[]) AS d(tid, sid) " <>
            "WHERE s.target_id = d.tid AND s.id = d.sid",
          [tids, sids]
        )

    :ok
  end

  # notify — one aggregated decrement per parent (dedup collapses N siblings in this
  # batch into a single −cnt on the parent row). parent_id order for the lock
  # discipline. Returns the queues of parents this batch woke (their join completed).
  defp flush_notify(repo, committed) do
    committed
    |> Enum.filter(fn {_id, p, s} -> not is_nil(p) and s in ["done", "failed"] end)
    |> Enum.frequencies_by(fn {_id, p, _s} -> p end)
    |> Enum.sort()
    |> case do
      [] ->
        []

      pairs ->
        {pids, cnts} = Enum.unzip(pairs)
        %{rows: wrows} = q!(repo, "flush_notify", @flush_notify_sql, [pids, cnts])
        Enum.uniq(for [q] <- wrows, not is_nil(q), do: q)
    end
  end

  # --- signals ---------------------------------------------------------------

  def load_signals(repo, target_id) do
    %{rows: rows} =
      q!(
        repo,
        "load_signals",
        "SELECT id, name, payload FROM signals WHERE target_id = $1 ORDER BY id",
        [target_id]
      )

    Enum.map(rows, fn [id, name, payload] ->
      %{id: id, name: name, payload: decode_json(payload)}
    end)
  end

  # --- await -----------------------------------------------------------------

  # One instance's await-relevant snapshot; nil when the row does not exist
  # (never did, or GC swept it). Plain MVCC read — takes no locks, sees the
  # last committed version, cannot block or be blocked by claims and outcomes.
  def await_status(repo, id) do
    %{rows: rows} =
      q!(
        repo,
        "await_status",
        "SELECT status::text, step, attempt, result, last_error FROM gen_durable WHERE id = $1",
        [id]
      )

    case rows do
      [[status, step, attempt, result, last_error]] ->
        %{
          status: status,
          step: step,
          attempt: attempt,
          result: decode_json(result),
          error: last_error
        }

      [] ->
        nil
    end
  end

  # The Watcher's batched probe: of `ids`, which are worth nudging their
  # waiters about, as `{id, status}` pairs — settled (nothing left to do
  # without external input) or missing (swept; status nil). The status lets
  # the Watcher skip `until: :terminal` waiters of merely-parked rows. Plain
  # read, one statement for every waiter on this node.
  def await_probe(repo, ids) do
    %{rows: rows} =
      q!(
        repo,
        "await_probe",
        "SELECT id, status::text FROM gen_durable WHERE id = ANY($1::bigint[])",
        [ids]
      )

    found = Map.new(rows, fn [id, status] -> {id, status} end)
    settled = ~w(done failed awaiting_signal awaiting_children)

    Enum.flat_map(ids, fn id ->
      case Map.get(found, id) do
        nil -> [{id, nil}]
        status -> if status in settled, do: [{id, status}], else: []
      end
    end)
  end

  # Deliver a signal in ONE statement (was BEGIN + resolve + INSERT + wake +
  # COMMIT = up to 5 round trips on the user-facing hot path). `target` is either
  # an internal id (integer) or a correlation_key (string, resolved via
  # `correlation_guard` — the partial unique index — to the single occupied
  # instance carrying it). Shape:
  #   target — the addressed LIVE instance (terminal and missing ids resolve to
  #            nothing: nobody will ever read their inbox, so the signal is
  #            refused as :no_target instead of durably storing garbage; we also
  #            never hold a signal for an instance that does not exist yet).
  #   ins    — the inbox row; ON CONFLICT makes dedup_key redelivery idempotent.
  #   wake   — the delivery side of the lost-wakeup fix. Joins target by id with
  #            the flip condition in CASE, not WHERE, so it always locks the
  #            row: racing a park (the flush await) it queues behind the park's
  #            row lock and re-evaluates the CASE against the committed row
  #            (READ COMMITTED follows the update chain), flipping the freshly-
  #            parked row. A status-guarded WHERE would skip the not-yet-parked
  #            row without locking or waiting — the lost wakeup.
  # ins and wake commit atomically (one statement), so the park's recheck sees
  # the signal exactly when the wake also happened. Returns {:ok, queue} when
  # the target is runnable AFTER delivery — woken by this signal, or already
  # runnable (RETURNING sees only the post-image; filtering on a pre-image read
  # in an earlier CTE would be EPQ-unsafe and could miss a real wake). The
  # caller pokes that queue: a spurious poke costs one empty pick, a missed
  # wake would cost a poll interval. {:ok, nil} otherwise, {:error, :no_target}
  # when the target does not resolve to a live instance.
  @signal_sql_body """
  ),
  ins AS (
    INSERT INTO signals (target_id, name, payload, dedup_key)
    SELECT id, $2, $3::text::jsonb, $4 FROM target
    ON CONFLICT (target_id, dedup_key) DO NOTHING
  ),
  wake AS (
    UPDATE gen_durable g
    SET status = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                      THEN 'runnable'::durable_status ELSE g.status END,
        eligible_at = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                           THEN now() ELSE g.eligible_at END,
        updated_at = CASE WHEN g.status = 'awaiting_signal' AND $2 = ANY(g.awaits)
                          THEN now() ELSE g.updated_at END
    FROM target t
    WHERE g.id = t.id
    RETURNING g.status, g.queue
  )
  SELECT (SELECT count(*) FROM target),
         (SELECT w.queue FROM wake w WHERE w.status = 'runnable' LIMIT 1)
  """

  @signal_by_id """
                WITH target AS (
                  SELECT id FROM gen_durable WHERE id = $1 AND status NOT IN ('done', 'failed')
                """ <> @signal_sql_body

  @signal_by_key """
                 WITH target AS (
                   SELECT id FROM gen_durable
                   WHERE correlation_guard = $1 AND status NOT IN ('done', 'failed')
                 """ <> @signal_sql_body

  def deliver_signal(repo, target, name, payload_json, dedup_key) do
    {stmt, sql} =
      if is_integer(target),
        do: {"signal_by_id", @signal_by_id},
        else: {"signal_by_key", @signal_by_key}

    %{rows: [[n, woken]]} = q!(repo, stmt, sql, [target, name, payload_json, dedup_key])
    if n == 1, do: {:ok, woken}, else: {:error, :no_target}
  end

  # --- insert / batch insert -------------------------------------------------

  # Seed/refresh the rate-limit policy table at engine start. `configs` is a list of
  # `%{name, rate, burst}`. Idempotent: re-running with changed numbers updates them.
  # Sorted by name: DO UPDATE locks existing rows in VALUES order, and two nodes
  # booting with differently-ordered configs would deadlock.
  def upsert_rate_configs(_repo, []), do: :ok

  def upsert_rate_configs(repo, configs) when is_list(configs) do
    configs = Enum.sort_by(configs, & &1.name)

    values =
      configs
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_c, i} ->
        "('rate', $#{i * 4 + 1}, $#{i * 4 + 2}, $#{i * 4 + 3}, $#{i * 4 + 4})"
      end)

    args = Enum.flat_map(configs, &[&1.name, &1.rate, &1.capacity, &1.shards])

    repo.query!(
      "INSERT INTO gen_durable_bucket_configs (kind, name, rate, capacity, shards) VALUES " <>
        values <>
        " ON CONFLICT (kind, name) DO UPDATE SET " <>
        "rate = EXCLUDED.rate, capacity = EXCLUDED.capacity, shards = EXCLUDED.shards",
      args
    )

    :ok
  end

  # Seed/refresh the concurrency-gate policy at engine start. `configs` is a
  # list of `%{name, capacity, shards}` (capacity = the whole-key cap). Rate is
  # NULL for gates (they refill by completion credit, not by time). Sorted by
  # name (DO UPDATE locks rows in VALUES order — arbiter-deadlock discipline).
  # Bucket rows pick up a changed cap/shards lazily via the GC reconciler.
  def upsert_concurrency_configs(_repo, []), do: :ok

  def upsert_concurrency_configs(repo, configs) when is_list(configs) do
    configs = Enum.sort_by(configs, & &1.name)

    values =
      configs
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_c, i} ->
        "('conc', $#{i * 3 + 1}, NULL, $#{i * 3 + 2}, $#{i * 3 + 3})"
      end)

    args = Enum.flat_map(configs, &[&1.name, &1.capacity, &1.shards])

    repo.query!(
      "INSERT INTO gen_durable_bucket_configs (kind, name, rate, capacity, shards) VALUES " <>
        values <>
        " ON CONFLICT (kind, name) DO UPDATE SET " <>
        "capacity = EXCLUDED.capacity, shards = EXCLUDED.shards",
      args
    )

    :ok
  end

  # The GC-riding healer for concurrency gates — the counters' source of truth
  # is the executing rows themselves, so any drift (crash leaks — conservative,
  # available too low; config cap/shards changes; bugs) is periodically repaired
  # from it. Exactness needs the park+recheck trick: lock the bucket rows first
  # (ordered SKIP LOCKED — a busy bucket is being actively debited/credited, skip
  # it until the next sweep), then, in FRESH-snapshot statements while the locks
  # are held (claims and credits of the locked keys wait on them), recompute and
  # repair. The final statement sweeps orphaned buckets (config removed, shard
  # out of range) and whole idle-full keys — deletable because the pick's
  # c_cold/c_mint recreate buckets FULL (minus what that pick takes), which is
  # exactly their idle state; deletion is all-shards-or-nothing per key,
  # because c_cold only fires for keys with NO bucket rows at all (backfill
  # below restores that invariant after a shards increase). Returns the number
  # of repaired + deleted rows.
  def reconcile_concurrency(repo) do
    {:ok, n} =
      repo.transaction(fn ->
        %{rows: locked} =
          q!(
            repo,
            "conc_lock",
            """
            SELECT key, shard FROM gen_durable_buckets
            WHERE kind = 'conc'
            ORDER BY key, shard
            FOR UPDATE SKIP LOCKED
            """,
            []
          )

        case locked do
          [] ->
            0

          pairs ->
            keys = Enum.map(pairs, &hd/1)
            shards = Enum.map(pairs, &Enum.at(&1, 1))

            %{num_rows: healed} =
              q!(
                repo,
                "conc_heal",
                """
                WITH tgt AS (SELECT k, s FROM unnest($1::text[], $2::int[]) AS t(k, s)),
                held AS (
                  SELECT g.concurrency_key AS k, g.concurrency_shard AS s, count(*) AS n
                  FROM gen_durable g
                  WHERE g.status = 'executing' AND g.concurrency_shard IS NOT NULL
                  GROUP BY 1, 2
                )
                UPDATE gen_durable_buckets b
                SET capacity = calc.cap, available = calc.available
                FROM (
                  SELECT t.k, t.s,
                         (floor(cc.capacity / cc.shards) +
                          CASE WHEN t.s < (cc.capacity::bigint % cc.shards) THEN 1 ELSE 0 END) AS cap,
                         GREATEST(0, floor(cc.capacity / cc.shards) +
                          CASE WHEN t.s < (cc.capacity::bigint % cc.shards) THEN 1 ELSE 0 END
                          - coalesce(h.n, 0)) AS available
                  FROM tgt t
                  JOIN gen_durable_bucket_configs cc
                    ON cc.kind = 'conc' AND cc.name = split_part(t.k, ':', 1)
                  LEFT JOIN held h ON h.k = t.k AND h.s = t.s
                  WHERE t.s < cc.shards
                ) calc
                WHERE b.kind = 'conc' AND b.key = calc.k AND b.shard = calc.s
                  AND (b.capacity <> calc.cap OR b.available <> calc.available)
                """,
                [keys, shards]
              )

            %{num_rows: deleted} =
              q!(
                repo,
                "conc_sweep",
                """
                WITH tgt AS (SELECT k, s FROM unnest($1::text[], $2::int[]) AS t(k, s)),
                orphan AS (
                  SELECT t.k, t.s
                  FROM tgt t
                  LEFT JOIN gen_durable_bucket_configs cc
                         ON cc.kind = 'conc' AND cc.name = split_part(t.k, ':', 1)
                  WHERE (cc.name IS NULL OR t.s >= cc.shards)
                    AND NOT EXISTS (SELECT 1 FROM gen_durable g
                                    WHERE g.status = 'executing'
                                      AND g.concurrency_key = t.k
                                      AND g.concurrency_shard = t.s)
                ),
                idle AS (
                  SELECT b.key
                  FROM gen_durable_buckets b
                  JOIN tgt t ON t.k = b.key AND t.s = b.shard
                  WHERE b.kind = 'conc'
                  GROUP BY b.key
                  HAVING bool_and(b.available = b.capacity)
                     AND count(*) = (SELECT count(*) FROM gen_durable_buckets x
                                     WHERE x.kind = 'conc' AND x.key = b.key)
                )
                DELETE FROM gen_durable_buckets b
                USING tgt t
                WHERE b.kind = 'conc' AND b.key = t.k AND b.shard = t.s
                  AND ((t.k, t.s) IN (SELECT k, s FROM orphan)
                       OR t.k IN (SELECT key FROM idle))
                """,
                [keys, shards]
              )

            # Restore the all-or-nothing shard invariant: a shards-count
            # increase leaves existing gates with missing high shards (silently
            # shrunk capacity), and the pick's cold-mint fires only for keys
            # with NO buckets at all — so mint the missing in-range shards here,
            # full (a never-existed shard has nothing executing against it).
            # ON CONFLICT DO NOTHING: a racing pick-mint's debited row wins.
            %{num_rows: minted} =
              q!(
                repo,
                "conc_backfill",
                """
                INSERT INTO gen_durable_buckets (kind, key, shard, capacity, available)
                SELECT 'conc', k.key, s.shard,
                       (floor(cc.capacity / cc.shards) +
                        CASE WHEN s.shard < (cc.capacity::bigint % cc.shards) THEN 1 ELSE 0 END),
                       (floor(cc.capacity / cc.shards) +
                        CASE WHEN s.shard < (cc.capacity::bigint % cc.shards) THEN 1 ELSE 0 END)
                FROM (SELECT DISTINCT key FROM gen_durable_buckets WHERE kind = 'conc') k
                JOIN gen_durable_bucket_configs cc
                  ON cc.kind = 'conc' AND cc.name = split_part(k.key, ':', 1)
                CROSS JOIN LATERAL generate_series(0, cc.shards - 1) AS s(shard)
                WHERE NOT EXISTS (SELECT 1 FROM gen_durable_buckets b
                                  WHERE b.kind = 'conc' AND b.key = k.key AND b.shard = s.shard)
                ORDER BY k.key, s.shard
                ON CONFLICT (kind, key, shard) DO NOTHING
                """,
                []
              )

            healed + deleted + minted
        end
      end)

    n
  end

  def insert(repo, p) do
    sql =
      "INSERT INTO gen_durable (#{@insert_cols}) VALUES " <>
        "($1, $2, $3, $4::text::jsonb, $5, $6, $7, COALESCE($8::timestamptz, now()), " <>
        "$9, $10::text[]::durable_status[], $11, $12)" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    case q!(repo, "insert", sql, row_args(p)) do
      %{rows: [[id]]} -> {:ok, id}
      %{rows: []} -> {:error, :duplicate}
    end
  end

  # Rows ride in as 12 parallel arrays via `unnest` — 13 parameters for any batch
  # size (the wire protocol caps a statement at 65535 parameters, which the old
  # per-row-placeholder form hit at ~5400 rows) and a static, cacheable SQL text.
  # ORDER BY correlation_key: deterministic insertion order across nodes — two
  # concurrent batches inserting the same NEW keys in opposite orders would
  # deadlock on the correlation_guard arbiter index (each waits on the other's
  # uncommitted entry). Ids are therefore assigned in key order, not entry order
  # — indistinguishable to callers, since dropped duplicates already break any
  # positional mapping. Keyless rows never touch the arbiter; their order is
  # irrelevant.
  def insert_all(repo, rows) when is_list(rows) do
    sql =
      "INSERT INTO gen_durable (#{@insert_cols}) " <>
        @unnest_row_select <>
        " " <>
        unnest_from(0) <>
        " ORDER BY t.correlation_key" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING id"

    %{rows: out} = q!(repo, "insert_all", sql, column_arrays(rows))

    List.flatten(out)
  end

  defp row_args(p) do
    [
      p.fsm,
      p.fsm_version,
      p.step,
      p.state_json,
      p.queue,
      p.priority,
      p.concurrency_key,
      p.eligible_at,
      p.correlation_key,
      p.correlation_scope,
      p.rate_limit,
      p.weight
    ]
  end

  # Startup reclaim: release rows still claimed by a dead predecessor of this
  # scheduler — same claim prefix (instance+queue+VM, see Scheduler.claim_prefix/2),
  # any incarnation suffix. Prefix-compared with `left(...)` rather than LIKE, so
  # queue/instance names containing `%`/`_` need no escaping.
  #
  # The lease-staleness condition ($3, `lease_ttl - 2 × heartbeat_interval` ms of
  # remaining lease at most) is the safety net for claim-prefix collisions: a
  # LIVE owner beats every heartbeat_interval, keeping its remaining lease above
  # the threshold, so its claims are never touched even if two VMs end up with
  # the same prefix (e.g. containers with identical hostnames and BEAM as OS
  # pid 1). A dead owner's lease decays below it after ~2 missed beats — the
  # reclaim fires then, still far ahead of full lease expiry (the reaper's
  # floor). Rows claimed moments before the predecessor died are the remainder;
  # they wait for the reaper. Scans the (small) executing set via the partial
  # lease index; runs once per scheduler boot.
  def reclaim_orphans(repo, queue, prefix, margin_ms) do
    %{num_rows: n} =
      q!(
        repo,
        "reclaim_orphans",
        """
        WITH stale AS (
          SELECT id FROM gen_durable
          WHERE queue = $1 AND status = 'executing'
            AND left(locked_by, char_length($2)) = $2
            AND lease_expires_at < now() + $3::int * interval '1 millisecond'
          ORDER BY id
          FOR NO KEY UPDATE SKIP LOCKED
        )
        UPDATE gen_durable g
        SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
        FROM stale s
        WHERE g.id = s.id
        """,
        [queue, prefix, margin_ms]
      )

    n
  end

  # Release our still-claimed rows back to runnable on graceful shutdown, so the
  # buffered (un-started) work is picked up immediately instead of waiting out the
  # lease. Guarded by `locked_by` so we only ever release our own claims.
  def release(_repo, [], _worker), do: :ok

  def release(repo, ids, worker) when is_list(ids) do
    q!(
      repo,
      "release",
      """
      WITH mine AS (
        SELECT id FROM gen_durable
        WHERE id = ANY($1) AND locked_by = $2 AND status = 'executing'
        ORDER BY id
        FOR NO KEY UPDATE SKIP LOCKED
      )
      UPDATE gen_durable g
      SET status = 'runnable', locked_by = null, lease_expires_at = null, updated_at = now()
      FROM mine m
      WHERE g.id = m.id
      """,
      [ids, worker]
    )

    :ok
  end

  # The binary branch covers jsonb scalar-string rows written by versions that
  # double-encoded JSON params (≤ 0.1.8); new rows arrive as decoded maps.
  defp decode_json(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json(value) when is_map(value), do: value
  defp decode_json(nil), do: %{}

  defp decode_json_or_nil(nil), do: nil
  defp decode_json_or_nil(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_json_or_nil(value) when is_map(value), do: value
end
