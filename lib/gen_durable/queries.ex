defmodule GenDurable.Queries do
  @moduledoc """
  Every database statement, one function each, as raw SQL.

  All functions take the `repo` explicitly. The `complete_*` functions run the
  outcome `UPDATE` and the consumed-signal `DELETE` in one transaction.
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
  # The 12 insert columns, used by insert / insert_all / complete_schedule_childs.

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

  # An outcome's ownership guard matched (1, committed) or not (0, the worker no
  # longer owns the row — the outcome was dropped).
  defp committed?(1), do: :ok
  defp committed?(0), do: :stale

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

  # --- step outcomes -----------------------------------------
  #
  # Each outcome is a SINGLE statement, not a transaction (one round-trip instead
  # of BEGIN + … + COMMIT) — except `:await`, which is deliberately a two-statement
  # transaction (park + recheck): the extra round trips close the lost-wakeup race
  # with deliver_signal (see complete_await). The signal consume rides along as a
  # data-modifying CTE, `consumed`, atomic with the outcome UPDATE because one
  # statement is its own implicit transaction; it runs to completion even though
  # the main query never reads it — Postgres guarantees data-modifying CTEs always
  # execute fully. What `consumed` deletes depends on the outcome:
  #   * progressing (:next / :schedule_childs) — exactly the awaited-signal ids the
  #     step received (`id = ANY($consumed)`); latecomers and non-awaited signals
  #     survive. Empty list ⇒ no-op.
  #   * terminal (:done / :stop) — the whole inbox (`target_id = $id`): the instance
  #     is finished, so nothing will read its signals again (cleanup).
  #   * :retry / :await consume nothing (the step is redone / still waiting).
  #
  # OWNERSHIP GUARD: every outcome commits only while the worker still owns the
  # claim — `locked_by = $worker AND status = 'executing'`. An orphaned task (its
  # scheduler crashed, so nobody heartbeats its rows) can outlive the lease; the
  # reaper then hands the row to a new claimant, and the orphan's late commit
  # must NOT land on top — unguarded it would rewind step/state mid-flight, null
  # the new claimant's locked_by (silencing its heartbeat), or, terminally,
  # delete the inbox and decrement the parent join barrier. Guarded, the stale
  # outcome affects zero rows and every side effect is gated on the guarded
  # UPDATE via CTE references (reading the update's RETURNING, never the table —
  # a table re-read would see the pre-update snapshot and fire the side effects
  # even when the guard EPQ-failed). Each complete_* returns :ok | :stale; the
  # executor emits [:gen_durable, :outcome, :stale] telemetry on the drop, and
  # the step's work is redone by the current claimant (at-least-once).

  # The join-barrier decrement, a CTE of the terminal outcomes. Reads the child's
  # `parent_id` from the guarded `terminal` CTE's RETURNING — empty when the
  # ownership guard failed, so a stale worker never touches the parent. No-op
  # when the row has no parent (the join yields nothing). The decrement that hits
  # zero releases the barrier; concurrent siblings serialize on the parent row
  # lock. `terminal` updates the child; this updates the parent (a different
  # row), so the two table-modifications never touch the same row. The final
  # SELECT reports whether the outcome committed (1) or was stale (0).
  # RETURNING reads the post-update row: a parent whose join just completed
  # comes back 'runnable' — its queue is surfaced so the executor can poke it
  # (the parent may live in a different queue than the child that woke it).
  @notify_parent """
  notify AS (
    UPDATE gen_durable p
    SET children_pending = p.children_pending - 1,
        status = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                      THEN 'runnable' ELSE p.status END,
        eligible_at = CASE WHEN p.children_pending - 1 <= 0 AND p.status = 'awaiting_children'
                           THEN now() ELSE p.eligible_at END,
        updated_at = now()
    FROM terminal c
    WHERE c.parent_id = p.id
    RETURNING p.status, p.queue
  )
  SELECT (SELECT count(*) FROM terminal),
         (SELECT n.queue FROM notify n WHERE n.status = 'runnable' LIMIT 1)
  """

  # The concurrency-gate release is now out-of-band: the row still nulls its
  # `concurrency_shard` on every outcome (leaving `executing`), but the slot is
  # credited back by `GenDurable.Limiter.credit/2`, called by the executor after a
  # committed (non-stale) outcome — see `GenDurable.Executor.apply_outcome`. A stale
  # outcome credits nothing (the executor skips it); a crash between commit and credit
  # leaks in the conservative direction (under-admission), healed by the reconciler.

  # :next sets the row's rate_limit key ($5, NULL ⇒ not limited) and weight ($6) for the
  # next step. No bucket rider: a missing bucket is minted, pre-debited, by the
  # first pick that grants from it (see r_mint) — cold keys admit with zero lag.
  #
  # `concurrency_key` is :keep (the default — identity keys persist across
  # steps), nil (release the key for the next steps), or a new key. The shard is
  # always cleared (the row leaves executing; a re-claim assigns a fresh one)
  # and the old slot is credited via the `credit` rider.
  def complete_next(repo, id, worker, step, state_json, consumed_ids, rate_limit, weight, ck) do
    {ck_change, ck_value} = if ck == :keep, do: {false, nil}, else: {true, ck}

    %{rows: [[n]]} =
      q!(
        repo,
        "complete_next",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, status = 'runnable', eligible_at = now(),
              attempt = 0, awaits = null, rate_limit = $5, weight = $6,
              concurrency_key = CASE WHEN $8::bool THEN $9 ELSE concurrency_key END,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $7 AND status = 'executing'
          RETURNING id
        ),
        consumed AS (
          DELETE FROM signals
          WHERE target_id IN (SELECT id FROM committed) AND id = ANY($4::bigint[])
        )
        SELECT count(*) FROM committed
        """,
        [id, step, state_json, consumed_ids, rate_limit, weight, worker, ck_change, ck_value]
      )

    committed?(n)
  end

  # INLINE CONTINUATION (run-ahead): commit the `:next` transition WITHOUT leaving
  # `executing` — the same worker keeps the claim and runs the next step in place,
  # skipping the requeue → re-pick round-trip. Durability is unchanged: the next
  # step's state is committed here, before that step runs; a crash re-runs it via the
  # reaper exactly as a requeued `:next` would. Same ownership guard as every outcome
  # (`locked_by = $worker AND status = 'executing'`): an orphaned chained task whose
  # lease expired commits nothing (`:stale`) — critically, this guarded commit runs
  # BEFORE the executor calls the (unguarded) out-of-band `Limiter.admit`, so an orphan
  # never debits/stamps a row a new claimant now owns.
  #
  # Concurrency handoff between steps (the executor computed the directives):
  #   * `set_key`   — false keeps the current `concurrency_key` (`:keep`); true sets it
  #                   to `key_value` (a new key, or NULL to release).
  #   * `set_shard` — false keeps the current shard; true sets it to `shard_value`:
  #                     NULL — release / unconfigured new key (re-enters the K=1 arbiter;
  #                            a unique violation here means the key is taken → `:contended`,
  #                            and the executor requeues through the picker), or
  #                     0    — provisional for a CONFIGURED new key (non-null ⇒ out of the
  #                            K=1 index); the real shard is stamped by the subsequent
  #                            `Limiter.admit`, mirroring the claim path.
  # The lease is extended so a long inline chain never relies solely on the heartbeat.
  def continue_next(
        repo,
        id,
        worker,
        step,
        state_json,
        consumed_ids,
        rate_limit,
        weight,
        set_key,
        key_value,
        set_shard,
        shard_value,
        lease_ttl_ms
      ) do
    %{rows: [[n]]} =
      q!(
        repo,
        "continue_next",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, status = 'executing', eligible_at = now(),
              attempt = 0, awaits = null, rate_limit = $5, weight = $6,
              concurrency_key = CASE WHEN $8::bool THEN $9 ELSE concurrency_key END,
              concurrency_shard = CASE WHEN $10::bool THEN $11 ELSE concurrency_shard END,
              lease_expires_at = now() + $12::int * interval '1 millisecond', updated_at = now()
          WHERE id = $1 AND locked_by = $7 AND status = 'executing'
          RETURNING id
        ),
        consumed AS (
          DELETE FROM signals
          WHERE target_id IN (SELECT id FROM committed) AND id = ANY($4::bigint[])
        )
        SELECT count(*) FROM committed
        """,
        [
          id,
          step,
          state_json,
          consumed_ids,
          rate_limit,
          weight,
          worker,
          set_key,
          key_value,
          set_shard,
          shard_value,
          lease_ttl_ms
        ]
      )

    committed?(n)
  rescue
    e in Postgrex.Error ->
      # An unconfigured new concurrency_key that another executing row already holds trips
      # the K=1 arbiter (gen_durable_concurrency_active). The whole statement rolls back —
      # nothing committed — so the executor falls back to a normal requeue (`complete_next`)
      # and the picker arbitrates the key, exactly as a first pick would.
      if is_map(e.postgres) && e.postgres[:constraint] == "gen_durable_concurrency_active",
        do: :contended,
        else: reraise(e, __STACKTRACE__)
  end

  # Reload the signal inbox and children snapshot for already-claimed jobs — the same
  # batched enrichment the pick runs, exposed for the inline-continuation path (a run-ahead
  # step gets a fresh snapshot, identical to what a re-pick would hand it).
  def enrich_jobs(repo, jobs), do: enrich(repo, jobs)

  # :retry redoes the same step, so it consumes nothing and KEEPS `awaits` — the
  # redo must see the same awaited signals it was handed. The gate slot is
  # released (the row leaves executing); the redo re-claims through admission.
  def complete_retry(repo, id, worker, state_json, delay_ms) do
    %{rows: [[n]]} =
      q!(
        repo,
        "complete_retry",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET state = $2::text::jsonb, status = 'runnable',
              eligible_at = now() + $3::int * interval '1 millisecond',
              attempt = attempt + 1, concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $4 AND status = 'executing'
          RETURNING id
        )
        SELECT count(*) FROM committed
        """,
        [id, state_json, delay_ms, worker]
      )

    committed?(n)
  end

  # Park on a name set ($3, text[]), transitioning to `next_step` ($4): when any named
  # signal arrives the row becomes runnable at `next_step`, which reads the matching
  # subset as `ctx.awaited`.
  #
  # Two statements in ONE transaction — the park side of the lost-wakeup fix:
  #   1. park — flips to awaiting_signal and takes the row lock (held to commit).
  #   2. recheck — a fresh snapshot: a matching signal already in the inbox (its
  #      delivery committed before this statement) flips straight to runnable.
  # A delivery the recheck cannot see must commit after it — but its wake UPDATE
  # matches the row unconditionally (see deliver_signal), so it queues on our row
  # lock and performs the flip itself once we commit. Either the recheck or the
  # delivery wakes the row; no interleaving leaves a matching signal with a
  # parked instance. A single statement cannot do this: under READ COMMITTED its
  # EXISTS runs on the statement snapshot, blind to a concurrently-committing
  # delivery, while that delivery's status-guarded wake skips the not-yet-parked
  # row without locking. The extra round trips buy the race away.
  #
  # `presented_ids` — the awaited-signal ids the parking step was HANDED
  # (ctx.awaited; :await consumes nothing, so they are still in the inbox). The
  # recheck excludes them: waking on a signal the step already saw and chose to
  # re-await would spin the accumulate-a-pack pattern (park → recheck flips →
  # immediate re-pick → re-await → …) at full speed until the pack completes.
  # A set-difference, not a max-id watermark: signal ids commit out of order, so
  # "id greater than the newest presented" could skip a signal that was inserted
  # earlier but committed later (invisible to the step's enrichment snapshot).
  # Deliveries are unaffected — a delivered signal is a fresh insert, never in
  # the presented set, and the wake checks only the name.
  # `timeout_ms` (nil ⇒ no deadline) arms `await_deadline`: the reaper's sweep
  # returns an expired park to runnable, and the woken step sees whatever is in
  # the inbox — for a fresh await an empty ctx.awaited means timeout. The park is
  # the only writer of the column (NULL propagation: a nil timeout stores NULL),
  # so a stale deadline left on a woken row is always overwritten by the next park
  # and never matches the sweep's status filter in between.
  def complete_await(repo, id, worker, state_json, names, next_step, presented_ids, timeout_ms) do
    {:ok, result} =
      repo.transaction(fn ->
        %{rows: [[parked]]} =
          q!(
            repo,
            "await_park",
            """
            WITH park AS (
              UPDATE gen_durable
              SET step = $4, state = $2::text::jsonb, awaits = $3::text[], eligible_at = now(),
                  status = 'awaiting_signal', attempt = 0, rate_limit = null, weight = 1,
                  concurrency_shard = null,
                  await_deadline = now() + $6::int * interval '1 millisecond',
                  locked_by = null, lease_expires_at = null, updated_at = now()
              WHERE id = $1 AND locked_by = $5 AND status = 'executing'
              RETURNING id
            )
            SELECT count(*) FROM park
            """,
            [id, state_json, names, next_step, worker, timeout_ms]
          )

        # Guard failed ⇒ the row is someone else's now — skip the recheck (its
        # own claimant parks and rechecks for itself).
        if parked == 1 do
          q!(
            repo,
            "await_recheck",
            """
            UPDATE gen_durable
            SET status = 'runnable', updated_at = now()
            WHERE id = $1 AND status = 'awaiting_signal'
              AND EXISTS (SELECT 1 FROM signals
                          WHERE target_id = $1 AND name = ANY(gen_durable.awaits)
                            AND id != ALL($2::bigint[]))
            """,
            [id, presented_ids]
          )

          :ok
        else
          :stale
        end
      end)

    result
  end

  # Returns {:ok, woken_parent_queue | nil} | :stale — the queue rides back so
  # the executor can poke a parent whose join this completion satisfied.
  def complete_done(repo, id, worker, result_json) do
    %{rows: [[n, wake]]} =
      q!(
        repo,
        "complete_done",
        """
        WITH terminal AS (
          UPDATE gen_durable
          SET result = $2::text::jsonb, status = 'done', awaits = null,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
        consumed AS (
          DELETE FROM signals WHERE target_id IN (SELECT id FROM terminal)
        ),
        """ <> @notify_parent,
        [id, result_json, worker]
      )

    if n == 1, do: {:ok, wake}, else: :stale
  end

  def complete_stop(repo, id, worker, reason_text) do
    %{rows: [[n, wake]]} =
      q!(
        repo,
        "complete_stop",
        """
        WITH terminal AS (
          UPDATE gen_durable
          SET status = 'failed', last_error = $2, awaits = null,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $3 AND status = 'executing'
          RETURNING id, parent_id
        ),
        consumed AS (
          DELETE FROM signals WHERE target_id IN (SELECT id FROM terminal)
        ),
        """ <> @notify_parent,
        [id, reason_text, worker]
      )

    if n == 1, do: {:ok, wake}, else: :stale
  end

  # :schedule_childs — spawn the batch and park the parent on the join
  # barrier, in one statement (consume + insert children + park). children_pending
  # is set to the number of children actually inserted; zero inserted ⇒ barrier
  # pre-satisfied ⇒ runnable.
  def complete_schedule_childs(repo, parent_id, worker, next_step, state_json, [], consumed_ids) do
    %{rows: [[n]]} =
      q!(
        repo,
        "schedule_childs_empty",
        """
        WITH committed AS (
          UPDATE gen_durable
          SET step = $2, state = $3::text::jsonb, children_pending = 0, status = 'runnable',
              eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1,
              concurrency_shard = null,
              locked_by = null, lease_expires_at = null, updated_at = now()
          WHERE id = $1 AND locked_by = $5 AND status = 'executing'
          RETURNING id
        ),
        consumed AS (
          DELETE FROM signals
          WHERE target_id IN (SELECT id FROM committed) AND id = ANY($4::bigint[])
        )
        SELECT count(*) FROM committed
        """,
        [parent_id, next_step, state_json, consumed_ids, worker]
      )

    committed?(n)
  end

  # Children ride in as 12 parallel arrays via `unnest` (base 4: $5..$16), so the
  # parameter count is fixed — 17 for any batch size (the wire protocol caps a
  # statement at 65535 parameters, ~5400 rows in per-row-placeholder form) — and
  # the SQL text is static (statement-cacheable). $1 doubles as the parent_id column.
  # The ownership guard lives in a leading `claim` CTE (SELECT … FOR UPDATE): the
  # children insert, the consume, and the parent park are all gated on it, because
  # the park needs the inserted-children count and so cannot itself be the guard.
  # Children insert ORDER BY correlation_key — same arbiter-deadlock discipline
  # as insert_all (see there).
  def complete_schedule_childs(
        repo,
        parent_id,
        worker,
        next_step,
        state_json,
        children,
        consumed_ids
      ) do
    sql =
      "WITH claim AS (SELECT id FROM gen_durable " <>
        "WHERE id = $1 AND locked_by = $17 AND status = 'executing' FOR NO KEY UPDATE), " <>
        "consumed AS (DELETE FROM signals WHERE target_id IN (SELECT id FROM claim) " <>
        "AND id = ANY($4::bigint[])), " <>
        "ins AS (INSERT INTO gen_durable (#{@insert_cols}, parent_id) " <>
        @unnest_row_select <>
        ", $1 " <>
        unnest_from(4) <>
        " WHERE EXISTS (SELECT 1 FROM claim)" <>
        " ORDER BY t.correlation_key" <>
        " ON CONFLICT (correlation_guard) WHERE correlation_guard IS NOT NULL DO NOTHING RETURNING 1), " <>
        "cnt AS (SELECT count(*) AS n FROM ins) " <>
        "UPDATE gen_durable SET step = $2, state = $3::text::jsonb, " <>
        "children_pending = (SELECT n FROM cnt), " <>
        "status = (CASE WHEN (SELECT n FROM cnt) = 0 THEN 'runnable' " <>
        "ELSE 'awaiting_children' END)::durable_status, " <>
        "eligible_at = now(), attempt = 0, awaits = null, rate_limit = null, weight = 1, " <>
        "concurrency_shard = null, " <>
        "locked_by = null, lease_expires_at = null, updated_at = now() " <>
        "WHERE id IN (SELECT id FROM claim)"

    args =
      [parent_id, next_step, state_json, consumed_ids] ++
        column_arrays(children) ++ [worker]

    %{num_rows: n} = q!(repo, "schedule_childs", sql, args)
    committed?(n)
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
  #            row: racing a park (complete_await) it queues behind the park's
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
