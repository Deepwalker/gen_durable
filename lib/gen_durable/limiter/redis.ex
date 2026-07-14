defmodule GenDurable.Limiter.Redis do
  @moduledoc """
  A `GenDurable.Limiter` backend on Redis — no Postgres locks, no bucket table.

    * **Concurrency** is a **lease-scored ZSET** per key (`gd:conc:<key>`): members are the
      holders (the `gen_durable` row id), scores are lease-expiry timestamps. `admit` prunes
      expired members first (`ZREMRANGEBYSCORE … now`) then admits up to `cap - ZCARD`. A
      crashed holder's lease simply expires and is pruned on the next admit — **the slot
      self-heals with no Postgres reconcile**. The heartbeat bumps live holders' scores
      (`renew`), so a long step is never pruned mid-flight.

    * **Rate** is a **token bucket** per key (`gd:rate:<key>`, hash `{t, s}`): refill by
      elapsed time, grant the priority prefix that fits, no credit (time refills it). An idle
      bucket is `PEXPIRE`d so a swept key reads as cold/full — mirroring the PG lazy-mint.

  `admit`/`renew` are single Lua `EVAL`s (Redis is single-threaded → atomic); `now` comes from
  Redis `TIME` so prune and lease share one clock. `credit` is a `ZREM` pipeline.

  **Config** (caps/rates) lives in `:persistent_term`, seeded by `sync_config/2` at startup —
  not in Redis — so `admit` needs no config round-trip and all nodes agree from the same app
  config. The handle is `%{conn: redix_name, lease_ttl_ms: ttl, cfg_key: term}`.

  **Single-node Redis only.** `admit` touches every batched key in one script; on Redis
  Cluster those keys would span hash slots and the `EVAL` would be rejected. (The engine's
  `{:redis, _}` poke transport assumes a single Redis too.)
  """

  @behaviour GenDurable.Limiter

  # ---- admit: prune+refill, admit in priority order, debit only finally-admitted ----------
  # ARGV: [lease_ttl_ms, then 7 per entry: id, conc_key, conc_cap, rate_key, rate, burst, weight]
  # ("" for an absent key). Returns one "a"/"d" per entry, in order. A gate slot is RESERVED in
  # rank order (holds the rank even if rate then denies — the freed rank is not reassigned, as
  # in the fused pick), but the ZADD (the actual take) fires only for the finally-admitted.
  @admit_lua """
  local T = redis.call('TIME')
  local now = tonumber(T[1]) * 1000 + math.floor(tonumber(T[2]) / 1000)
  local lease = now + tonumber(ARGV[1])
  local n = (#ARGV - 1) / 7

  local cavail, rtok, rcfg = {}, {}, {}
  for i = 0, n - 1 do
    local b = 2 + i * 7
    local ck = ARGV[b + 1]
    if ck ~= '' and cavail[ck] == nil then
      redis.call('ZREMRANGEBYSCORE', 'gd:conc:' .. ck, '-inf', now)
      cavail[ck] = tonumber(ARGV[b + 2]) - redis.call('ZCARD', 'gd:conc:' .. ck)
    end
    local rk = ARGV[b + 3]
    if rk ~= '' and rtok[rk] == nil then
      local rate, burst = tonumber(ARGV[b + 4]), tonumber(ARGV[b + 5])
      local h = redis.call('HMGET', 'gd:rate:' .. rk, 't', 's')
      local tok, ts = tonumber(h[1]), tonumber(h[2])
      if tok == nil then tok, ts = burst, now end
      rtok[rk] = math.min(burst, tok + (now - ts) * rate / 1000)
      rcfg[rk] = { rate, burst }
    end
  end

  local res, rblocked = {}, {}
  for i = 0, n - 1 do
    local b = 2 + i * 7
    local id, ck, rk, w = ARGV[b], ARGV[b + 1], ARGV[b + 3], tonumber(ARGV[b + 6])
    local gate_ok
    if ck == '' then
      gate_ok = true
    elseif cavail[ck] > 0 then
      gate_ok = true
      cavail[ck] = cavail[ck] - 1
    else
      gate_ok = false
    end

    local ok = false
    if gate_ok then
      if rk == '' then
        ok = true
      elseif not rblocked[rk] and rtok[rk] >= w then
        ok = true
        rtok[rk] = rtok[rk] - w
      else
        rblocked[rk] = true
      end
    end

    if ok and ck ~= '' then redis.call('ZADD', 'gd:conc:' .. ck, lease, id) end
    res[i + 1] = ok and 'a' or 'd'
  end

  for rk, tok in pairs(rtok) do
    redis.call('HSET', 'gd:rate:' .. rk, 't', tostring(tok), 's', tostring(now))
    local rate, burst = rcfg[rk][1], rcfg[rk][2]
    local secs = rate > 0 and (burst / rate) or 3600
    redis.call('PEXPIRE', 'gd:rate:' .. rk, math.ceil(secs * 1000) + 1000)
  end

  return res
  """

  # ---- renew: bump live holders' lease scores (GT = never lower one) ----------------------
  # ARGV: [lease_ttl_ms, then pairs conc_key, id]
  @renew_lua """
  local T = redis.call('TIME')
  local lease = tonumber(T[1]) * 1000 + math.floor(tonumber(T[2]) / 1000) + tonumber(ARGV[1])
  for i = 2, #ARGV, 2 do
    redis.call('ZADD', 'gd:conc:' .. ARGV[i], 'GT', 'CH', lease, ARGV[i + 1])
  end
  return 'ok'
  """

  @impl true
  def sync_config(%{cfg_key: cfg_key}, configs) do
    index = %{
      conc: Map.new(for({:conc, name, cap, _shards} <- configs, do: {name, cap})),
      rate: Map.new(for({:rate, name, rate, burst, _shards} <- configs, do: {name, {rate, burst}}))
    }

    :persistent_term.put(cfg_key, index)
    :ok
  end

  @impl true
  def admit(_handle, []), do: %{admitted: [], denied: []}

  def admit(%{conn: conn, lease_ttl_ms: ttl, cfg_key: cfg_key}, entries) do
    cfg = :persistent_term.get(cfg_key)

    argv =
      [to_string(ttl)] ++
        Enum.flat_map(entries, fn e ->
          {ck, ccap} = conc_args(e.conc, cfg)
          {rk, rate, burst, w} = rate_args(e.rate, cfg)
          [to_string(e.id), ck, ccap, rk, rate, burst, w]
        end)

    {:ok, codes} = Redix.command(conn, ["EVAL", @admit_lua, "0" | argv])

    entries
    |> Enum.zip(codes)
    |> Enum.reduce(%{admitted: [], denied: []}, fn {e, code}, acc ->
      if code == "a" do
        slot = if e.conc, do: {e.conc, e.id}, else: nil
        %{acc | admitted: [{e.id, slot} | acc.admitted]}
      else
        %{acc | denied: [e.id | acc.denied]}
      end
    end)
  end

  @impl true
  def credit(_handle, []), do: :ok

  def credit(%{conn: conn}, slots) do
    cmds = for {key, id} <- slots, do: ["ZREM", "gd:conc:" <> key, to_string(id)]
    {:ok, _} = Redix.pipeline(conn, cmds)
    :ok
  end

  @impl true
  def renew(_handle, []), do: :ok

  def renew(%{conn: conn, lease_ttl_ms: ttl}, slots) do
    argv = [to_string(ttl)] ++ Enum.flat_map(slots, fn {key, id} -> [key, to_string(id)] end)
    {:ok, _} = Redix.command(conn, ["EVAL", @renew_lua, "0" | argv])
    :ok
  end

  # Lease expiry is the self-heal; nothing to reconcile from Postgres. Zeroes keep the GC's
  # telemetry shape uniform with the PG backend.
  @impl true
  def reconcile(_handle), do: %{buckets: 0, gates: 0}

  # --- config lookups (name = the key's prefix before ':', as in the PG config join) --------

  defp conc_args(nil, _cfg), do: {"", "0"}

  defp conc_args(key, cfg) do
    cap = Map.get(cfg.conc, name(key), 0)
    {key, to_string(cap)}
  end

  defp rate_args(nil, _cfg), do: {"", "0", "0", "1"}

  defp rate_args({key, weight}, cfg) do
    {rate, burst} = Map.get(cfg.rate, name(key), {0, 0})
    {key, to_string(rate), to_string(burst), to_string(weight)}
  end

  defp name(key), do: key |> String.split(":", parts: 2) |> hd()
end
