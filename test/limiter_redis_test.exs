defmodule GenDurable.LimiterRedisTest do
  use ExUnit.Case, async: false

  alias GenDurable.Limiter.Redis, as: R

  @redis System.get_env("REDIS_URL", "redis://localhost:6379")

  setup do
    {:ok, conn} = Redix.start_link(@redis)
    # A dedicated DB, isolated from the poke-transport tests on db 0.
    Redix.command!(conn, ["SELECT", "15"])
    Redix.command!(conn, ["FLUSHDB"])

    cfg_key = {__MODULE__, System.unique_integer([:positive])}
    handle = %{conn: conn, lease_ttl_ms: 60_000, cfg_key: cfg_key}

    # gate "api" cap 2; rate "email" burst 5 @ 5/s
    R.sync_config(handle, [{:conc, "api", 2, 1}, {:rate, "email", 5.0, 5, 1}])

    on_exit(fn -> :persistent_term.erase(cfg_key) end)
    {:ok, handle: handle, conn: conn}
  end

  defp entry(id, opts), do: %{id: id, conc: opts[:conc], rate: opts[:rate]}
  defp ids(admitted), do: admitted |> Enum.map(&elem(&1, 0)) |> Enum.sort()

  test "concurrency admits up to cap; credit readmits", %{handle: h} do
    r = R.admit(h, [entry(1, conc: "api:x"), entry(2, conc: "api:x"), entry(3, conc: "api:x")])
    assert ids(r.admitted) == [1, 2]
    assert r.denied == [3]
    assert Enum.all?(r.admitted, fn {id, slot} -> slot == {"api:x", id} end)

    # cap exhausted
    assert %{admitted: [], denied: [4]} = R.admit(h, [entry(4, conc: "api:x")])

    # credit one slot back → the next admit readmits exactly one
    {_id, slot} = hd(r.admitted)
    :ok = R.credit(h, [slot])
    assert %{admitted: [{5, _}], denied: []} = R.admit(h, [entry(5, conc: "api:x")])
  end

  test "distinct keys are independent semaphores", %{handle: h} do
    r =
      R.admit(h, [
        entry(1, conc: "api:a"),
        entry(2, conc: "api:b"),
        entry(3, conc: "api:a"),
        entry(4, conc: "api:b"),
        entry(5, conc: "api:a")
      ])

    # cap 2 per key: a → {1,3}, b → {2,4}; 5 denied
    assert ids(r.admitted) == [1, 2, 3, 4]
    assert r.denied == [5]
  end

  test "a leaked slot self-heals when its lease expires (no credit)", %{handle: h0} do
    h = %{h0 | lease_ttl_ms: 1}
    assert length(R.admit(h, [entry(1, conc: "api:x"), entry(2, conc: "api:x")]).admitted) == 2

    Process.sleep(10)

    # holders 1,2 "crashed" (never credited); their expired leases are pruned on next admit
    r = R.admit(h0, [entry(3, conc: "api:x"), entry(4, conc: "api:x")])
    assert ids(r.admitted) == [3, 4]
  end

  test "renew keeps a live holder from being pruned mid-step", %{handle: h0} do
    h = %{h0 | lease_ttl_ms: 150}
    %{admitted: [{1, slot}]} = R.admit(h, [entry(1, conc: "api:x")])

    # heartbeat past the original 150ms expiry
    for _ <- 1..4 do
      Process.sleep(60)
      :ok = R.renew(h, [slot])
    end

    # holder 1 still occupies a slot → only one of cap 2 is free
    assert length(R.admit(h, [entry(2, conc: "api:x"), entry(3, conc: "api:x")]).admitted) == 1
  end

  test "rate grants the priority prefix that fits the budget; no slot", %{handle: h} do
    r =
      R.admit(h, [
        entry(1, rate: {"email:x", 2}),
        entry(2, rate: {"email:x", 2}),
        entry(3, rate: {"email:x", 2})
      ])

    # burst 5: cw 2,4 fit; 6 > 5 stops the prefix
    assert ids(r.admitted) == [1, 2]
    assert r.denied == [3]
    assert Enum.all?(r.admitted, fn {_id, slot} -> slot == nil end)
  end

  test "gated AND rated: the gate slot is taken only when rate also passes", %{handle: h} do
    r =
      R.admit(h, [
        %{id: 1, conc: "api:x", rate: {"email:x", 3}},
        %{id: 2, conc: "api:x", rate: {"email:x", 3}}
      ])

    # entry 1: gate ok + rate 3<=5 → admitted. entry 2: gate ok but rate 3+3>5 → denied,
    # so its gate slot is NOT taken.
    assert ids(r.admitted) == [1]
    assert r.denied == [2]

    # proof the gate slot was not consumed by the rate-denied entry: only 1 of cap 2 is used
    assert %{admitted: [{3, {"api:x", 3}}]} = R.admit(h, [entry(3, conc: "api:x")])
  end
end
