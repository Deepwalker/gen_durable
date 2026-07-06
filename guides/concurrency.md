# Concurrency keys

A `concurrency_key` is a **semaphore of size K** on a key: at most K instances sharing the
key execute at once, while different keys run in parallel. K comes from the
`concurrency_limits:` engine config, matched by the key's *name* (the part before `:`);
an **unconfigured key defaults to K = 1** — strict mutual exclusion, the answer to
"process everything for one account one-at-a-time, but accounts in parallel."

```elixir
# mutual exclusion (no config for "account" ⇒ K = 1)
GenDurable.insert(Account.Sync, state: %{id: 7}, concurrency_key: "account:7")
GenDurable.insert(Account.Sync, state: %{id: 7}, concurrency_key: "account:7")  # waits for the first
GenDurable.insert(Account.Sync, state: %{id: 9}, concurrency_key: "account:9")  # runs in parallel

# a concurrency GATE: cap the in-flight work against an external API, cluster-wide
# engine: concurrency_limits: [stripe: [limit: 100]]
GenDurable.insert(Charge, state: %{...}, concurrency_key: {:stripe, tenant_id})
```

Unlike a [rate limit](rate_limiting.md) (a **flow** cap — starts per second, blind to how
long steps run), a concurrency gate is a **stock** cap: the slot is held for the whole
step and returned on its outcome. A hundred-per-minute rate limit happily launches 100
thirty-second calls at once; `limit: 10` never has more than 10 in flight.

## How it is enforced

- **K = 1 (unconfigured)**: a unique partial index over executing keys — a second
  executing row per key is *uncommittable*. The claim is the lock: held exactly for the
  step window, released by any outcome, or by the [reaper](operations.md) if the worker
  dies. No per-step locks, no pinned connections.
- **Gates (configured)**: per-key slot counters with a database `CHECK` — over-admission
  is uncommittable. The pick debits slots in one batched pass; every outcome credits its
  slot back. Crash paths under-credit (the safe direction: temporarily stricter than K)
  and the GC reconciler repairs the counters from the executing-rows truth each sweep.
- An await **releases** the slot (parking is not executing); the woken step re-admits
  through the gate. Prefetch-buffered rows hold slots (they are claimed).

A `NULL` `concurrency_key` (the common case) never serializes and pays nothing for any of
this machinery.

## Sharding a big gate

Completions of one key serialize on its slot counter (a row lock held to commit), which
caps a single gate's completion throughput at roughly `1 / commit_latency` — about 1–3k/s
per shard on local disks. For large limits, split the counter:

```elixir
concurrency_limits: [stripe: [limit: 1000, shards: 10]]
```

Claims draw from the aggregate (spread across shards); releases credit back the shard
they came from. Size it as `shards ≥ limit × commit_latency / step_duration` — e.g.
limit 1000, 100 ms steps, 1 ms commits ⇒ 10 shards. A gate is only hot if its key is hot,
and the cap itself throttles the key, so defaults rarely need touching below `limit ≈ 500`.

## Releasing or switching the key mid-flight

The key **persists across steps** by default (identity semantics). A step that no longer
needs it can release it, or switch it, per transition:

```elixir
{:next, "wrap_up", state, concurrency_key: nil}          # release
{:next, "call_api", state, concurrency_key: {:stripe, t}} # switch (admitted at next claim)
```

## ⚠ Config names capture key prefixes

`concurrency_limits: [order: [limit: 5]]` applies to **every** key named `order:*` —
including ones you meant as mutual-exclusion identities. That silently turns "steps of
order:42 never overlap" into "five at a time" and breaks the exclusion. Keep gate names
(integrations: `stripe`, `openai`) disjoint from identity names (entities: `order`,
`account`), and treat adding a config as a semantics change for every existing key with
that prefix.

## Concurrency key vs queue concurrency vs rate limit

| Knob | Bounds | Set by |
|---|---|---|
| **queue concurrency** | how many run at once on a node | `queues: [default: 10]` |
| **`concurrency_key`** | K-at-a-time per key, cluster-wide (K = 1 default) | per-instance `concurrency_key:` + `concurrency_limits:` |
| **[rate limit](rate_limiting.md)** | how many *start* per period | per-step `rate_limit:` |

They compose: a key can be serialized/gated *and* rate-limited; the pick admits a row
only when it passes both, and debits neither limit for a row the other rejected.
