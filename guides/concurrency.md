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
  slot back. Counters are minted lazily, **pre-debited, by the first claim itself** — a
  cold gate (first use, or swept by GC after the key went idle) admits with zero lag.
  Crash paths under-credit (the safe direction: temporarily stricter than K)
  and the GC reconciler repairs the counters from the executing-rows truth each sweep.
- An await **releases** the slot (parking is not executing); the woken step re-admits
  through the gate. Prefetch-buffered rows hold slots (they are claimed).

A `NULL` `concurrency_key` (the common case) never serializes and pays nothing for any of
this machinery.

> **A saturated gate's backlog crowds its queue.** Rows denied by a full gate stay
> runnable and keep occupying the pick window (unlike K = 1 siblings, which are filtered
> out for free), so a deep backlog on one saturated key can starve *unrelated*
> same-priority work behind it until completions free slots. Give a high-volume gated
> flow its own queue — see the honest-list entry in the
> [performance notes](../PERFORMANCE.md).

## Sharding a big gate

A gate's `cap` is split across `shards` slot-counter rows (default 1):

```elixir
concurrency_limits: [stripe: [limit: 1000, shards: 10]]
```

Sharding buys two things. **Pick-side parallelism**: each pick locks the shards it needs
with `FOR UPDATE OF b SKIP LOCKED`, so pickers on different nodes take *disjoint* shards and
admit in parallel instead of serializing (or blocking) on one row — size `shards ≥ the nodes
that contend the key`. **Release-side throughput**: completions credit back the shard they
came from (a row lock held to commit), so one key's completions spread across shard rows
instead of serializing at `1 / commit_latency` (≈1–3k/s on local disks) — size
`shards ≥ limit × commit_latency / step_duration` (limit 1000, 100 ms steps, 1 ms commits ⇒
10 shards). Take the larger of the two. A lone picker grabs all shards and admits up to the
full cap, identical to an unsharded gate; a gate is only hot if its key is hot, and the cap
itself throttles the key, so defaults rarely need touching below `limit ≈ 500`. `shards` is
clamped to `limit` (more shards than slots is nonsense).

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
