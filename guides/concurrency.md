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
  dies. No per-step locks, no pinned connections. The picker additionally *pre-filters*
  candidates whose key is already executing, so a blocked row is skipped instead of
  bouncing off the arbiter — an optimization over the index, which stays the correctness.
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

## ⚠ Changing a gate's config while its rows are executing

Which regime a row belongs to is decided **when it is claimed** and recorded on the row, in
`concurrency_shard`: a K = 1 claim leaves it `NULL` (the unique arbiter polices the row), a
gated claim stamps a shard number (the slot counters police it, and the row drops out of the
arbiter's partial index). That marker lives until the row leaves `executing` — so if the
config changes in between, a running row keeps being policed by the regime it was *born*
into, while new claims for the same key use the new one.

Both directions can therefore admit **one row more than the current config promises**, for
as long as the pre-change rows keep running:

| Change | What happens |
|---|---|
| Name **added** to `concurrency_limits:` | Rows already executing under K = 1 carry a `NULL` shard. New claims take the gated path, which does not consult the K = 1 guard at all, so a gated row can run beside that pre-existing one — `limit + 1` in flight. |
| Name **removed** from `concurrency_limits:` | Rows already executing under the gate carry a stamped shard, which puts them outside both the arbiter's index and the K = 1 guard. A new unconfigured claim of the same key is admitted beside them — two executing rows where K = 1 now promises one. |

This is not a transient of the *deploy command*, it is a transient of the **rolling deploy**:
each node builds its gate-name set from its own config at boot, so while a release rolls out,
some nodes read a key as gated and others as K = 1 for minutes at a time.

The window closes by itself once the pre-change rows finish. If a key must never
double-execute across a config change, drain it first (stop enqueuing, let it go quiet) and
change the config after — or keep the gate name and change only its `limit`, which never
reclassifies a row.

> The "removed" row of that table became reachable in 0.2.15, when the picker's K = 1 guard
> was narrowed to exactly the rows the arbiter polices — the change that made a claim's cost
> independent of how much work the cluster has in flight (see [performance notes](../PERFORMANCE.md)
> §2.8 and `ISSUES.md` #30). The "added" row has always behaved this way.

## Concurrency key vs queue concurrency vs rate limit

| Knob | Bounds | Set by |
|---|---|---|
| **queue concurrency** | how many run at once on a node | `queues: [default: 10]` |
| **`concurrency_key`** | K-at-a-time per key, cluster-wide (K = 1 default) | per-instance `concurrency_key:` + `concurrency_limits:` |
| **[rate limit](rate_limiting.md)** | how many *start* per period | per-step `rate_limit:` |

They compose: a key can be serialized/gated *and* rate-limited; the pick admits a row
only when it passes both, and debits neither limit for a row the other rejected.
