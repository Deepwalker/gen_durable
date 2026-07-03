# Concurrency keys

A `concurrency_key` **serializes** the steps of instances that share the key — at most one runs
at a time per key, with strict ordering — while instances with different keys run in parallel.
It is the answer to "process everything for one account/order one-at-a-time, but accounts in
parallel."

```elixir
GenDurable.insert(Account.Sync, state: %{id: 7}, concurrency_key: "account:7")
GenDurable.insert(Account.Sync, state: %{id: 7}, concurrency_key: "account:7")  # waits for the first
GenDurable.insert(Account.Sync, state: %{id: 9}, concurrency_key: "account:9")  # runs in parallel
```

The picker never claims more than one row per `concurrency_key`, and never claims a key that is
already executing, so steps sharing a key cannot overlap. Enforcement is a session-level
Postgres **advisory lock** held for the duration of the step; a worker death drops the
connection and releases the lock automatically (crash-safe).

A `NULL` `concurrency_key` (the common case) never serializes — those instances run with the
full queue concurrency and pay nothing for the machinery.

## Concurrency key vs queue concurrency vs rate limit

Three different knobs, often used together:

| Knob | Bounds | Set by |
|---|---|---|
| **queue concurrency** | how many run at once on a node | `queues: [default: 10]` |
| **`concurrency_key`** | one-at-a-time per key (any node) | per-instance `concurrency_key:` |
| **[rate limit](rate_limiting.md)** | how many *start* per period | per-step `rate_limit:` |

`concurrency_key` is about **ordering and mutual exclusion** per key; a rate limit is about
**throughput over time**. They compose: a key can be both serialized and rate-limited.
