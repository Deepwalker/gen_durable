# Testing

`GenDurable.Testing` runs your FSMs **inline**: no engine, no background processes —
`drain/1` synchronously executes everything runnable in the calling process, through the
production pick/executor SQL. Deterministic, fast, and compatible with
`Ecto.Adapters.SQL.Sandbox` (every query runs in the test process, so sandbox ownership
just works).

```elixir
defmodule MyApp.CheckoutTest do
  use ExUnit.Case, async: true
  use GenDurable.Testing, repo: MyApp.Repo

  test "checkout waits for payment, then ships" do
    {:ok, id} = GenDurable.insert(Checkout, state: %{order: 42}, repo: MyApp.Repo)

    assert %{await: 1} = drain()
    assert_awaiting(id, "payment_confirmed")

    :ok = GenDurable.signal(id, "payment_confirmed", %{amount: 100}, repo: MyApp.Repo)

    assert %{done: 1} = drain()
    assert_done(id, %{"shipped" => true})
  end
end
```

Don't start the engine in the test env; pass `repo:` to `insert`/`signal` explicitly.
FSMs with a custom `:name` need registering, as with the engine:
`use GenDurable.Testing, repo: MyApp.Repo, fsms: [My.CustomNamed]`.

## drain/1

Runs until quiescence and returns a tally you can pattern-match:

```elixir
assert %{steps: 5, next: 3, await: 1, done: 1} = drain()
```

- Child fan-outs run to the join in the same drain (children execute inline too).
- Scheduled work and retry backoffs are **collapsed by default** (`with_scheduled: false`
  to keep future work parked) — a Crasher that retries twice and stops shows up as
  `%{retry: 2, stop: 1}` without waiting out the backoffs.
- `queue:` drains one queue; default is all.
- `max_steps:` (default 1000) fails the test with a clear error instead of hanging when an
  FSM transitions or retries forever.

## Assertions

- `assert_status(target, :executing | :runnable | …)` — status, with a helpful failure
  message (step, attempt, last_error).
- `assert_awaiting(target, "payment")` — parked on the given name(s).
- `assert_done(target, %{"shipped" => true})` — done, with the result (`:any` to skip).
- `assert_failed(target, ~r/timeout/)` — failed, `last_error` by `==` or regex.
- `durable(target)` — the full row map (`status`, `step`, `state`, `result`,
  `last_error`, `attempt`, `awaits`) for arbitrary asserts.

`target` is the id from `insert`, or a correlation key — the helper falls back to the
latest row carrying the key, so a **finished** instance is still assertable by key (the
public API deliberately doesn't do this; in a test you own the lifecycle and GC isn't
running).

## Timeouts

Await deadlines are fired by the engine's reaper, which isn't running inline. Force them:

```elixir
assert %{await: 1} = drain()
fire_timeouts()                 # every armed deadline fires, however much time is left
assert %{done: 1} = drain()
```

## Inline semantics vs the engine

Steps run one at a time in pick order, so `concurrency_key` serialization is trivially
satisfied (no advisory locks are taken). A raise in a step routes to `handle/2` exactly as
in production; a bare `exit` crashes the test process — there is no reaper inline, which
also means crash-recovery paths (lease expiry, reclaim) are engine behavior you cover with
an engine test, not inline.
