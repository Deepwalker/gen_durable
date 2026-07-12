# Signals & await

`await` parks an instance until a signal arrives from the outside — a webhook, a human
approval, a partner callback. It is the durable answer to "wait for an external event."
(Distinct from `GenDurable.await/3`, which is the *caller* waiting for an instance's
result — see [the jobs guide](jobs.md#waiting-for-a-result-sync-over-async).)

```elixir
def step("start", ctx), do: {:await, "payment_confirmed", "ship", ctx.state}
def step("ship", ctx),  do: {:done, %{"paid" => hd(ctx.awaited).payload}}
```

`{:await, names, next_step, state}` parks the instance (`awaiting_signal`) on a **name or a set
of names**. When any of them arrives, the engine runs `next_step` with the matched signals in
`ctx.awaited`. The whole inbox is always in `ctx.all`.

## Delivering a signal

```elixir
GenDurable.signal(id, "payment_confirmed", %{amount: 100}, dedup_key: "evt-7")
```

Address by the internal id, or by a [`correlation_key`](identity.md) (a business key you set at
insert). Signals are **durable** (a row in the inbox) and **at-least-once** — a signal that
arrives before, or concurrently with, the park is not lost (delivery inserts the row first and
then takes the instance's row lock to wake it; parking rechecks the inbox under the same lock,
so neither side can slip between the other's check and commit), and
`:dedup_key` makes redelivery idempotent. Delivery wakes the instance **only on a name match**;
non-matching signals stay in the inbox for a later `await`. A signal addressed to a terminal
or nonexistent instance is refused as `{:error, :no_target}` — nothing would ever read it.

## Consumption

A woken step sees only the **subset it awaited** as `ctx.awaited` (the engine pre-filters the
set you parked on). On a progressing outcome (`:next`/`:schedule_childs`) the engine deletes
exactly the signal ids the step received — latecomers and never-awaited signals survive. A
terminal outcome (`:done`/`:stop`) clears the whole inbox.

### Accumulating a pack

Because consumption is by received id, you can **wait for a whole set** by re-awaiting until
all of them have arrived, then progress once:

```elixir
@names ["a", "b", "c"]

def step("collect", ctx) do
  have = MapSet.new(ctx.awaited, & &1.name)

  if MapSet.size(have) == length(@names) do
    {:done, %{"sum" => ctx.awaited |> Enum.map(& &1.payload["v"]) |> Enum.sum()}}
  else
    {:await, @names, "collect", ctx.state}   # re-await: consumes nothing, the pack accumulates
  end
end
```

`{:retry, ...}` on an awaiting step also keeps the awaited inputs, so a redo re-sees them.

Re-awaiting is cheap: the engine wakes a park only on signals the parking step was **not**
handed, so re-awaiting while your inputs sit unconsumed in the inbox parks cleanly — the
instance sleeps until the next new arrival, it does not spin.

## Timeouts

`{:await, names, next_step, state, timeout: 30_000}` arms a deadline: if no matching signal
arrives in time, the engine wakes the instance anyway and runs `next_step`. A timeout is a
**wake, not a failure** — `attempt` is untouched, and the step distinguishes the cases by
what it received:

```elixir
def step("wait", ctx), do: {:await, "payment", "decide", ctx.state, timeout: :timer.hours(1)}

def step("decide", %{awaited: []} = ctx), do: {:stop, "payment never came"}  # timed out
def step("decide", ctx), do: {:next, "ship", ctx.state}                      # signal arrived
```

For a fresh await, empty `ctx.awaited` on wake means the deadline fired. In the
[accumulate pattern](#accumulating-a-pack), a timeout wakes you with the **partial pack** —
"proceed with what arrived" falls out naturally. Timeout resolution is bounded by
the reaper's `interval` (the sweep that fires them; default 30s), so treat the deadline as
"at least this long", not a precise timer.

## Under the hood

`await` is sugar over a raw signal model: every instance has a durable inbox (`ctx.all`), and a
step can park on a name set. You can always write a step that inspects `ctx.all` itself and
decides what to do — `await` just pre-filters and consumes the set you named.
