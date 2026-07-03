# Signals & await

`await` parks an instance until a signal arrives from the outside — a webhook, a human
approval, a partner callback. It is the durable answer to "wait for an external event."

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
non-matching signals stay in the inbox for a later `await`.

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

## Under the hood

`await` is sugar over a raw signal model: every instance has a durable inbox (`ctx.all`), and a
step can park on a name set. You can always write a step that inspects `ctx.all` itself and
decides what to do — `await` just pre-filters and consumes the set you named.
