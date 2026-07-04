# Instance identity: correlation_key

A `correlation_key` is an instance's **business identity** — one key that is both how you
**address** it (send it a signal without knowing its internal id) and how the engine
**deduplicates** it. It is the durable-execution model: the business key you assign is the same
key the engine enforces uniqueness on; the internal `id` is just a per-execution handle.

```elixir
{:ok, _id} = GenDurable.insert(Checkout, state: %{order: 42}, correlation_key: "order:42")

# later, from a webhook that only knows the business key:
GenDurable.signal("order:42", "payment_confirmed", %{amount: 100})
```

## Addressing

`signal/4` takes either the internal id (an integer) or a `correlation_key` (a string). A
correlation_key resolves to the single instance currently **occupying** it. If no live instance
occupies the key, delivery returns `{:error, :no_target}` — a finished instance can no longer be
woken, and a signal is not durably held for an instance that does not exist yet.

This removes the caller's need to keep its own "business key → engine id" mapping.

## Uniqueness

The same key is a uniqueness guard. Inserting a second instance whose key is currently occupied
is rejected:

```elixir
{:ok, _}                 = GenDurable.insert(Checkout, correlation_key: "order:42")
{:error, :duplicate}     = GenDurable.insert(Checkout, correlation_key: "order:42")
```

**`:correlation_scope`** is the set of statuses in which the key counts as "occupied". It
defaults to the non-terminal statuses — so a key is unique among **live** instances and is
**freed when the instance terminates** (you can start a fresh `"order:42"` once the previous one
is done). Override it to change the window:

```elixir
# never reuse the key, even after the instance finishes:
GenDurable.insert(Checkout, correlation_key: "order:42",
  correlation_scope: ~w(runnable executing awaiting_signal awaiting_children done failed))

# no uniqueness at all (a pure address — but addressing then needs the key to stay unique):
GenDurable.insert(Checkout, correlation_key: "order:42", correlation_scope: [])
```

> Including a terminal status (`done`/`failed`) in the scope keeps a *finished* instance
> occupying its key (`signal/4` to it is refused as `{:error, :no_target}` — nothing reads a
> terminal inbox). Keep terminal statuses out of the scope unless you specifically want the
> key reserved after the instance ends — and note the reservation lasts only as long as the
> row does: [GC](operations.md) deletes terminal rows after the retention window, freeing the
> key. For a longer reservation, raise `:gc_retention` (or disable GC).

With no `correlation_key`, an instance is neither addressable nor deduplicated.
