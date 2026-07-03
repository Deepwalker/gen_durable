# Child fan-out

`schedule_childs` spawns a batch of child instances and parks the parent on a **join barrier**
that releases only when every child reaches a terminal status. It is "fan work out, then wait
for all of it."

```elixir
def step("fan", %{state: s}) do
  children = for kid <- s["kids"], do: {LineItem, state: kid}
  {:schedule_childs, "join", children, s}
end

def step("join", %{childs: childs}) do
  {:done, %{"shipped" => Enum.count(childs, &(&1.status == "done"))}}
end
```

`{:schedule_childs, next_step, children, state}`:

- **`children`** is a list of child specs, each `{FsmModule, insert_opts}` (or a bare
  `FsmModule`). They are ordinary instances, stamped with a `parent_id`.
- The spawn and the park happen **atomically** — children become runnable only once the parent
  is parked, so a child can never finish before the barrier exists.
- The parent re-runs **`next_step`** (never the spawning step) once all children are terminal.

## The join

When the parent wakes, its children are in `ctx.childs`:

```elixir
[%{id: _, fsm: _, status: "done" | "failed", state: _, result: _, last_error: _}, ...]
```

The barrier waits for **termination, not success** — a child that ends `failed` still releases
its slot. The parent sees the failure in `ctx.childs` and decides what to do (proceed,
`{:stop, _}`, compensate). The engine does not auto-propagate child failure.

## Not covered

- **No cancel/cascade** — terminating or failing a parent does not touch its children, and
  vice-versa.
- **No quorum** — the join is all-or-nothing; build `k`-of-`n` yourself with
  [`await`](signals.md) + per-child signals.
- **No barrier deadline** — a child that hangs forever hangs the parent; bound it inside the
  child (e.g. `{:retry, ...}` with a timeout).
- **Nesting is free** — a child may itself `schedule_childs`, each level an independent barrier.

Children are ordinary terminal rows after they finish; they are reaped by
[GC](operations.md#garbage-collection) like any other terminal instance, not deleted on read.
