defmodule GenDurable.RegistryTest do
  # async: true — the registry is now a per-instance unnamed ETS table, no
  # global state to collide on.
  use ExUnit.Case, async: true

  alias GenDurable.Registry
  alias GenDurable.Test.{Auto, Counter, Awaiter}

  setup do
    {:ok, table: Registry.new([Counter, Awaiter])}
  end

  test "resolves {name, version} to the module", %{table: t} do
    assert Registry.fetch!(t, "counter", 1) == Counter
    assert Registry.fetch!(t, "awaiter", 1) == Awaiter
  end

  test "resolves an unregistered FSM by its default (module) name and version", %{table: t} do
    # Auto is not in :fsms; its name defaults to its module name.
    assert Registry.fetch!(t, "GenDurable.Test.Auto", 1) == Auto
  end

  test "raises NotFound for an unknown machine, a custom-name miss, or a version mismatch",
       %{table: t} do
    # "counter" is a custom name (not a module) → no dynamic fallback.
    assert_raise Registry.NotFound, fn -> Registry.fetch!(t, "counter", 2) end
    assert_raise Registry.NotFound, fn -> Registry.fetch!(t, "nope", 1) end
    # Right module, wrong version → not accepted dynamically.
    assert_raise Registry.NotFound, fn -> Registry.fetch!(t, "GenDurable.Test.Auto", 2) end
  end

  test "two tables are independent (per-instance registries)" do
    only_counter = Registry.new([Counter])
    only_awaiter = Registry.new([Awaiter])

    assert Registry.fetch!(only_counter, "counter", 1) == Counter
    assert_raise Registry.NotFound, fn -> Registry.fetch!(only_awaiter, "counter", 1) end
  end
end
