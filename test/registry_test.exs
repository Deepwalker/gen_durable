defmodule GenDurable.RegistryTest do
  # async: false — the registry uses a globally-named process + ETS table.
  use ExUnit.Case, async: false

  alias GenDurable.Registry
  alias GenDurable.Test.{Auto, Counter, Awaiter}

  setup do
    start_supervised!({Registry, fsms: [Counter, Awaiter]})
    :ok
  end

  test "resolves {name, version} to the module" do
    assert Registry.fetch!("counter", 1) == Counter
    assert Registry.fetch!("awaiter", 1) == Awaiter
  end

  test "resolves an unregistered FSM by its default (module) name and version" do
    # Auto is not in :fsms; its name defaults to its module name.
    assert Registry.fetch!("GenDurable.Test.Auto", 1) == Auto
  end

  test "raises NotFound for an unknown machine, a custom-name miss, or a version mismatch" do
    # "counter" is a custom name (not a module) → no dynamic fallback.
    assert_raise Registry.NotFound, fn -> Registry.fetch!("counter", 2) end
    assert_raise Registry.NotFound, fn -> Registry.fetch!("nope", 1) end
    # Right module, wrong version → not accepted dynamically.
    assert_raise Registry.NotFound, fn -> Registry.fetch!("GenDurable.Test.Auto", 2) end
  end
end
