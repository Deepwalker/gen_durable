defmodule GenDurable.RegistryTest do
  # async: false — the registry uses a globally-named process + ETS table.
  use ExUnit.Case, async: false

  alias GenDurable.Registry
  alias GenDurable.Test.{Counter, Awaiter}

  setup do
    start_supervised!({Registry, fsms: [Counter, Awaiter]})
    :ok
  end

  test "resolves {name, version} to the module" do
    assert Registry.fetch!("counter", 1) == Counter
    assert Registry.fetch!("awaiter", 1) == Awaiter
  end

  test "raises NotFound for an unknown machine" do
    assert_raise Registry.NotFound, fn -> Registry.fetch!("counter", 2) end
    assert_raise Registry.NotFound, fn -> Registry.fetch!("nope", 1) end
  end
end
