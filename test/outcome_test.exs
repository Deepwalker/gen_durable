defmodule GenDurable.OutcomeTest do
  use ExUnit.Case, async: true

  alias GenDurable.Outcome

  test "validates and normalizes each outcome" do
    assert {:ok, {:next, "ship", %{n: 1}}} = Outcome.validate({:next, :ship, %{n: 1}})
    assert {:ok, {:next, "ship", :st}} = Outcome.validate({:next, "ship", :st})
    assert {:ok, {:replay, :st, 500}} = Outcome.validate({:replay, :st, 500})
    # await: a single name or a list, both normalize to a list of strings
    assert {:ok, {:await, ["go"], "woke", :st}} = Outcome.validate({:await, :go, :woke, :st})

    assert {:ok, {:await, ["go", "stop"], "woke", :st}} =
             Outcome.validate({:await, ["go", :stop], "woke", :st})

    assert {:ok, {:done, %{"ok" => true}}} = Outcome.validate({:done, %{"ok" => true}})
    assert {:ok, {:stop, :boom}} = Outcome.validate({:stop, :boom})
  end

  test "rejects malformed outcomes" do
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:replay, :st, -1})
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:done, "not a map"})
    # the old 3-tuple await is no longer a valid outcome
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, :go, :st})
    # await with an empty name set is invalid
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, [], "woke", :st})
    assert {:error, {:bad_outcome, _}} = Outcome.validate(:nonsense)
  end

  test "validate! raises on bad shape" do
    assert_raise ArgumentError, fn -> Outcome.validate!({:weird, 1}) end
  end

  test "kind returns the tag" do
    assert Outcome.kind({:next, "x", %{}}) == :next
    assert Outcome.kind({:done, %{}}) == :done
  end
end
