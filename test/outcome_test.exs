defmodule GenDurable.OutcomeTest do
  use ExUnit.Case, async: true

  alias GenDurable.Outcome

  test "validates and normalizes each outcome" do
    assert {:ok, {:next, "ship", %{n: 1}}} = Outcome.validate({:next, :ship, %{n: 1}})
    assert {:ok, {:next, "ship", :st}} = Outcome.validate({:next, "ship", :st})
    assert {:ok, {:replay, :st, 500}} = Outcome.validate({:replay, :st, 500})
    assert {:ok, {:await, "go", :st}} = Outcome.validate({:await, :go, :st})
    assert {:ok, {:done, %{"ok" => true}}} = Outcome.validate({:done, %{"ok" => true}})
    assert {:ok, {:stop, :boom}} = Outcome.validate({:stop, :boom})
  end

  test "rejects malformed outcomes" do
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:replay, :st, -1})
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:done, "not a map"})
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
