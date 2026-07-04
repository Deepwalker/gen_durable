defmodule GenDurable.OutcomeTest do
  use ExUnit.Case, async: true

  alias GenDurable.Outcome

  test "validates and normalizes each outcome" do
    # :next normalizes to a 4-tuple carrying the per-transition opts map (spec §12)
    assert {:ok, {:next, "ship", %{n: 1}, %{rate_limit: nil, weight: 1}}} =
             Outcome.validate({:next, :ship, %{n: 1}})

    assert {:ok, {:next, "ship", :st, %{rate_limit: nil, weight: 1}}} =
             Outcome.validate({:next, "ship", :st})

    # :next with opts: rate_limit name | {name, partition} and weight
    assert {:ok, {:next, "charge", :st, %{rate_limit: "stripe", weight: 1}}} =
             Outcome.validate({:next, "charge", :st, rate_limit: :stripe})

    assert {:ok, {:next, "charge", :st, %{rate_limit: "stripe:42", weight: 50}}} =
             Outcome.validate({:next, "charge", :st, rate_limit: {:stripe, 42}, weight: 50})

    assert {:ok, {:retry, :st, 500}} = Outcome.validate({:retry, :st, 500})

    # await: a single name or a list, both normalize to a list of strings; the
    # optional timeout normalizes into the opts map (5-tuple)
    assert {:ok, {:await, ["go"], "woke", :st, %{timeout: nil}}} =
             Outcome.validate({:await, :go, :woke, :st})

    assert {:ok, {:await, ["go", "stop"], "woke", :st, %{timeout: nil}}} =
             Outcome.validate({:await, ["go", :stop], "woke", :st})

    assert {:ok, {:await, ["go"], "woke", :st, %{timeout: 30_000}}} =
             Outcome.validate({:await, :go, :woke, :st, timeout: 30_000})

    assert {:ok, {:done, %{"ok" => true}}} = Outcome.validate({:done, %{"ok" => true}})
    assert {:ok, {:stop, :boom}} = Outcome.validate({:stop, :boom})
  end

  test "rejects malformed outcomes" do
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:retry, :st, -1})
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:done, "not a map"})
    # the old 3-tuple await is no longer a valid outcome
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, :go, :st})
    # await with an empty name set is invalid
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, [], "woke", :st})
    # await timeout must be a positive integer (or absent)
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, :go, :woke, :st, timeout: 0})
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:await, :go, :woke, :st, timeout: "x"})
    # :next with a bad weight (non-positive) is invalid
    assert {:error, {:bad_outcome, _}} = Outcome.validate({:next, "x", :st, weight: 0})
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
