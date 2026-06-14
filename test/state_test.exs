defmodule GenDurable.StateTest do
  use ExUnit.Case, async: true

  alias GenDurable.State
  alias GenDurable.Test.Counter

  describe "typed struct state" do
    test "cast accepts a map (atom keys) and round-trips through the DB form" do
      json = State.cast(Counter.State, %{target: 3})
      assert State.from_db(Counter.State, json) == %Counter.State{target: 3, n: 0}
    end

    test "cast accepts the struct itself" do
      json = State.cast(Counter.State, %Counter.State{target: 5, n: 2})
      assert State.from_db(Counter.State, json) == %Counter.State{target: 5, n: 2}
    end

    test "to_db dumps a struct to JSON" do
      assert Jason.decode!(State.to_db(Counter.State, %Counter.State{n: 7, target: 9})) ==
               %{"n" => 7, "target" => 9}
    end

    test "from_db loads an empty jsonb to schema defaults" do
      assert State.from_db(Counter.State, "{}") == %Counter.State{target: 0, n: 0}
    end
  end

  describe "plain-map state (no module)" do
    test "round-trips a string-keyed map" do
      json = State.cast(nil, %{"a" => 1})
      assert json == ~s({"a":1})
      assert State.from_db(nil, json) == %{"a" => 1}
    end

    test "from_db decodes a raw jsonb string" do
      assert State.from_db(nil, ~s({"x":[1,2]})) == %{"x" => [1, 2]}
    end
  end
end
