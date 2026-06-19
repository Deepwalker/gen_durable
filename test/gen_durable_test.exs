defmodule GenDurableTest do
  use ExUnit.Case, async: false

  alias GenDurable.Test.Repo

  test "schema is migrated and a gen_durable row round-trips" do
    {:ok, %{rows: [[id]]}} =
      Repo.query(
        """
        INSERT INTO gen_durable (fsm, step, state)
        VALUES ($1, $2, $3::jsonb)
        RETURNING id
        """,
        ["Checkout", "start", ~s({"order": 42})]
      )

    assert is_integer(id)

    {:ok, %{rows: [[fsm, step, status, state, scope]]}} =
      Repo.query(
        "SELECT fsm, step, status::text, state, correlation_scope FROM gen_durable WHERE id = $1",
        [id]
      )

    assert fsm == "Checkout"
    assert step == "start"
    assert status == "runnable"
    # Ecto leaves json/jsonb undecoded at the driver level for raw queries.
    assert Jason.decode!(state) == %{"order" => 42}
    assert scope == []
  end

  test "build_params expands the :unique policy to a correlation_scope" do
    live = GenDurable.build_params(GenDurable.Test.Plain, correlation_key: "k")
    assert "runnable" in live.correlation_scope
    assert "done" not in live.correlation_scope

    global = GenDurable.build_params(GenDurable.Test.Plain, correlation_key: "k", unique: :global)
    assert "done" in global.correlation_scope
    assert "failed" in global.correlation_scope

    assert_raise ArgumentError, fn ->
      GenDurable.build_params(GenDurable.Test.Plain, unique: :bogus)
    end
  end

  test "signals table and FK to gen_durable exist" do
    {:ok, %{rows: [[target_id]]}} =
      Repo.query("INSERT INTO gen_durable (fsm, step) VALUES ('F', 's') RETURNING id", [])

    {:ok, %{num_rows: 1}} =
      Repo.query(
        "INSERT INTO signals (target_id, name) VALUES ($1, $2)",
        [target_id, "go"]
      )

    # FK violation on an unknown target
    assert {:error, _} =
             Repo.query("INSERT INTO signals (target_id, name) VALUES ($1, $2)", [
               -1,
               "go"
             ])
  end
end
