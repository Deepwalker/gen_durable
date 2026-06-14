defmodule GenDurable.FSMTest do
  use ExUnit.Case, async: true

  test "defining both step/2 and perform raises at compile time" do
    assert_raise CompileError, ~r/both step\/2 and perform/, fn ->
      Code.compile_string("""
      defmodule GenDurable.Test.BadBoth do
        use GenDurable.FSM
        def step(_s, _ctx), do: {:done, %{}}
        def perform(_args), do: :ok
      end
      """)
    end
  end

  test "defining neither step nor perform raises at compile time" do
    assert_raise CompileError, ~r/must define step\/2/, fn ->
      Code.compile_string("""
      defmodule GenDurable.Test.BadNeither do
        use GenDurable.FSM
      end
      """)
    end
  end

  test "default backoff is a capped exponential (ms)" do
    assert GenDurable.FSM.__backoff__(0) == 1_000
    assert GenDurable.FSM.__backoff__(1) == 2_000
    assert GenDurable.FSM.__backoff__(3) == 8_000
    # capped at 5 minutes
    assert GenDurable.FSM.__backoff__(20) == 300_000
  end

  test "a job's state schema can still be adopted by convention" do
    assert GenDurable.Test.JobOk.__gd_state__() == nil
    assert GenDurable.Test.Counter.__gd_state__() == GenDurable.Test.Counter.State
  end
end
