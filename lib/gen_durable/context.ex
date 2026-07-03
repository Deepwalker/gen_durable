defmodule GenDurable.Context do
  @moduledoc """
  The context handed to `c:GenDurable.FSM.step/2` and `c:GenDurable.FSM.handle/2`.

  `state` is the FSM's loaded state (a struct when the FSM declares `state:`,
  otherwise a plain string-keyed map).

  Two signal views: `awaited` is the subset the step's `:await` named (what the
  engine delivers and consumes — empty when the step was not reached via an await),
  and `all` is the full instance inbox. Both are empty for `handle/2`. `childs`
  holds this instance's children — populated when a parent wakes from
  `schedule_childs`, empty otherwise. See `GenDurable.FSM`.
  """

  @enforce_keys [:id, :fsm, :fsm_version, :step, :attempt, :state]
  defstruct [:id, :fsm, :fsm_version, :step, :attempt, :state, awaited: [], all: [], childs: []]

  @type signal :: %{id: integer(), name: String.t(), payload: map()}

  @type t :: %__MODULE__{
          id: integer(),
          fsm: String.t(),
          fsm_version: integer(),
          step: String.t(),
          attempt: non_neg_integer(),
          state: term(),
          awaited: [signal()],
          all: [signal()],
          childs: [
            %{
              id: integer(),
              fsm: String.t(),
              status: String.t(),
              state: map(),
              result: map() | nil,
              last_error: String.t() | nil
            }
          ]
        }
end
