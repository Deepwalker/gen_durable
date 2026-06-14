defmodule GenDurable.Context do
  @moduledoc """
  The context handed to `c:GenDurable.FSM.step/2` and `c:GenDurable.FSM.handle/2`.

  `state` is the FSM's loaded state (a struct when the FSM declares `state:`,
  otherwise a plain string-keyed map). `signals` holds the instance inbox at the
  moment the step started (empty for `handle/2`); see `GenDurable.FSM`.
  """

  @enforce_keys [:id, :fsm, :fsm_version, :step, :attempt, :state]
  defstruct [:id, :fsm, :fsm_version, :step, :attempt, :state, signals: []]

  @type t :: %__MODULE__{
          id: integer(),
          fsm: String.t(),
          fsm_version: integer(),
          step: String.t(),
          attempt: non_neg_integer(),
          state: term(),
          signals: [%{id: integer(), name: String.t(), payload: map()}]
        }
end
