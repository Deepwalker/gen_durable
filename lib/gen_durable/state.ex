defmodule GenDurable.State do
  @moduledoc """
  Typed FSM state: an Ecto embedded schema per FSM, encoded to/from jsonb.
  Typically defined as a nested `State` module inside the FSM, where it is
  adopted by convention (see `GenDurable.FSM`):

      defmodule Checkout do
        use GenDurable.FSM, version: 1

        defmodule State do
          use GenDurable.State

          embedded_schema do
            field :order, :integer
            field :n, :integer, default: 0
          end
        end
      end

  The engine loads the jsonb column into the struct before `step/2`
  (`from_db/2`) and dumps the returned struct back to jsonb on the outcome
  (`to_db/2`). FSMs may also run with no state module, in which case state is a
  plain string-keyed map (allowed, but unsupported — you are on your own).
  """

  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema

      @primary_key false
    end
  end

  @doc "Load a DB jsonb value (raw string or already-decoded map) into the FSM state."
  def from_db(nil, data), do: decode(data)
  def from_db(module, data), do: Ecto.embedded_load(module, decode(data), :json)

  @doc "Dump an FSM state value returned by a step into a JSON string for a jsonb param."
  def to_db(_module, %_{} = struct), do: Jason.encode!(Ecto.embedded_dump(struct, :json))
  def to_db(_module, map) when is_map(map), do: Jason.encode!(map)

  @doc """
  Normalize user-supplied state (for `insert`) into a JSON string. Accepts the
  state struct, or a map (atom or string keys) which is cast through the schema.
  """
  def cast(nil, value) when is_map(value), do: Jason.encode!(value)
  def cast(module, %_{} = struct), do: to_db(module, struct)

  def cast(module, map) when is_map(map),
    do: to_db(module, Ecto.embedded_load(module, json_normalize(map), :json))

  defp decode(data) when is_binary(data), do: Jason.decode!(data)
  defp decode(data) when is_map(data), do: data
  defp decode(nil), do: %{}

  defp json_normalize(map), do: Jason.decode!(Jason.encode!(map))
end
