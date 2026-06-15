defmodule GenDurable.Outcome do
  @moduledoc """
  The five step/handle outcomes from the spec §3, with shape validation.

      {:next, step, state}        # transition, runnable, attempt := 0
      {:replay, state, delay_ms}  # same step, runnable, attempt += 1, eligible_at += delay
      {:await, names, next_step, state} # park; on any of `names`, run next_step (ctx.awaited)
      {:done, result}             # terminal, done
      {:stop, reason}             # terminal, failed

  Step names and signal names are normalized to strings. `:await` accepts a single
  name or a list of names; both normalize to a list.
  """

  @type t ::
          {:next, String.t(), term()}
          | {:replay, term(), non_neg_integer()}
          | {:await, [String.t()], String.t(), term()}
          | {:schedule_childs, String.t(), [term()], term()}
          | {:done, map()}
          | {:stop, term()}

  @spec validate(term()) :: {:ok, t()} | {:error, {:bad_outcome, term()}}
  def validate({:next, step, state}) when is_binary(step) or is_atom(step),
    do: {:ok, {:next, to_string(step), state}}

  def validate({:replay, state, delay}) when is_integer(delay) and delay >= 0,
    do: {:ok, {:replay, state, delay}}

  def validate({:await, names, next_step, state})
      when is_binary(next_step) or is_atom(next_step) do
    list = List.wrap(names)

    if list != [] and Enum.all?(list, &(is_binary(&1) or is_atom(&1))),
      do: {:ok, {:await, Enum.map(list, &to_string/1), to_string(next_step), state}},
      else: {:error, {:bad_outcome, {:await, names, next_step, state}}}
  end

  def validate({:schedule_childs, step, children, state})
      when (is_binary(step) or is_atom(step)) and is_list(children),
      do: {:ok, {:schedule_childs, to_string(step), children, state}}

  def validate({:done, result}) when is_map(result),
    do: {:ok, {:done, result}}

  def validate({:stop, reason}),
    do: {:ok, {:stop, reason}}

  def validate(other), do: {:error, {:bad_outcome, other}}

  @spec validate!(term()) :: t()
  def validate!(outcome) do
    case validate(outcome) do
      {:ok, o} -> o
      {:error, reason} -> raise ArgumentError, "invalid gen_durable outcome: #{inspect(reason)}"
    end
  end

  @doc "The outcome's tag, for telemetry/metadata."
  def kind(outcome), do: elem(outcome, 0)
end
