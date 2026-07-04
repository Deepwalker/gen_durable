defmodule GenDurable.Outcome do
  @moduledoc """
  The step/handle outcomes, with shape validation.

      {:next, step, state}        # transition, runnable, attempt := 0
      {:next, step, state, opts}  # …with per-transition opts: rate_limit, weight
      {:retry, state, delay_ms}   # same step, runnable, attempt += 1, eligible_at += delay
      {:await, names, next_step, state}       # park; on any of `names`, run next_step
      {:await, names, next_step, state, opts} # …with opts: timeout (ms; wake with what's
                                              # in the inbox when it fires)
      {:done, result}             # terminal, done
      {:stop, reason}             # terminal, failed

  Step names and signal names are normalized to strings. `:await` accepts a single
  name or a list of names; both normalize to a list. `:next` accepts an optional 4th
  keyword `opts` (`rate_limit:`, `weight:`), `:await` an optional 5th (`timeout:`);
  both normalize to an opts map in the tuple's last position.
  """

  @type next_opts :: %{rate_limit: String.t() | nil, weight: number()}
  @type await_opts :: %{timeout: pos_integer() | nil}

  @type t ::
          {:next, String.t(), term(), next_opts()}
          | {:retry, term(), non_neg_integer()}
          | {:await, [String.t()], String.t(), term(), await_opts()}
          | {:schedule_childs, String.t(), [term()], term()}
          | {:done, map()}
          | {:stop, term()}

  @spec validate(term()) :: {:ok, t()} | {:error, {:bad_outcome, term()}}
  def validate({:next, step, state}) when is_binary(step) or is_atom(step),
    do: {:ok, {:next, to_string(step), state, %{rate_limit: nil, weight: 1}}}

  def validate({:next, step, state, opts})
      when (is_binary(step) or is_atom(step)) and is_list(opts) do
    with {:ok, rl} <- normalize_rate_limit(Keyword.get(opts, :rate_limit)),
         {:ok, w} <- normalize_weight(Keyword.get(opts, :weight)) do
      {:ok, {:next, to_string(step), state, %{rate_limit: rl, weight: w}}}
    else
      :error -> {:error, {:bad_outcome, {:next, step, state, opts}}}
    end
  end

  def validate({:retry, state, delay}) when is_integer(delay) and delay >= 0,
    do: {:ok, {:retry, state, delay}}

  def validate({:await, names, next_step, state}),
    do: validate({:await, names, next_step, state, []})

  def validate({:await, names, next_step, state, opts})
      when (is_binary(next_step) or is_atom(next_step)) and is_list(opts) do
    list = List.wrap(names)
    timeout = Keyword.get(opts, :timeout)

    if list != [] and Enum.all?(list, &(is_binary(&1) or is_atom(&1))) and
         (is_nil(timeout) or (is_integer(timeout) and timeout > 0)),
       do:
         {:ok,
          {:await, Enum.map(list, &to_string/1), to_string(next_step), state, %{timeout: timeout}}},
       else: {:error, {:bad_outcome, {:await, names, next_step, state, opts}}}
  end

  def validate({:schedule_childs, step, children, state})
      when (is_binary(step) or is_atom(step)) and is_list(children),
      do: {:ok, {:schedule_childs, to_string(step), children, state}}

  def validate({:done, result}) when is_map(result),
    do: {:ok, {:done, result}}

  def validate({:stop, reason}),
    do: {:ok, {:stop, reason}}

  def validate(other), do: {:error, {:bad_outcome, other}}

  # rate_limit key: nil | name | {name, partition} -> nil | "name" | "name:partition"
  defp normalize_rate_limit(nil), do: {:ok, nil}

  defp normalize_rate_limit(name) when is_binary(name) or is_atom(name),
    do: {:ok, to_string(name)}

  defp normalize_rate_limit({name, partition})
       when (is_binary(name) or is_atom(name)) and
              (is_binary(partition) or is_atom(partition) or is_integer(partition)),
       do: {:ok, "#{name}:#{partition}"}

  defp normalize_rate_limit(_), do: :error

  defp normalize_weight(nil), do: {:ok, 1}
  defp normalize_weight(w) when is_number(w) and w > 0, do: {:ok, w}
  defp normalize_weight(_), do: :error

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
