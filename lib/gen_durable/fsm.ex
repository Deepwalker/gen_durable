defmodule GenDurable.FSM do
  @moduledoc """
  Behaviour for a durable FSM definition.

      defmodule Checkout do
        use GenDurable.FSM, version: 1, queue: "checkout", state: Checkout.State

        @impl true
        def step("start", %{state: s}), do: {:next, "await_pay", %{s | n: s.n + 1}}
        def step("await_pay", ctx) do
          case Enum.find(ctx.signals, & &1.name == "payment_confirmed") do
            nil -> {:await, "payment_confirmed", ctx.state}
            sig -> {:next, "ship", apply_payment(ctx.state, sig)}
          end
        end
        def step("ship", _ctx), do: {:done, %{"shipped" => true}}

        @impl true
        def handle(reason, ctx) do
          if ctx.attempt < 5, do: {:replay, ctx.state, backoff(ctx.attempt)}, else: {:stop, reason}
        end
      end

  ## `use` options

    * `:name`    — FSM name stored in the `fsm` column (default: `inspect(module)`).
    * `:version` — `fsm_version` (default `1`). Old versions coexist as separate
      registered modules; see `GenDurable.Registry`.
    * `:queue`   — default queue for instances (default `"default"`).
    * `:state`   — the `GenDurable.State` embedded-schema module (default `nil`,
      i.e. plain-map state).
    * `:initial` — initial step for `GenDurable.insert/2` (default `"start"`).

  `handle/2` defaults to `{:stop, reason}` and is overridable.
  """

  alias GenDurable.{Context, Outcome}

  @callback step(step :: String.t(), ctx :: Context.t()) :: Outcome.t() | term()
  @callback handle(reason :: term(), ctx :: Context.t()) :: Outcome.t() | term()

  defmacro __using__(opts) do
    quote do
      @behaviour GenDurable.FSM
      @gd_opts unquote(opts)

      def __gd_name__, do: Keyword.get(@gd_opts, :name, inspect(__MODULE__))
      def __gd_version__, do: Keyword.get(@gd_opts, :version, 1)
      def __gd_queue__, do: Keyword.get(@gd_opts, :queue, "default")
      def __gd_state__, do: Keyword.get(@gd_opts, :state)
      def __gd_initial__, do: Keyword.get(@gd_opts, :initial, "start")

      @impl true
      def handle(reason, _ctx), do: {:stop, reason}

      defoverridable handle: 2
    end
  end
end
