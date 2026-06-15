defmodule GenDurable.FSM do
  @moduledoc """
  Behaviour for a durable FSM — or, in its degenerate one-step form, a durable job.

  A module defines **either** `step/2` (a state machine) **or** `perform/1`/`perform/2`
  (a one-shot job), never both.

  ## Job form (one step)

  Define `perform/1` or `perform/2` and you get a durable job — no step names, no
  outcome tuples:

      defmodule Cleanup do
        use GenDurable.FSM

        @impl true
        def perform(args, _ctx) do
          File.rm_rf!(args["path"])
          :ok
        end
      end

      GenDurable.insert(Cleanup, args: %{"path" => "/tmp/x"})

  `perform` receives the instance args (the plain map passed as `:args`/`:state`,
  or the typed struct if a `State` schema is declared) and, in the `/2` form, the
  `t:GenDurable.Context.t/0`. It returns:

    * `:ok` / `{:ok, result_map}` — the job is `done`.
    * `{:error, reason}` — retried with `backoff/1` until `:max_attempts`, then `failed`.
    * `{:cancel, reason}` — `failed` immediately, no retry.
    * a raised exception — treated as `{:error, exception}` (routed through `handle/2`).

  `:max_attempts` (default `20`) and `backoff/1` (default a capped exponential)
  tune retries. Override `backoff/1` to change the schedule.

  ## FSM form (many steps)

      defmodule Checkout do
        use GenDurable.FSM, version: 1, queue: "checkout"

        defmodule State do
          use GenDurable.State

          embedded_schema do
            field :n, :integer, default: 0
          end
        end

        @impl true
        def step("start", %{state: s}), do: {:await, "payment_confirmed", "ship", %{s | n: s.n + 1}}
        def step("ship", ctx), do: {:done, %{"shipped" => true, "paid" => hd(ctx.awaited).payload}}

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
    * `:state`   — the `GenDurable.State` embedded-schema module. Optional: if a
      nested `State` schema module is defined inside the FSM (as above) it is picked
      up by convention, so you rarely pass this. Omit both for plain-map state.
    * `:initial` — initial step for `GenDurable.insert/2` (default `"start"`).
    * `:max_attempts` — job retry cap (default `20`). Job form only.

  For the FSM form, `handle/2` defaults to `{:stop, reason}` and is overridable.
  For the job form, both `handle/2` (retry-with-backoff) and `backoff/1` are
  generated and overridable; overriding `handle/2` drops to the FSM outcome contract.
  """

  alias GenDurable.{Context, Outcome}

  @type result :: :ok | {:ok, map()} | {:error, term()} | {:cancel, term()}

  @callback step(step :: String.t(), ctx :: Context.t()) :: Outcome.t() | term()
  @callback perform(args :: term()) :: result()
  @callback perform(args :: term(), ctx :: Context.t()) :: result()
  @callback handle(reason :: term(), ctx :: Context.t()) :: Outcome.t() | term()
  @callback backoff(attempt :: non_neg_integer()) :: non_neg_integer()

  @optional_callbacks step: 2, perform: 1, perform: 2, handle: 2, backoff: 1

  defmacro __using__(opts) do
    quote do
      @behaviour GenDurable.FSM
      @before_compile GenDurable.FSM
      @gd_opts unquote(opts)

      def __gd_name__, do: Keyword.get(@gd_opts, :name, inspect(__MODULE__))
      def __gd_version__, do: Keyword.get(@gd_opts, :version, 1)
      def __gd_queue__, do: Keyword.get(@gd_opts, :queue, "default")
      def __gd_initial__, do: Keyword.get(@gd_opts, :initial, "start")
    end
  end

  # Decide the module's shape from what it defines, and generate the rest. Runs
  # after the module body, so the nested `State` module (and the user's step/perform/
  # handle/backoff) are all visible. Everything emitted here is a compile-time
  # constant or a thin bridge — no runtime cost beyond the user's own code.
  @doc false
  defmacro __before_compile__(env) do
    mod = env.module
    opts = Module.get_attribute(mod, :gd_opts)

    has_step = Module.defines?(mod, {:step, 2})
    has_perform = Module.defines?(mod, {:perform, 2}) or Module.defines?(mod, {:perform, 1})

    cond do
      has_step and has_perform ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "#{inspect(mod)} defines both step/2 and perform/_; use exactly one " <>
              "(step/2 for a machine, perform/1|2 for a one-shot job)"

      has_step ->
        quote do
          unquote(state_def(opts, mod))
          unquote(unless_defined(mod, {:handle, 2}, fsm_handle()))
        end

      has_perform ->
        quote do
          unquote(state_def(opts, mod))
          unquote(job_step(mod, opts))
          unquote(unless_defined(mod, {:backoff, 1}, job_backoff()))
          unquote(unless_defined(mod, {:handle, 2}, job_handle(opts)))
        end

      true ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "#{inspect(mod)} must define step/2 (a machine) or perform/1|2 (a one-shot job)"
    end
  end

  # --- compile-time codegen helpers ------------------------------------------

  # Resolve the state schema: explicit `:state` wins; else a nested `State` schema.
  defp state_def(opts, mod) do
    explicit = Keyword.get(opts, :state)
    nested = Module.concat(mod, "State")

    state =
      explicit ||
        if Code.ensure_loaded?(nested) and function_exported?(nested, :__schema__, 1),
          do: nested

    quote do
      def __gd_state__, do: unquote(state)
    end
  end

  defp unless_defined(mod, fa, ast), do: unless(Module.defines?(mod, fa), do: ast)

  defp fsm_handle do
    quote do
      @impl true
      def handle(reason, _ctx), do: {:stop, reason}
    end
  end

  defp job_step(mod, opts) do
    max = Keyword.get(opts, :max_attempts, 20)

    call =
      if Module.defines?(mod, {:perform, 2}),
        do: quote(do: perform(ctx.state, ctx)),
        else: quote(do: perform(ctx.state))

    quote do
      @impl true
      def step(_step, ctx) do
        GenDurable.FSM.__outcome__(unquote(call), ctx, unquote(max), &backoff/1)
      end
    end
  end

  defp job_handle(opts) do
    max = Keyword.get(opts, :max_attempts, 20)

    quote do
      @impl true
      def handle(reason, ctx) do
        GenDurable.FSM.__retry__(reason, ctx, unquote(max), &backoff/1)
      end
    end
  end

  defp job_backoff do
    quote do
      @impl true
      def backoff(attempt), do: GenDurable.FSM.__backoff__(attempt)
    end
  end

  # --- runtime bridge (called from generated job code) -----------------------

  @doc false
  def __outcome__(result, ctx, max_attempts, backoff_fun) do
    case result do
      :ok -> {:done, %{}}
      {:ok, map} when is_map(map) -> {:done, map}
      {:error, reason} -> __retry__(reason, ctx, max_attempts, backoff_fun)
      {:cancel, reason} -> {:stop, reason}
      other -> {:stop, "perform returned an invalid value: #{inspect(other)}"}
    end
  end

  @doc false
  def __retry__(reason, ctx, max_attempts, backoff_fun) do
    if ctx.attempt + 1 < max_attempts do
      {:replay, ctx.state, backoff_fun.(ctx.attempt)}
    else
      {:stop, reason}
    end
  end

  @doc false
  def __backoff__(attempt), do: min(1000 * Integer.pow(2, attempt), 300_000)
end
