defmodule GenDurable.Registry do
  @moduledoc """
  Resolves `{fsm_name, fsm_version}` to an FSM module.

  Two resolution paths:

    * **Explicit registry** — modules passed in `:fsms` are indexed by
      `__gd_name__/0` + `__gd_version__/0` in a protected ETS table. Use this when
      a machine has a custom `:name` (the `fsm` column is then not a module name)
      or to keep old `:version`s running (spec §8): old instances finish on their
      `fsm_version`, so the old module must stay registered.
    * **Dynamic fallback** — on a miss, the `fsm` name is interpreted as a module
      name (its default is `inspect(module)`) and accepted if that module is a
      `GenDurable.FSM` whose own name and version match the row. So a machine with
      the default name and a single version needs **no** `:fsms` entry at all.

  Reads are an ETS lookup plus, only on a miss, a cheap module check — no
  GenServer call on the hot path.
  """

  use GenServer

  @table __MODULE__

  defmodule NotFound do
    defexception [:name, :version]

    @impl true
    def message(%{name: name, version: version}),
      do: "no FSM registered for #{inspect(name)} v#{version}"
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    table = :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])

    for mod <- Keyword.get(opts, :fsms, []) do
      :ets.insert(table, {{mod.__gd_name__(), mod.__gd_version__()}, mod})
    end

    {:ok, table}
  end

  @doc """
  Look up the module for `{name, version}` — the explicit registry first, then a
  dynamic resolution of `name` as a module. Raises `NotFound` if neither matches.
  """
  def fetch!(name, version) do
    case :ets.lookup(@table, {name, version}) do
      [{_, module}] -> module
      [] -> resolve(name, version) || raise NotFound, name: name, version: version
    end
  rescue
    ArgumentError ->
      # ETS table absent — registry not started.
      reraise "GenDurable.Registry is not running", __STACKTRACE__
  end

  # Treat `name` as a module name (the default `__gd_name__`), accept it only if it
  # is an FSM whose own name and version match — so we never run an arbitrary or
  # wrong-version module. A custom `:name` or an old `:version` won't match here and
  # must be registered explicitly.
  defp resolve(name, version) when is_binary(name) do
    module = String.to_existing_atom("Elixir." <> name)

    if Code.ensure_loaded?(module) and function_exported?(module, :__gd_name__, 0) and
         module.__gd_name__() == name and module.__gd_version__() == version do
      module
    end
  rescue
    ArgumentError -> nil
  end

  defp resolve(_name, _version), do: nil
end
