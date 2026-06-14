defmodule GenDurable.Registry do
  @moduledoc """
  Resolves `{fsm_name, fsm_version}` to an FSM module.

  Explicit by design (spec §8): old versions finish on their `fsm_version`, so
  they must remain registered as their own modules. Started with a `:fsms` list;
  each module is indexed by `__gd_name__/0` + `__gd_version__/0`. Reads go
  through a protected ETS table (no GenServer call on the hot path).
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

  @doc "Look up the module for `{name, version}`; raises `NotFound` if missing."
  def fetch!(name, version) do
    case :ets.lookup(@table, {name, version}) do
      [{_, module}] -> module
      [] -> raise NotFound, name: name, version: version
    end
  rescue
    ArgumentError ->
      # ETS table absent — registry not started.
      reraise "GenDurable.Registry is not running", __STACKTRACE__
  end
end
