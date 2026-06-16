defmodule GenDurable.MixProject do
  use Mix.Project

  @source_url "https://github.com/Deepwalker/gen_durable"

  def project do
    [
      app: :gen_durable,
      version: "0.1.4",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "gen_durable",
      description: "Postgres-backed durable FSM engine on top of GenServer.",
      source_url: @source_url,
      docs: docs(),
      package: package()
    ]
  end

  defp package do
    [
      licenses: ["BSD-3-Clause"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE PERFORMANCE.md .formatter.exs
                gen_durable_spec.md gen_durable_plan.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "PERFORMANCE.md": [title: "Performance"],
        "gen_durable_spec.md": [title: "Specification"],
        "gen_durable_plan.md": [title: "Implementation plan"]
      ],
      groups_for_modules: [
        "Public API": [GenDurable, GenDurable.FSM, GenDurable.State, GenDurable.Migration],
        Runtime: [
          GenDurable.Supervisor,
          GenDurable.Scheduler,
          GenDurable.Reaper,
          GenDurable.Registry,
          GenDurable.Executor
        ],
        Internals: [GenDurable.Queries, GenDurable.Outcome, GenDurable.Context]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ecto_sql, "~> 3.12"},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
