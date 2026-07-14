defmodule GenDurable.MixProject do
  use Mix.Project

  @source_url "https://github.com/Deepwalker/gen_durable"

  def project do
    [
      app: :gen_durable,
      version: "0.2.9",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "gen_durable",
      description:
        "Postgres-backed durable execution for Elixir: declare an FSM, the engine " <>
          "commits its state before each step proceeds, so instances survive process " <>
          "and node death and resume where they left off.",
      source_url: @source_url,
      docs: docs(),
      package: package()
    ]
  end

  defp package do
    [
      licenses: ["BSD-3-Clause"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib guides mix.exs README.md LICENSE CHANGELOG.md PERFORMANCE.md .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "guides/jobs.md": [title: "Jobs"],
        "guides/machines.md": [title: "State machines"],
        "guides/signals.md": [title: "Signals & await"],
        "guides/children.md": [title: "Child fan-out"],
        "guides/rate_limiting.md": [title: "Rate limiting"],
        "guides/concurrency.md": [title: "Concurrency keys"],
        "guides/identity.md": [title: "Instance identity"],
        "guides/scheduling.md": [title: "Scheduling & queues"],
        "guides/testing.md": [title: "Testing"],
        "guides/operations.md": [title: "Operations"],
        "guides/internals.md": [title: "Database internals"],
        "PERFORMANCE.md": [title: "Performance"],
        "CHANGELOG.md": [title: "Changelog"]
      ],
      groups_for_extras: [
        Guides: ~r{guides/.*}
      ],
      groups_for_modules: [
        "Public API": [
          GenDurable,
          GenDurable.FSM,
          GenDurable.State,
          GenDurable.Migration,
          GenDurable.Testing
        ],
        Runtime: [
          GenDurable.Supervisor,
          GenDurable.Scheduler,
          GenDurable.Poke,
          GenDurable.Await,
          GenDurable.Reaper,
          GenDurable.GC,
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
      # only for the poke: {:redis, _} transport — see GenDurable.Poke
      {:redix, "~> 1.2", optional: true},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
