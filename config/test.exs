import Config

config :gen_durable, ecto_repos: [GenDurable.Test.Repo]

config :gen_durable, GenDurable.Test.Repo,
  username: System.get_env("POSTGRES_USER", "postgres"),
  password: System.get_env("POSTGRES_PASSWORD", "postgres"),
  hostname: System.get_env("POSTGRES_HOST", "localhost"),
  database: System.get_env("POSTGRES_DB", "gen_durable_test"),
  pool_size: 10,
  log: false
