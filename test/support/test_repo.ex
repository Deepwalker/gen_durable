defmodule GenDurable.Test.Repo do
  use Ecto.Repo, otp_app: :gen_durable, adapter: Ecto.Adapters.Postgres
end
