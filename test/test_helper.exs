alias GenDurable.Test.Repo

# Start each test run from a pristine database.
_ = Repo.__adapter__().storage_down(Repo.config())

case Repo.__adapter__().storage_up(Repo.config()) do
  :ok -> :ok
  {:error, :already_up} -> :ok
  other -> raise "could not create test database: #{inspect(other)}"
end

{:ok, _} = Repo.start_link()

Ecto.Migrator.run(Repo, [{1, GenDurable.Test.Migrations.Setup}], :up, all: true)

ExUnit.start(exclude: [:bench])
