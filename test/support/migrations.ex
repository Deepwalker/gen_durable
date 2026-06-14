defmodule GenDurable.Test.Migrations.Setup do
  use Ecto.Migration

  def up, do: GenDurable.Migration.up()
  def down, do: GenDurable.Migration.down()
end
