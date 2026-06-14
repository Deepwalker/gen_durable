defmodule GenDurable.Migration do
  @moduledoc """
  Library-owned schema migration (Oban-style).

  The host application does not copy the DDL; it writes a one-line Ecto
  migration that delegates here:

      defmodule MyApp.Repo.Migrations.SetupGenDurable do
        use Ecto.Migration

        def up,   do: GenDurable.Migration.up()
        def down, do: GenDurable.Migration.down()
      end

  ## Options

    * `:prefix`  — Postgres schema the tables live in (default `"public"`).
    * `:version` — schema version to migrate to (default: latest for `up/1`,
      `0` for `down/1`).

  The installed schema version is recorded in `COMMENT ON TABLE gen_durable`,
  so `up/1` only applies the increments that are missing. This keeps the
  host-facing call stable as the schema evolves across library releases.
  """

  use Ecto.Migration

  @latest_version 1

  @doc "Migrate the schema up to `:version` (default: latest)."
  def up(opts \\ []) do
    prefix = prefix(opts)
    target = Keyword.get(opts, :version, @latest_version)
    current = migrated_version(prefix)

    if prefix != "public" do
      execute("CREATE SCHEMA IF NOT EXISTS #{prefix}")
    end

    if current < target do
      for v <- (current + 1)..target, do: change(v, :up, prefix)
      record_version(prefix, target)
    end

    :ok
  end

  @doc "Migrate the schema down to `:version` (default: 0, i.e. drop everything)."
  def down(opts \\ []) do
    prefix = prefix(opts)
    target = Keyword.get(opts, :version, 0)
    current = migrated_version(prefix)

    if current > target do
      for v <- current..(target + 1)//-1, do: change(v, :down, prefix)
      if target > 0, do: record_version(prefix, target)
    end

    :ok
  end

  # --- version 1: spec §9 DDL ------------------------------------------------

  defp change(1, :up, p) do
    execute("""
    CREATE TYPE #{p}.durable_status AS ENUM
      ('runnable', 'executing', 'awaiting_signal', 'done', 'failed')
    """)

    execute("""
    CREATE TABLE #{p}.gen_durable (
      id            bigint generated always as identity primary key,
      fsm           text not null,
      fsm_version   int  not null default 1,
      step          text not null,
      status        #{p}.durable_status not null default 'runnable',
      state         jsonb not null default '{}',
      result        jsonb,
      awaits        text,

      queue         text     not null default 'default',
      priority      smallint not null default 0,
      partition_key text,
      eligible_at   timestamptz not null default now(),
      attempt       int  not null default 0,
      last_error    text,

      locked_by        text,
      lease_expires_at timestamptz,

      unique_key    bytea,
      unique_scope  #{p}.durable_status[] not null default '{}',
      unique_guard  bytea generated always as (
        case when unique_key is not null and status = any(unique_scope)
             then unique_key end
      ) stored,

      inserted_at   timestamptz not null default now(),
      updated_at    timestamptz not null default now()
    )
    """)

    execute("""
    CREATE INDEX gen_durable_pick ON #{p}.gen_durable (queue, priority, eligible_at)
      WHERE status = 'runnable'
    """)

    execute("""
    CREATE INDEX gen_durable_lease ON #{p}.gen_durable (lease_expires_at)
      WHERE status = 'executing'
    """)

    execute("""
    CREATE UNIQUE INDEX gen_durable_unique ON #{p}.gen_durable (unique_guard)
      WHERE unique_guard IS NOT NULL
    """)

    execute("""
    CREATE TABLE #{p}.signals (
      id          bigint generated always as identity primary key,
      target_id   bigint not null references #{p}.gen_durable(id) on delete cascade,
      name        text   not null,
      payload     jsonb  not null default '{}',
      dedup_key   text,
      inserted_at timestamptz not null default now(),
      unique (target_id, dedup_key)
    )
    """)

    execute("CREATE INDEX signals_target ON #{p}.signals (target_id, name)")
  end

  defp change(1, :down, p) do
    execute("DROP TABLE IF EXISTS #{p}.signals")
    execute("DROP TABLE IF EXISTS #{p}.gen_durable")
    execute("DROP TYPE IF EXISTS #{p}.durable_status")
  end

  # --- helpers ---------------------------------------------------------------

  defp prefix(opts), do: Keyword.get(opts, :prefix, "public")

  defp record_version(p, version) do
    execute("COMMENT ON TABLE #{p}.gen_durable IS '#{version}'")
  end

  # Current installed version from the table comment; 0 if the table is absent.
  defp migrated_version(p) do
    query = """
    SELECT pg_catalog.obj_description(c.oid, 'pg_class')
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'gen_durable' AND n.nspname = $1
    """

    case repo().query(query, [p]) do
      {:ok, %{rows: [[version]]}} when is_binary(version) -> String.to_integer(version)
      _ -> 0
    end
  end
end
