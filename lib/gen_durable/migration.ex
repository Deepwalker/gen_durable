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
      The runtime queries reference the tables **unqualified**, so a non-default
      prefix also requires the schema on the repo's `search_path`
      (e.g. `after_connect: {Postgrex, :query!, ["SET search_path TO my_schema, public", []]}`).
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

  # --- version 1: DDL ------------------------------------------------

  defp change(1, :up, p) do
    execute("""
    CREATE TYPE #{p}.durable_status AS ENUM
      ('runnable', 'executing', 'awaiting_signal', 'awaiting_children', 'done', 'failed')
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
      awaits        text[],

      queue         text     not null default 'default',
      priority      smallint not null default 0,
      concurrency_key text,
      eligible_at   timestamptz not null default now(),
      attempt       int  not null default 0,
      last_error    text,

      -- await timeout: set by the park when the step passed `timeout:`; the
      -- reaper sweeps expired parks back to runnable (a wake, not a failure).
      await_deadline timestamptz,

      -- rate limiting: rate_limit is the bucket key for the CURRENT step
      -- (NULL ⇒ not rate-limited), weight is how many budget units this step's execution
      -- consumes (default 1). Both rewritten on every transition; kept on :retry.
      rate_limit    text,
      weight        double precision not null default 1,

      locked_by        text,
      lease_expires_at timestamptz,

      parent_id        bigint references #{p}.gen_durable(id) on delete set null,
      children_pending int not null default 0,

      -- correlation_key: the instance's business identity — both the signal address
      -- and the uniqueness guard. correlation_scope is the set of statuses in
      -- which the key is "occupied", supplied by the caller (defaults to the non-terminal
      -- statuses ⇒ unique among live, freed on termination). The guard equals the key
      -- while the status is occupied, else NULL (drops out of the unique/address index).
      -- scope is durable_status[] (not text[]) so the generated column stays IMMUTABLE
      -- — the enum->text cast (enum_out) is only STABLE.
      correlation_key   text,
      correlation_scope #{p}.durable_status[] not null default '{}',
      correlation_guard text generated always as (
        case when correlation_key is not null and status = any(correlation_scope)
             then correlation_key end
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

    # Supports the await-timeout sweep: parked rows with an armed deadline.
    execute("""
    CREATE INDEX gen_durable_await_deadline ON #{p}.gen_durable (await_deadline)
      WHERE status = 'awaiting_signal' AND await_deadline IS NOT NULL
    """)

    # concurrency_key serialization, enforced by the database: at most ONE
    # executing row per key can ever be committed. The picker's NOT EXISTS guard
    # reads it as an optimization; the UNIQUE arbiter is the correctness — a
    # cross-node claim race ends in a unique violation and the losing pick
    # retries. The "lock" is the row's own executing status, so it spans exactly
    # the step window and releases with any outcome (or the reaper, on a crash).
    # Scoped to non-null keys: a non-keyed claim (the common case) never writes
    # to this index.
    execute("""
    CREATE UNIQUE INDEX gen_durable_concurrency_active ON #{p}.gen_durable (concurrency_key)
      WHERE status = 'executing' AND concurrency_key IS NOT NULL
    """)

    # correlation_key: one partial unique index does double duty — it enforces
    # uniqueness among "occupied" statuses (per the :unique policy / scope) AND backs
    # the address lookup in deliver_signal (`correlation_guard = $1` resolves to the
    # single occupied instance). A row whose status leaves the scope drops out, freeing the key.
    execute("""
    CREATE UNIQUE INDEX gen_durable_correlation ON #{p}.gen_durable (correlation_guard)
      WHERE correlation_guard IS NOT NULL
    """)

    execute("""
    CREATE INDEX gen_durable_parent ON #{p}.gen_durable (parent_id)
      WHERE parent_id IS NOT NULL
    """)

    # Supports the GC sweep: terminal rows ordered by termination instant.
    execute("""
    CREATE INDEX gen_durable_gc ON #{p}.gen_durable (updated_at)
      WHERE status IN ('done', 'failed')
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

    # rate limiting. Configs are seeded at engine start from the `rate_limits:`
    # option; the picker joins them. Buckets are token-bucket counters, one row per distinct
    # key, ensured (full) by the transition that assigns the key.
    execute("""
    CREATE TABLE #{p}.gen_durable_rate_configs (
      name  text primary key,
      rate  double precision not null,   -- tokens per second (allowed / period)
      burst double precision not null    -- bucket capacity
    )
    """)

    execute("""
    CREATE TABLE #{p}.gen_durable_rate_buckets (
      key         text primary key,
      tokens      double precision not null,
      last_refill timestamptz not null default clock_timestamp()
    )
    """)
  end

  defp change(1, :down, p) do
    execute("DROP TABLE IF EXISTS #{p}.gen_durable_rate_buckets")
    execute("DROP TABLE IF EXISTS #{p}.gen_durable_rate_configs")
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
