defmodule GenDurable.Limiter do
  @moduledoc """
  Out-of-band admission for **configured** limits (`rate_limits:` / `concurrency_limits:`).

  Admission used to be fused into the pick's single `@pick_sql` claim (the `c_*`/`r_*`
  CTEs in `GenDurable.Queries`). Under fan-out that fused claim locks hard, and sharding
  the buckets (0.2.7) smears the contention without curing it. So admission moves out of
  the claim: the pick now (1) claims candidate rows into `executing`, then (2) asks a
  `Limiter` backend to admit them, then (3) keeps the admitted and releases the denied.
  The claim and the admit are no longer one statement — the price is a narrow
  over/under-admission window, which self-heals exactly as a crash already does (see
  `c:reconcile/1`).

  What STAYS in-band and is NOT this module's concern: K=1 mutual exclusion for
  *unconfigured* concurrency keys, enforced by the `gen_durable_concurrency_active`
  unique index. That is intrinsic to the durable row, not a configured limit.

  ## Backends

  A limiter is a `{module, handle}` pair. `module` implements this behaviour; `handle`
  is whatever connection state it needs (Postgres: `%{repo: repo}`; Redis: a Redix conn).
  Call sites use the dispatch helpers (`admit/2`, `credit/2`, …), never the backend
  module directly.

    * `GenDurable.Limiter.Postgres` — the sharded `gen_durable_buckets` table, its
      refill/debit logic, and `reconcile_concurrency`/`gc_buckets`, run as standalone
      statements instead of welded into the pick. Default; no new dependency.

    * `GenDurable.Limiter.Redis` — Lua. Rate is a token bucket; concurrency is a
      lease-scored ZSET that self-heals when a holder's lease expires.

  ## Ordering contract

  `admit/2` receives the batch already ordered by `(priority, eligible_at)`. Rate
  admission is cumulative per key (grant while the running weight fits the key's
  available tokens), so order is significant — a backend MUST honour it.
  """

  @typedoc "The configured concurrency_key of a gated entry, e.g. `\"tenant:acme\"`."
  @type conc_key :: String.t()

  @typedoc "The rate_limit key of a rate-limited entry, e.g. `\"api:stripe\"`."
  @type rate_key :: String.t()

  @typedoc """
  One claimed candidate awaiting admission.

    * `:rate` — `{rate_key, weight}` when the step is rate-limited, else `nil`.
    * `:conc` — the `conc_key` when the key is CONFIGURED (has a `conc` bucket config),
      else `nil`. Unconfigured keys are admitted in-band and never reach the Limiter.
  """
  @type entry :: %{
          id: term(),
          rate: {rate_key(), pos_integer()} | nil,
          conc: conc_key() | nil
        }

  @typedoc """
  Opaque handle a backend returns for each admitted, concurrency-gated entry. It carries
  whatever the backend needs to credit the slot back on completion (PG: `{key, shard}`;
  Redis: `{key, member}`). Threaded onto the row and handed to `credit/2` unchanged.
  """
  @type slot :: term()

  @typedoc "One configured limit, pushed to the backend by `sync_config/2`."
  @type config ::
          {:rate, name :: String.t(), rate :: number(), burst :: number(), shards :: pos_integer()}
          | {:conc, name :: String.t(), capacity :: number(), shards :: pos_integer()}

  @typedoc """
  Admission outcome.

    * `:admitted` — entries cleared to execute. Each carries its `slot` (`nil` for a
      rate-only entry, which has no slot to credit).
    * `:denied` — entry ids to release back to `runnable`; their limit is biting.
  """
  @type admission :: %{
          admitted: [{id :: term(), slot()}],
          denied: [id :: term()]
        }

  @typedoc "A backend instance: its module plus the connection state it dispatches on."
  @type t :: {module(), handle :: term()}

  @doc """
  Admit a priority-ordered batch. Debits rate tokens and takes concurrency slots for the
  entries it clears. Debit happens BEFORE the caller flips a row to `executing`, so a crash
  in the window leaks in the safe direction (under-admission), healed by `reconcile/1`.
  """
  @callback admit(handle :: term(), entries :: [entry()]) :: admission()

  @doc """
  Credit concurrency slots back — the rows are leaving `executing`. Rate has no credit
  (it refills by time). A slot whose backing counter is gone is dropped conservatively.
  """
  @callback credit(handle :: term(), slots :: [slot()]) :: :ok

  @doc """
  Extend the leases of slots still held by in-flight/buffered rows — called on the scheduler's
  heartbeat, alongside the row-lease renewal. A lease-native backend (Redis) bumps each held
  slot's expiry so a live holder is never pruned mid-step; a backend that counts holders from
  the executing-rows truth (Postgres) has nothing to renew and no-ops.
  """
  @callback renew(handle :: term(), slots :: [slot()]) :: :ok

  @doc "Upsert the configured limits. Called on startup and on config change."
  @callback sync_config(handle :: term(), configs :: [config()]) :: :ok

  @doc """
  Self-heal hook the GC calls each sweep. Repairs leaked concurrency slots from the
  executing-rows truth (`available = cap - count(executing)`); PG does the reconcile, a
  lease-native backend (Redis) may no-op since expiry returns slots for free. Returns a
  stats map merged into the GC's `[:gen_durable, :gc, :swept]` telemetry.
  """
  @callback reconcile(handle :: term()) :: %{optional(atom()) => non_neg_integer()}

  # --- dispatch --------------------------------------------------------------

  @doc "See `c:admit/2`."
  @spec admit(t(), [entry()]) :: admission()
  def admit(_limiter, []), do: %{admitted: [], denied: []}
  def admit({mod, handle}, entries), do: mod.admit(handle, entries)

  @doc "See `c:credit/2`."
  @spec credit(t(), [slot()]) :: :ok
  def credit(_limiter, []), do: :ok
  def credit({mod, handle}, slots), do: mod.credit(handle, slots)

  @doc "See `c:renew/2`."
  @spec renew(t(), [slot()]) :: :ok
  def renew(_limiter, []), do: :ok
  def renew({mod, handle}, slots), do: mod.renew(handle, slots)

  @doc "See `c:sync_config/2`."
  @spec sync_config(t(), [config()]) :: :ok
  def sync_config({mod, handle}, configs), do: mod.sync_config(handle, configs)

  @doc "See `c:reconcile/1`."
  @spec reconcile(t()) :: %{optional(atom()) => non_neg_integer()}
  def reconcile({mod, handle}), do: mod.reconcile(handle)
end
