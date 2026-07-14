defmodule GenDurable.Testing do
  @moduledoc """
  Inline execution and assertions for tests.

  No engine, no background processes: `drain/1` synchronously runs everything
  runnable **in the calling process**, through the production pick/executor
  path — so a test inserts, drains, delivers a signal, drains again, and
  asserts. Because every query runs in the test process, it composes with
  `Ecto.Adapters.SQL.Sandbox` ownership out of the box.

      defmodule MyApp.CheckoutTest do
        use ExUnit.Case, async: true
        use GenDurable.Testing, repo: MyApp.Repo

        test "checkout waits for payment, then ships" do
          {:ok, id} = GenDurable.insert(Checkout, state: %{order: 42}, repo: MyApp.Repo)

          assert %{await: 1} = drain()
          assert_awaiting(id, "payment_confirmed")

          :ok = GenDurable.signal(id, "payment_confirmed", %{amount: 100}, repo: MyApp.Repo)

          assert %{done: 1} = drain()
          assert_done(id, %{"shipped" => true})
        end
      end

  `use GenDurable.Testing, repo: MyRepo` injects repo-bound wrappers for every
  helper below; without `use`, call them with the repo as the first argument.
  FSMs with a custom `:name` (or pinned old versions) need registering, exactly
  as with the engine's `:fsms` option: `use GenDurable.Testing, repo: MyRepo,
  fsms: [My.CustomNamed]` (or per call, `drain(fsms: [...])`).

  Inline semantics vs the engine:

    * steps run one at a time, in pick order — `concurrency_key` serialization
      is trivially satisfied;
    * scheduled/backoff delays are collapsed by default (`with_scheduled: true`)
      so retries and `schedule_in` run immediately — an FSM that retries forever
      hits the `max_steps` cap and raises instead of hanging the test;
    * a step that raises routes to `handle/2` exactly as in production; a bare
      `exit` crashes the test process (there is no reaper to catch it);
    * await timeouts do not fire by themselves (that is the reaper's tick) —
      `fire_timeouts/1` force-fires every armed deadline.
  """

  import ExUnit.Assertions

  alias GenDurable.{Executor, Outcome, Queries, Registry}

  @statuses %{
    "runnable" => :runnable,
    "executing" => :executing,
    "awaiting_signal" => :awaiting_signal,
    "awaiting_children" => :awaiting_children,
    "done" => :done,
    "failed" => :failed
  }

  defmacro __using__(opts) do
    repo = Keyword.fetch!(opts, :repo)
    fsms = Keyword.get(opts, :fsms, [])

    quote do
      def drain(opts \\ []) do
        opts
        |> Keyword.put_new(:repo, unquote(repo))
        |> Keyword.put_new(:fsms, unquote(fsms))
        |> GenDurable.Testing.drain()
      end

      def fire_timeouts, do: GenDurable.Testing.fire_timeouts(unquote(repo))

      def durable(target), do: GenDurable.Testing.durable(unquote(repo), target)

      def assert_status(target, expected),
        do: GenDurable.Testing.assert_status(unquote(repo), target, expected)

      def assert_awaiting(target, names \\ nil),
        do: GenDurable.Testing.assert_awaiting(unquote(repo), target, names)

      def assert_done(target, result \\ :any),
        do: GenDurable.Testing.assert_done(unquote(repo), target, result)

      def assert_failed(target, error \\ :any),
        do: GenDurable.Testing.assert_failed(unquote(repo), target, error)
    end
  end

  @doc """
  Synchronously execute runnable instances until quiescence (nothing left that
  can run) and return a tally of the outcomes committed:

      %{steps: 5, next: 3, retry: 0, await: 1, schedule_childs: 0, done: 1, stop: 0}

  Options:

    * `:repo` — required (injected by `use`);
    * `:queue` — drain one queue only (default: all queues);
    * `:with_scheduled` — collapse future `eligible_at` (scheduled work, retry
      backoffs) so it runs immediately (default `true`);
    * `:fsms` — modules to register explicitly, as the engine's `:fsms` option
      (only for custom `:name`s/versions — module-named FSMs resolve dynamically);
    * `:max_steps` — safety cap, raises when exceeded (default `1_000`): an FSM
      that retries or transitions forever fails the test instead of hanging it.
  """
  def drain(opts) do
    repo = Keyword.fetch!(opts, :repo)

    config = %{
      name: __MODULE__,
      repo: repo,
      limiter: {GenDurable.Limiter.Postgres, %{repo: repo}},
      registry: Registry.new(Keyword.get(opts, :fsms, [])),
      rate_limit_names: configured_rate_limits(repo)
    }

    loop(
      config,
      "inline-#{System.unique_integer([:positive])}",
      opts[:queue] && to_string(opts[:queue]),
      Keyword.get(opts, :with_scheduled, true),
      Keyword.get(opts, :max_steps, 1_000),
      %{steps: 0, next: 0, retry: 0, await: 0, schedule_childs: 0, done: 0, stop: 0}
    )
  end

  @doc """
  Force-fire every armed await deadline: parked instances with a `timeout:` go
  straight back to runnable (as if the deadline had passed and the reaper
  swept), regardless of how much time is actually left. Follow with `drain/1`.
  """
  def fire_timeouts(repo) do
    %{num_rows: n} =
      repo.query!("""
      UPDATE gen_durable
      SET status = 'runnable', eligible_at = now(), await_deadline = null, updated_at = now()
      WHERE status = 'awaiting_signal' AND await_deadline IS NOT NULL
      """)

    n
  end

  @doc """
  Fetch an instance for arbitrary asserts: a map with `:id`, `:fsm`, `:step`,
  `:status` (atom), `:state`, `:result`, `:last_error`, `:attempt`, `:awaits`,
  `:queue` — or `nil`. `target` is the internal id, or a correlation key (the
  occupied instance; falls back to the **latest** row carrying the key, so a
  finished instance is still assertable by key).

  Test-only introspection: the public API deliberately does not expose
  `state`/`result` (they are GC-bounded); in a test you own the lifecycle.
  """
  def durable(repo, target) do
    {where, param} =
      cond do
        is_integer(target) -> {"id = $1", target}
        is_binary(target) -> {"correlation_key = $1 ORDER BY id DESC", target}
      end

    %{rows: rows} =
      repo.query!(
        """
        SELECT id, fsm, step, status::text, state, result, last_error, attempt, awaits, queue
        FROM gen_durable WHERE #{where} LIMIT 1
        """,
        [param]
      )

    case rows do
      [] ->
        nil

      [[id, fsm, step, status, state, result, last_error, attempt, awaits, queue]] ->
        %{
          id: id,
          fsm: fsm,
          step: step,
          status: Map.fetch!(@statuses, status),
          state: decode(state),
          result: result && decode(result),
          last_error: last_error,
          attempt: attempt,
          awaits: awaits,
          queue: queue
        }
    end
  end

  @doc "Assert the instance exists and has `expected` status. Returns the row map."
  def assert_status(repo, target, expected) do
    row = durable(repo, target) || flunk("no gen_durable instance for #{inspect(target)}")

    assert row.status == expected,
           "expected #{inspect(target)} to be #{inspect(expected)}, got: #{inspect(row.status)} " <>
             "(step #{inspect(row.step)}, attempt #{row.attempt}" <>
             if(row.last_error, do: ", last_error: #{inspect(row.last_error)})", else: ")")

    row
  end

  @doc """
  Assert the instance is parked on a signal await — optionally on the given
  name(s) (a subset check against its `awaits`). Returns the row map.
  """
  def assert_awaiting(repo, target, names \\ nil) do
    row = assert_status(repo, target, :awaiting_signal)

    if names do
      wanted = names |> List.wrap() |> Enum.map(&to_string/1)

      assert Enum.all?(wanted, &(&1 in row.awaits)),
             "expected #{inspect(target)} to await #{inspect(wanted)}, " <>
               "but it awaits #{inspect(row.awaits)}"
    end

    row
  end

  @doc "Assert the instance is `:done` — and, unless `:any`, that its result equals `result`. Returns the result."
  def assert_done(repo, target, result \\ :any) do
    row = assert_status(repo, target, :done)

    unless result == :any do
      assert row.result == result
    end

    row.result
  end

  @doc "Assert the instance is `:failed` — and, unless `:any`, that `last_error` matches (`==` for strings, `=~` for regexes). Returns `last_error`."
  def assert_failed(repo, target, error \\ :any) do
    row = assert_status(repo, target, :failed)

    case error do
      :any -> :ok
      %Regex{} -> assert row.last_error =~ error
      _ -> assert row.last_error == error
    end

    row.last_error
  end

  # --- the inline loop ---------------------------------------------------------

  defp loop(config, worker, queue, with_scheduled, budget, acc) do
    if with_scheduled, do: promote_scheduled(config.repo, queue)

    jobs =
      due_queues(config.repo, queue)
      |> Enum.flat_map(&Queries.pick(config.repo, &1, 100, worker, 60_000, config.limiter))

    cond do
      jobs == [] ->
        acc

      acc.steps + length(jobs) > budget ->
        raise "GenDurable.Testing.drain/1 exceeded max_steps=#{budget} — " <>
                "an FSM is likely retrying or transitioning forever"

      true ->
        acc =
          Enum.reduce(jobs, acc, fn job, acc ->
            kind = Outcome.kind(Executor.run(config, job))
            acc |> Map.update!(:steps, &(&1 + 1)) |> Map.update!(kind, &(&1 + 1))
          end)

        loop(config, worker, queue, with_scheduled, budget, acc)
    end
  end

  # Collapse future eligible_at so scheduled work and retry backoffs run now.
  defp promote_scheduled(repo, queue) do
    repo.query!(
      """
      UPDATE gen_durable SET eligible_at = now()
      WHERE status = 'runnable' AND eligible_at > now() AND ($1::text IS NULL OR queue = $1)
      """,
      [queue]
    )
  end

  defp due_queues(repo, nil) do
    %{rows: rows} =
      repo.query!(
        "SELECT DISTINCT queue FROM gen_durable WHERE status = 'runnable' AND eligible_at <= now()"
      )

    List.flatten(rows)
  end

  defp due_queues(_repo, queue), do: [queue]

  defp configured_rate_limits(repo) do
    %{rows: rows} = repo.query!("SELECT name FROM gen_durable_bucket_configs WHERE kind = 'rate'")
    MapSet.new(List.flatten(rows))
  end

  # jsonb objects arrive as maps; scalar-string rows (the pre-0.2.0
  # double-encoded format) as binaries.
  defp decode(value) when is_binary(value), do: Jason.decode!(value)
  defp decode(value), do: value
end
