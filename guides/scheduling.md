# Scheduling, priority & queues

## When an instance runs

By default an instance is `runnable` immediately. Delay it with any of:

```elixir
GenDurable.insert(Report, schedule_in: 60_000)                       # ms from now
GenDurable.insert(Report, schedule_at: ~U[2026-07-01 09:00:00Z])     # a DateTime
GenDurable.insert(Report, eligible_at: ~U[2026-07-01 09:00:00Z])     # explicit column value
```

Precedence is `:eligible_at` → `:schedule_at` → `:schedule_in`. Within a step, `{:retry, state,
delay_ms}` schedules the next attempt the same way (the poll/backoff primitive).

There is **no built-in cron**. Drive recurring work from your existing scheduler (a k8s
CronJob, a cloud scheduler, or any timer) by calling `insert/2` — and make it safe against
double-firing for free by giving each occurrence a time-bucketed
[`correlation_key`](identity.md): a duplicate fire is rejected as `{:error, :duplicate}`.

```elixir
GenDurable.insert(DailyReport, correlation_key: "daily-report:#{Date.utc_today()}")
```

## Priority

Lower number = earlier. Within a queue, the picker orders by `(priority, eligible_at)`.

```elixir
GenDurable.insert(Urgent, priority: 0)
GenDurable.insert(Bulk,   priority: 10)
```

## Queues

A queue is a logical pool with its own concurrency limit. Configure them at start; an instance
picks its queue from the FSM's `:queue` option or a per-instance `:queue`.

```elixir
{GenDurable, repo: MyApp.Repo, queues: [default: 10, checkout: 5, email: 50]}

GenDurable.insert(SendEmail, queue: "email")
```

`concurrency` is the maximum number of steps that queue runs at once on a node. See the
[feeder knobs](operations.md#tuning-the-feeder) for tuning prefetch and poll behaviour, and
[concurrency keys](concurrency.md) for per-key serialization within a queue.
