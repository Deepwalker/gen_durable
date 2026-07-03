# Rate limiting

Rate limiting throttles a **specific step** to N starts per period — typically "don't exceed an
external quota" (≤100 Stripe calls/second, ≤5 emails/minute per user). It is distinct from
[concurrency](concurrency.md): concurrency bounds how many run *at once*, rate bounds how many
*start per unit time*.

The limit attaches to a step, not a queue — a queue holds many machines with many steps, and
the limited resource is touched by one specific step.

## Configure named limits

Engine-start option:

```elixir
{GenDurable,
 repo: MyApp.Repo,
 rate_limits: [
   stripe: [allowed: 100, period: {1, :minute}],
   emails: [allowed: 5, period: 60, burst: 10]
 ]}
```

It is a **token bucket**: `allowed`/`period` set the sustained rate, `burst` (default `allowed`)
the instantaneous slack. `period` is seconds or `{n, :second | :minute | :hour | :day}`.

## Opt a step in

A step declares the limit for its **next** step (it cannot gate its own execution — by the time
it runs, the API call has already happened):

```elixir
def step("prepare", ctx), do: {:next, "charge", ctx.state, rate_limit: :stripe}
def step("charge",  ctx), do: # ≤ the "stripe" budget; makes the API call
```

`rate_limit:` is a configured **name** (one global bucket), or `{name, partition}` for a bucket
**per partition** — same policy, separate budget per key:

```elixir
# ≤ "stripe" rate globally:
{:next, "charge", state, rate_limit: :stripe}

# ≤ "stripe" rate per tenant (each tenant its own bucket):
{:next, "charge", state, rate_limit: {:stripe, tenant_id}}
```

`insert/2` accepts the same `:rate_limit` when the **first** step is limited. The key is kept
across `:retry` (a limited step that retries is still limited) and cleared on any other
transition. A step with no `rate_limit` is the common case and costs nothing.

## Weights

By default each step execution consumes one token. A step that does several units of the
limited work at once (e.g. N API calls in a loop) can consume more:

```elixir
{:next, "bulk_charge", state, rate_limit: :stripe, weight: 50}
```

Grants take the most-urgent prefix whose **cumulative weight** fits the available budget (strict
priority order; a fat step that doesn't fit waits until enough tokens accumulate, without
starving).

> **`weight ≤ burst` is your responsibility — it is not validated.** A step whose weight exceeds
> the bucket capacity can never run *and freezes the whole bucket behind it.* The cure for a
> too-fat step is to **split it**: N units of limited work = N steps (or a
> [`schedule_childs`](children.md) fan-out) of `weight 1` — which removes the freeze risk
> entirely. Weights exist only for genuinely unsplittable chunky steps.

## Semantics

- **At-least-once accounting.** A token is taken when the step is *claimed*; there is no refund.
  A crash that re-runs the step takes another token (every execution counts).
- **Unknown key.** A `rate_limit` whose name has no configured policy makes the row **stall** (no
  bucket) and emits `[:gen_durable, :rate_limit, :unknown]`. Keep your keys configured.
- **Cross-node correctness.** The bucket is a single Postgres counter row; concurrent pickers
  across nodes serialize on it. A rate-limited bucket is low-throughput by definition, so this is
  never the bottleneck — see the [performance notes](../PERFORMANCE.md).

## Telemetry

- `[:gen_durable, :rate_limit, :throttled]` — a bucket granted fewer rows than wanted in a pick.
  Measurements `%{wanted, granted}`; metadata `%{key, queue}`. The signal that a limit is biting.
- `[:gen_durable, :rate_limit, :unknown]` — a step named an unconfigured limit. Metadata
  `%{key, name, fsm, step}`.
