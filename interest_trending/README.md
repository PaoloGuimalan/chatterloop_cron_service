# interest_trending

Decays `interests_interesttrendingscore` so "trending" means recent activity
rather than lifetime accumulation.

```
docker build -t johnpauloramil187/chatterloop_interest_trending .
docker run --rm johnpauloramil187/chatterloop_interest_trending python interest_trending_cron.py --dry-run
```

## How it runs

Once, then exits — exactly like `post_scores`. No internal scheduler, no sleep
loop: the container starts, does one pass, and stops. **Docker Swarm's cron
manager re-executes it** on a schedule.

|         |                                                                                                                 |
| ------- | --------------------------------------------------------------------------------------------------------------- |
| success | exit **0**                                                                                                      |
| failure | exit **non-zero** — the exception propagates, so a failed run is visible to the cron manager rather than silent |

Nothing catches broadly at the top level, deliberately: a job that swallows its
own errors and exits 0 looks identical to one that worked.

This is also why `DECAY_INTERVAL_HOURS` exists. The decay is applied **once per
invocation**, so the script has no way to know how much elapsed time that
invocation represents unless it is told — it must match the schedule the cron
manager is configured with.

## Why

Every writer of that table is additive: `interests/services/affinity.py`, the Go
worker's `BumpInterestAffinity`, and the moderation service's interest sink all
do `score = score + delta`. Nothing had ever reduced it.

So the ranking was a lifetime total. Measured 2026-08-26, `chatterloop diary`
led at 60.0 with an `updated_at` of **2026-07-10** — six weeks stale —
outranking `motorcycle` at 25.05 which had been touched that day.

`recent_activity_boost` was worse: a multiplier that only ever climbed. 142 of
194 rows sat above its 1.0 default and `test` had reached 2.9 against a cap of
3.0. Once rows pin at the cap it stops separating anything, which is the whole
job of a multiplier.

## What it does

```
score                 = score * factor
recent_activity_boost = 1.0 + (boost - 1.0) * factor
```

The boost decays toward **1.0, its identity**, not toward zero — a quiet
interest should become neutral, not suppressed below interests nobody has ever
touched.

`updated_at` is deliberately **not** written. It records when an interest last
saw _activity_, and a decay pass is not activity; bumping it would make every
interest look freshly active.

## Configuration

|                           | default |                                    |
| ------------------------- | ------- | ---------------------------------- |
| `TRENDING_HALF_LIFE_DAYS` | 7       | how long a score takes to halve    |
| `DECAY_INTERVAL_HOURS`    | 24      | **must match the actual schedule** |
| `TRENDING_MIN_SCORE`      | 0.01    | below this, snapped to 0           |

The half-life is a **product decision**: how fast should yesterday's topic stop
being trending? Seven days halves a burst in a week and leaves ~5% after a
month.

`DECAY_INTERVAL_HOURS` must match whatever invokes the container, because the
decay is applied **per run** — a job that claims 24h but fires hourly decays 24×
too fast. The effective factor is logged on every run so a mismatch is visible.

### Why per-run and not from `updated_at`

Decaying each row by its own age looks right and is wrong: a bump resets
`updated_at`, so an actively-bumped interest would never decay and would keep
its full historical accumulation forever — precisely the runaway this fixes.
Decay must apply to everything per unit of wall-clock time.

## What decay does and does not do

Uniform multiplicative decay **preserves relative order on its own** — every
score shrinks by the same factor. It works in combination with ongoing bumps:
fresh activity accrues while old accumulation shrinks.

Simulated against the real numbers, a stale 60.0 receiving nothing versus a
fresh 25.05 receiving +3.0/day:

- **with decay** — the fresh interest overtakes on **day 8**, and the stale one
  keeps falling
- **without decay** — it overtakes on day 12, and the stale one _never falls_

An interest bumped +3.0/day converges to a steady state of ~31.8 rather than
growing without bound. That ceiling is the point.
