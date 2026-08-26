#!/usr/bin/env python3
"""
Decay interests_interesttrendingscore.

WHY THIS EXISTS
---------------
Every writer of that table is purely additive - interests/services/affinity.py,
the Go worker's BumpInterestAffinity, and the moderation service's interest
sink all do `score = score + delta`. Nothing has ever reduced it.

So "trending" was a LIFETIME TOTAL, not a trend. Measured 2026-08-26:
"chatterloop diary" sat top at 60.0 with an updated_at of 2026-07-10 - six
weeks stale - outranking "motorcycle" at 25.05 which had been touched that day.
An interest that was busy once outranked one that is busy now, permanently.

`recent_activity_boost` has the same problem and is worse for it: it is a
MULTIPLIER that only ever climbed. 142 of 194 rows were already above its 1.0
default and "test" had reached 2.9 against a cap of 3.0. Once several rows pin
at the cap it stops separating them, which is the entire job of a multiplier.

WHAT IT DOES
------------
Applies exponential decay per run:

    score = score * factor
    recent_activity_boost = 1.0 + (recent_activity_boost - 1.0) * factor

The boost decays toward 1.0, its IDENTITY, not toward zero. A quiet interest
should become neutral, not suppressed - multiplying a quiet interest's score by
something under 1.0 would push it below interests nothing has ever touched.

WHY A PER-RUN FACTOR, NOT AN AGE CALCULATION
--------------------------------------------
The obvious approach - decay each row by its own `updated_at` age - is wrong
here. A bump resets updated_at, so an actively-bumped interest would never
decay at all and would keep its entire historical accumulation forever, which
is precisely the runaway this fixes. Decay has to apply to everything per unit
of wall-clock time, so the factor comes from the SCHEDULE.

That means DECAY_INTERVAL_HOURS must match what actually invokes this. The
effective factor and implied half-life are logged on every run so a mismatch is
visible rather than silent.

HOW IT IS RUN
-------------
Once, then exit - exactly like post_scores_cron.py. There is no internal
scheduler and no sleep loop: the container starts, does one pass, and stops.
Docker Swarm's cron manager is what re-executes it on a schedule.

That is also why DECAY_INTERVAL_HOURS exists and must match the schedule the
cron manager is configured with. The decay is applied ONCE PER INVOCATION, so
the script has no way to know how much time it represents unless it is told.

Exit code 0 on success. An unhandled failure propagates and exits non-zero,
which is what lets the cron manager see a failed run rather than a silent one -
so nothing here catches broadly at the top level.
"""

import argparse
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()
os.environ["PYTHONUNBUFFERED"] = "1"


def setup_logging():
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    return logging.getLogger(__name__)


logger = setup_logging()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "connect_timeout": 30,
    "sslmode": "require",
    "application_name": "interest-trending-decay",
}

# How long a score takes to halve. This is a PRODUCT decision - how fast should
# yesterday's topic stop being "trending"? Seven days means a burst is mostly
# gone within a month and halved within a week, which is what most people mean
# by trending on a social platform. Raise it for a slower, more archival
# ranking; lower it to make the list twitchier.
HALF_LIFE_DAYS = float(os.getenv("TRENDING_HALF_LIFE_DAYS", "7"))

# MUST match the schedule that actually invokes this container. The decay is
# per-run, so a job that claims 24h but really fires hourly decays 24x too fast.
INTERVAL_HOURS = float(os.getenv("DECAY_INTERVAL_HOURS", "24"))

# Below this a score is snapped to zero. Not cosmetic: without it, rows decay
# into ever-smaller floats forever and every run rewrites all of them.
MIN_SCORE = float(os.getenv("TRENDING_MIN_SCORE", "0.01"))
# The boost's identity. Within this of 1.0 it is snapped, for the same reason.
BOOST_EPSILON = 0.001


@contextmanager
def get_db_transaction():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"🚨 ROLLBACK: {e}")
        raise
    finally:
        if conn:
            conn.close()


def decay_factor() -> float:
    """The multiplier applied once per run.

    0.5 ** (elapsed / half_life) - so after exactly one half-life's worth of
    runs, a score that received no bumps has halved.
    """
    if HALF_LIFE_DAYS <= 0:
        return 1.0
    return 0.5 ** ((INTERVAL_HOURS / 24.0) / HALF_LIFE_DAYS)


def preview(cur, factor: float) -> None:
    """What the run will do to the top of the ranking, before it does it."""
    cur.execute(
        """
        SELECT i.name, t.score, t.recent_activity_boost
        FROM interests_interesttrendingscore t
        JOIN interests_interest i ON i.id = t.interest_id
        ORDER BY t.score DESC
        LIMIT 8
        """
    )
    logger.info("   %-24s %10s %10s   %8s %8s", "interest", "score", "→", "boost", "→")
    for row in cur.fetchall():
        score = float(row["score"])
        boost = float(row["recent_activity_boost"])
        logger.info(
            "   %-24s %10.3f %10.3f   %8.3f %8.3f",
            row["name"][:24],
            score,
            score * factor,
            boost,
            1.0 + (boost - 1.0) * factor,
        )


def decay_trending_scores(dry_run: bool = False) -> int:
    factor = decay_factor()

    logger.info(
        f"⚙️  half-life {HALF_LIFE_DAYS}d, interval {INTERVAL_HOURS}h "
        f"→ factor {factor:.6f} per run"
    )
    if factor >= 1.0:
        logger.warning("⚠️  factor >= 1.0 - nothing will decay. Check the config.")
        return 0

    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS c FROM interests_interesttrendingscore")
            total = cur.fetchone()["c"]
            logger.info(f"📊 {total} interests to decay")

            preview(cur, factor)

            if dry_run:
                logger.info("🧪 DRY RUN - nothing written")
                return 0

            # One statement, not a read-modify-write loop. The whole table is
            # a few hundred rows and the arithmetic is expressible in SQL, so
            # pulling it into Python would only add a race with live bumps.
            #
            # updated_at is deliberately NOT touched: it records when an
            # interest last saw ACTIVITY, and a decay pass is not activity.
            # Bumping it here would make every interest look freshly active.
            cur.execute(
                """
                UPDATE interests_interesttrendingscore
                SET score = CASE
                        -- GREATEST(0, ...) is the floor, not the CASE: a score
                        -- must never be negative, and multiplying cannot create
                        -- one but also cannot HEAL one. Any negative left by a
                        -- decrement elsewhere is clamped to 0 on the next run.
                        WHEN score * %(factor)s < %(min_score)s THEN 0
                        ELSE GREATEST(0, score * %(factor)s)
                    END,
                    recent_activity_boost = CASE
                        WHEN abs(1.0 + (recent_activity_boost - 1.0) * %(factor)s - 1.0)
                             < %(epsilon)s THEN 1.0
                        -- Floored at 0 too. The boost is a MULTIPLIER: a
                        -- negative one would invert the sign of every score it
                        -- touches, turning a popular interest into the most
                        -- suppressed one on the platform.
                        ELSE GREATEST(0, 1.0 + (recent_activity_boost - 1.0) * %(factor)s)
                    END
                -- score <> 0 rather than > 0, so a negative row is picked up
                -- and clamped rather than skipped for not being positive.
                WHERE score <> 0 OR recent_activity_boost <> 1.0
                """,
                {
                    "factor": factor,
                    "min_score": MIN_SCORE,
                    "epsilon": BOOST_EPSILON,
                },
            )
            updated = cur.rowcount

            cur.execute(
                """
                SELECT coalesce(sum(score), 0) AS total,
                       count(*) FILTER (WHERE score = 0) AS zeroed,
                       count(*) FILTER (WHERE recent_activity_boost > 1.0) AS boosted
                FROM interests_interesttrendingscore
                """
            )
            after = cur.fetchone()

    logger.info(
        f"🎉 DECAY COMPLETE: {updated} rows | total score now "
        f"{float(after['total']):.2f} | {after['zeroed']} at zero | "
        f"{after['boosted']} still boosted"
    )
    return updated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interest Trending Decay Cron")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show the effect, write nothing"
    )
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    args = parser.parse_args()

    if args.debug:
        os.environ["LOG_LEVEL"] = "DEBUG"
        logger = setup_logging()

    logger.info("🚀 SINGLE RUN - Interest trending decay")
    start_time = datetime.now()
    decay_trending_scores(dry_run=args.dry_run)
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ COMPLETE in {duration:.1f}s - container will exit normally")
    sys.exit(0)
