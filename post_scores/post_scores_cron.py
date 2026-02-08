#!/usr/bin/env python3
import os, signal, sys, logging
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
from contextlib import contextmanager
import argparse

load_dotenv()
os.environ["PYTHONUNBUFFERED"] = "1"


def setup_logging():
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
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
    "application_name": "ranking-cron",
}

CHUNK_SIZE = 100


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


def recalculate_post_rankings():
    logger.info("🚀 PRODUCTION DECAY CRON - PRESERVES content_type_weight")

    # STEP 1: Get ALL post_ids
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT post_id FROM newsfeed_post ORDER BY post_id ASC")
            all_post_ids = [row["post_id"] for row in cur.fetchall()]

    total_posts = len(all_post_ids)
    logger.info(f"📊 {total_posts} posts to process")

    # STEP 2: Process 100 posts per batch
    total_processed = 0
    batch_num = 0

    while total_processed < total_posts:
        batch_num += 1
        start_idx = (batch_num - 1) * CHUNK_SIZE
        batch_ids = all_post_ids[start_idx : start_idx + CHUNK_SIZE]

        if not batch_ids:
            break

        logger.info(f"📦 Batch {batch_num}: {len(batch_ids)} posts")
        processed = process_exact_batch(batch_ids)
        total_processed += processed

    logger.info(f"🎉 DECAY COMPLETE: {total_processed} posts in {batch_num} batches")


def process_exact_batch(post_ids):
    """Decay using decay(current_score) + engagement_boost - PREVENTS runaway growth"""
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            # Get FRESH counts from source tables + current score
            cur.execute(
                """
                SELECT 
                    p.post_id, p.date_posted,
                    COALESCE(ps.likes_count, 0) as likes,
                    COALESCE(ps.comments_count, 0) as comments,
                    COALESCE(ps.shares_count, 0) as shares,
                    COALESCE(ps.ranking_score, 1.0) as current_score,
                    COALESCE(ps.content_type_weight, 1.0) as content_type_weight,
                    COALESCE(ps.recent_update_boost, 1.0) as recent_update_boost,
                    COALESCE(ps.affinity_score, 1) as affinity_score
                FROM newsfeed_post p
                JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                WHERE p.post_id = ANY(%s)
                ORDER BY p.post_id
                """,
                (post_ids,),
            )

            # rows = cur.fetchall()

            now_utc = datetime.now(timezone.utc)
            updates = []

            for row in cur.fetchall():
                post_id = row["post_id"]

                # Fresh engagement counts
                likes = int(row["likes"])
                comments = int(row["comments"])
                shares = int(row["shares"])

                # Preserve original weights
                content_weight = float(row["content_type_weight"])
                update_boost = float(row["recent_update_boost"])

                # formula
                age_hours = (now_utc - row["date_posted"]).total_seconds() / 3600
                affinity_score = float(row["affinity_score"])
                content_type_weight = float(row["content_type_weight"])
                comments_count = comments
                likes_count = likes
                shares_count = shares

                base_engagement = 1

                weighted_engagement = (
                    comments_count * 3
                    + likes_count * 1
                    + shares_count * 5
                    + base_engagement
                )
                decay_factor = (age_hours + 1) ** 0.5
                ranking_score = (
                    (weighted_engagement / decay_factor)
                    * affinity_score
                    * content_type_weight
                    * update_boost
                )

                updates.append(
                    (
                        post_id,  # post_id
                        ranking_score,  # ranking_score
                        1,  # recent_update_boost (preserved)
                    )
                )

                # Log first 2 for verification
                if len(updates) <= 2:
                    logger.info(
                        f"  {post_id[:8]}... "
                        f"L={likes} C={comments} S={shares} "
                        f"D={decay_factor:.3f} E={update_boost:.4f} "
                        f"W={weighted_engagement:.2f} → {ranking_score:.4f}"
                    )

            # Atomic batch update - ONLY specified fields
            execute_values(
                cur,
                """
                UPDATE newsfeed_postscore
                SET
                    ranking_score = excluded.ranking_score,
                    recent_update_boost = excluded.recent_update_boost
                FROM (VALUES %s) AS excluded(
                    post_id, ranking_score, recent_update_boost
                )
                WHERE newsfeed_postscore.post_id = excluded.post_id
            """,
                updates,
            )

            avg_score = sum(u[1] for u in updates) / len(updates)
            logger.info(f"✅ {len(updates)} posts | avg score: {avg_score:.4f}")
            return len(updates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post Ranking Decay Cron")
    parser.add_argument("--test", action="store_true", help="Test single run")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    args = parser.parse_args()

    # if args.debug:
    #     os.environ["LOG_LEVEL"] = "DEBUG"
    #     setup_logging()

    # if args.test:
    #     logger.info("🧪 TEST MODE - Single decay run (content_type_weight preserved)")
    #     start_time = datetime.now()
    #     recalculate_post_rankings()
    #     duration = (datetime.now() - start_time).total_seconds()
    #     logger.info(f"✅ Test complete in {duration:.1f}s")
    #     sys.exit(0)

    # # Production cron - every 5 hours
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(
    #     recalculate_post_rankings,
    #     CronTrigger(hour=os.getenv("CRON_HOURS"), minute=os.getenv("CRON_MINUTES")),
    #     id="post_decay_cron",
    #     max_instances=1,
    # )
    # scheduler.start()
    # logger.info("⏰ PRODUCTION CRON STARTED - Every 5 hours")

    # def shutdown(signum=None, frame=None):
    #     logger.info("🛑 Graceful shutdown...")
    #     scheduler.shutdown()
    #     sys.exit(0)

    # signal.signal(signal.SIGTERM, shutdown)
    # signal.signal(signal.SIGINT, shutdown)

    # try:
    #     while True:
    #         import time

    #         time.sleep(10)
    # except KeyboardInterrupt:
    #     shutdown()

    if args.test:
        logger.info("🧪 TEST MODE")

    # SINGLE EXECUTION - No scheduler, no infinite loop
    logger.info("🚀 SINGLE RUN - Post ranking decay")
    recalculate_post_rankings()
    logger.info("✅ COMPLETE - Container will exit normally")
    sys.exit(0)
