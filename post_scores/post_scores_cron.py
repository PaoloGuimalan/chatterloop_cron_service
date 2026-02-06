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
    """Decay using FULL PostScore formula: ranking_score * affinity * content_weight * boost"""
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            # Get ALL PostScore fields + fresh date_posted
            cur.execute(
                """
                SELECT 
                    ps.post_id,
                    p.date_posted,
                    COALESCE(ps.affinity_score, 1.0) as affinity_score,
                    COALESCE(ps.content_type_weight, 1.0) as content_type_weight,
                    COALESCE(ps.recent_update_boost, 1.0) as recent_update_boost,
                    COALESCE(ps.likes_count, 0) as likes_count,
                    COALESCE(ps.comments_count, 0) as comments_count,
                    COALESCE(ps.shares_count, 0) as shares_count,
                    COALESCE(ps.ranking_score, 1.0) as current_score
                FROM newsfeed_post p
                LEFT JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                WHERE p.post_id = ANY(%s)
                ORDER BY p.post_id
            """,
                (post_ids,),
            )

            rows = cur.fetchall()
            now_utc = datetime.now(timezone.utc)
            updates = []

            for row in rows:
                post_id = row["post_id"]

                # ✅ USE ALL ORIGINAL PostScore VALUES
                affinity_score = float(row.get("affinity_score", 1.0))
                content_type_weight = float(row.get("content_type_weight", 1.0))
                recent_update_boost = float(row.get("recent_update_boost", 1.0))
                likes = int(row.get("likes_count", 0))
                comments = int(row.get("comments_count", 0))
                shares = int(row.get("shares_count", 0))

                # AGE DECAY on base score
                age_hours = max(
                    0.1, (now_utc - row["date_posted"]).total_seconds() / 3600
                )
                decay_factor = 0.999**age_hours
                current_score = float(row.get("current_score", 1.0))

                # ✅ PROPER DECAY: Apply decay THEN multiply by ALL weights
                decayed_base = current_score * decay_factor
                engagement_boost = (comments * 3 + likes * 1 + shares * 5) * 0.0005
                new_base_score = max(0.001, decayed_base + engagement_boost)

                # FINAL SCORE using ALL original weights
                new_score = (
                    new_base_score
                    * affinity_score
                    * content_type_weight
                    * recent_update_boost
                )

                updates.append((post_id, likes, comments, shares, new_score))

                # Log sample to verify weights preserved
                if len(updates) <= 2:
                    logger.info(
                        f"   {post_id[:8]}... "
                        f"A={affinity_score:.2f} C={content_type_weight:.2f} "
                        f"B={recent_update_boost:.2f} → {new_score:.4f}"
                    )

            # UPDATE ONLY counts + final ranking_score
            execute_values(
                cur,
                """
                UPDATE newsfeed_postscore 
                SET 
                    likes_count = excluded.likes_count,
                    comments_count = excluded.comments_count,
                    shares_count = excluded.shares_count,
                    ranking_score = excluded.ranking_score
                FROM (VALUES %s) AS excluded(post_id, likes_count, comments_count, shares_count, ranking_score)
                WHERE newsfeed_postscore.post_id = excluded.post_id
            """,
                updates,
            )

            avg_score = sum(u[4] for u in updates) / len(updates)
            logger.info(f"✅ {len(updates)} posts | avg score: {avg_score:.4f}")
            return len(updates)

    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    p.post_id, p.date_posted,
                    COALESCE(likes.likes_count, 0), 
                    COALESCE(comments.comment_count, 0),
                    COALESCE(shares.share_count, 0),
                    COALESCE(ps.ranking_score, 1.0)
                FROM newsfeed_post p
                LEFT JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                LEFT JOIN (SELECT post_id, COUNT(*) as likes_count FROM newsfeed_reaction WHERE post_id = ANY(%s) GROUP BY post_id) likes ON p.post_id = likes.post_id
                LEFT JOIN (SELECT post_id, COUNT(*) as comment_count FROM newsfeed_comment WHERE post_id = ANY(%s) AND deleted_at IS NULL GROUP BY post_id) comments ON p.post_id = comments.post_id
                LEFT JOIN (SELECT post_id, MAX(CASE WHEN count_type = 'share' THEN count END) as share_count FROM newsfeed_activitycount WHERE post_id = ANY(%s) GROUP BY post_id) shares ON p.post_id = shares.post_id
                WHERE p.post_id = ANY(%s)
            """,
                (post_ids, post_ids, post_ids, post_ids),
            )

            rows = cur.fetchall()
            now_utc = datetime.now(timezone.utc)
            updates = []

            for row in rows:
                post_id = row["post_id"]
                likes = int(row.get("likes_count", 0))
                comments = int(row.get("comment_count", 0))
                shares = int(row.get("share_count", 0))

                age_hours = max(
                    0.1, (now_utc - row["date_posted"]).total_seconds() / 3600
                )
                decay_factor = 0.999**age_hours
                current_score = float(row.get("ranking_score", 1.0))
                new_score = max(
                    0.001,
                    current_score * decay_factor
                    + (comments * 3 + likes * 1 + shares * 5) * 0.0005,
                )

                # ✅ CORRECT ORDER: (post_id, likes_count, comments_count, shares_count, ranking_score)
                updates.append((post_id, likes, comments, shares, new_score))

            # ✅ FIXED SYNTAX - column order matches VALUES order
            execute_values(
                cur,
                """
                UPDATE newsfeed_postscore 
                SET 
                    likes_count = excluded.likes_count,
                    comments_count = excluded.comments_count,
                    shares_count = excluded.shares_count,
                    ranking_score = excluded.ranking_score
                FROM (VALUES %s) AS excluded(post_id, likes_count, comments_count, shares_count, ranking_score)
                WHERE newsfeed_postscore.post_id = excluded.post_id
            """,
                updates,
            )

            logger.info(
                f"✅ {len(updates)} posts updated - content_type_weight UNTOUCHED"
            )
            return len(updates)

    """UPDATE ONLY counts + ranking_score - PRESERVE content_type_weight!"""
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            # Get fresh metrics + current ranking_score
            cur.execute(
                """
                SELECT 
                    p.post_id, p.date_posted,
                    COALESCE(likes.likes_count, 0) as post_likes_count,
                    COALESCE(comments.comment_count, 0) as post_comment_count,
                    COALESCE(shares.share_count, 0) as post_share_count,
                    COALESCE(ps.ranking_score, 1.0) as current_score
                FROM newsfeed_post p
                LEFT JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                LEFT JOIN (
                    SELECT post_id, COUNT(*) as likes_count 
                    FROM newsfeed_reaction WHERE post_id = ANY(%s) GROUP BY post_id
                ) likes ON p.post_id = likes.post_id
                LEFT JOIN (
                    SELECT post_id, COUNT(*) as comment_count 
                    FROM newsfeed_comment WHERE post_id = ANY(%s) AND deleted_at IS NULL GROUP BY post_id
                ) comments ON p.post_id = comments.post_id
                LEFT JOIN (
                    SELECT post_id, MAX(CASE WHEN count_type = 'share' THEN count END) as share_count
                    FROM newsfeed_activitycount WHERE post_id = ANY(%s) GROUP BY post_id
                ) shares ON p.post_id = shares.post_id
                WHERE p.post_id = ANY(%s)
                ORDER BY p.post_id
            """,
                (post_ids, post_ids, post_ids, post_ids),
            )

            rows = cur.fetchall()

            now_utc = datetime.now(timezone.utc)
            updates = []

            for row in rows:
                post_id = row["post_id"]

                # Fresh counts
                likes = int(row.get("post_likes_count", 0))
                comments = int(row.get("post_comment_count", 0))
                shares = int(row.get("post_share_count", 0))

                # Age-based decay on ranking_score ONLY
                date_posted = row.get("date_posted")
                if date_posted:
                    age_hours = max(0.1, (now_utc - date_posted).total_seconds() / 3600)
                    decay_factor = 0.999**age_hours
                else:
                    decay_factor = 0.999

                current_score = float(row.get("current_score", 1.0))
                decayed_score = current_score * decay_factor
                engagement_boost = (comments * 3 + likes * 1 + shares * 5) * 0.0005
                new_score = max(0.001, decayed_score + engagement_boost)

                # ONLY these 4 fields get updated
                updates.append((likes, comments, shares, new_score, post_id))

            # ✅ CRITICAL: UPDATE ONLY 4 fields - NO INSERT, NO content_type_weight override!
            execute_values(
                cur,
                """
                UPDATE newsfeed_postscore 
                SET 
                    likes_count = excluded.likes_count,
                    comments_count = excluded.comments_count,
                    shares_count = excluded.shares_count,
                    ranking_score = excluded.ranking_score
                FROM (VALUES %s) AS excluded(post_id, likes_count, comments_count, shares_count, ranking_score)
                WHERE newsfeed_postscore.post_id = excluded.post_id
            """,
                updates,
            )

            avg_score = sum(u[3] for u in updates) / len(updates)
            logger.info(
                f"   ✅ {len(updates)} posts | avg score: {avg_score:.4f} | content_type_weight PRESERVED"
            )
            return len(updates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post Ranking Decay Cron")
    parser.add_argument("--test", action="store_true", help="Test single run")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    args = parser.parse_args()

    if args.debug:
        os.environ["LOG_LEVEL"] = "DEBUG"
        setup_logging()

    if args.test:
        logger.info("🧪 TEST MODE - DECAY ONLY (content_type_weight preserved)")
        start_time = datetime.now()
        recalculate_post_rankings()
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Test complete in {duration:.1f}s")
        sys.exit(0)

    # Production cron
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        recalculate_post_rankings,
        CronTrigger(hour="*/5"),
        id="post_decay_cron",
        max_instances=1,
    )
    scheduler.start()
    logger.info("⏰ PRODUCTION CRON STARTED - Every 5h")

    def shutdown(signum=None, frame=None):
        logger.info("🛑 Graceful shutdown...")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            import time

            time.sleep(10)
    except KeyboardInterrupt:
        shutdown()
