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
    logger.info("🚀 PRODUCTION DECAY CRON - AGE-BASED + 100 posts/batch")

    # STEP 1: Get ALL post_ids (194 posts = safe)
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT post_id FROM newsfeed_post ORDER BY post_id ASC")
            all_post_ids = [row["post_id"] for row in cur.fetchall()]

    total_posts = len(all_post_ids)
    logger.info(f"📊 {total_posts} posts to process")

    # STEP 2: Process EXACTLY 100 posts per batch
    total_processed = 0
    batch_num = 0

    while total_processed < total_posts:
        batch_num += 1
        start_idx = (batch_num - 1) * CHUNK_SIZE
        batch_ids = all_post_ids[start_idx : start_idx + CHUNK_SIZE]

        if not batch_ids:
            break

        logger.info(
            f"📦 Batch {batch_num}: {len(batch_ids)} posts (#{start_idx+1}-#{min(start_idx+CHUNK_SIZE, total_posts)})"
        )
        processed = process_exact_batch(batch_ids)
        total_processed += processed

    logger.info(
        f"🎉 DECAY COMPLETE: {total_processed} posts processed in {batch_num} batches"
    )


def process_exact_batch(post_ids):
    """Process EXACTLY these post_ids with AGE-BASED DECAY"""
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            # Get metrics + date_posted for age decay
            cur.execute(
                """
                SELECT 
                    p.post_id, p.date_posted,
                    COALESCE(likes.likes_count, 0) as post_likes_count,
                    COALESCE(comments.comment_count, 0) as post_comment_count,
                    COALESCE(shares.share_count, 0) as post_share_count,
                    COALESCE(ps.ranking_score, 1.0) as current_score
                FROM newsfeed_post p
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
                LEFT JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                WHERE p.post_id = ANY(%s)
                ORDER BY p.post_id
            """,
                (post_ids, post_ids, post_ids, post_ids),
            )

            rows = cur.fetchall()
            metrics = {row["post_id"]: row for row in rows}

            # AGE-BASED DECAY CALCULATION
            now_utc = datetime.now(timezone.utc)
            data = []
            total_decay = 0
            total_age = 0

            for post_id in post_ids:
                row = metrics.get(post_id, {})

                # ✅ AGE DECAY: Older posts decay MORE
                date_posted = row.get("date_posted")
                if date_posted:
                    age_hours = max(0.1, (now_utc - date_posted).total_seconds() / 3600)
                    decay_factor = 0.999**age_hours  # 24h=0.98, 7d=0.93, 14d=0.86
                    total_age += age_hours
                else:
                    decay_factor = 0.999
                    age_hours = 1.0

                current_score = float(row.get("current_score", 1.0))
                likes = float(row.get("post_likes_count", 0))
                comments = float(row.get("post_comment_count", 0))
                shares = float(row.get("post_share_count", 0))

                # FINAL SCORE: decayed_score + fresh engagement
                decayed_score = current_score * decay_factor
                engagement_boost = (comments * 3 + likes * 1 + shares * 5) * 0.0005
                new_score = max(0.001, decayed_score + engagement_boost)

                total_decay += (1 - decay_factor) * 100  # % decayed

                data.append(
                    (
                        post_id,
                        1.0,
                        1.0,
                        1.0,  # fixed weights
                        int(likes),
                        int(comments),
                        int(shares),
                        float(new_score),
                    )
                )

            # Atomic batch upsert
            execute_values(
                cur,
                """
                INSERT INTO newsfeed_postscore 
                (post_id, affinity_score, content_type_weight, recent_update_boost,
                 likes_count, comments_count, shares_count, ranking_score)
                VALUES %s 
                ON CONFLICT (post_id) DO UPDATE SET
                    affinity_score = EXCLUDED.affinity_score,
                    content_type_weight = EXCLUDED.content_type_weight,
                    recent_update_boost = EXCLUDED.recent_update_boost,
                    likes_count = EXCLUDED.likes_count,
                    comments_count = EXCLUDED.comments_count,
                    shares_count = EXCLUDED.shares_count,
                    ranking_score = EXCLUDED.ranking_score
            """,
                data,
            )

            avg_age = total_age / len(post_ids)
            avg_decay_pct = total_decay / len(post_ids)
            avg_score = sum(s[7] for s in data) / len(data)

            logger.info(
                f"   ✅ {len(post_ids)} posts | age: {avg_age:.1f}h | decay: {avg_decay_pct:.2f}% | score: {avg_score:.4f}"
            )
            return len(post_ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post Ranking Decay Cron")
    parser.add_argument("--test", action="store_true", help="Test single run")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    args = parser.parse_args()

    if args.debug:
        os.environ["LOG_LEVEL"] = "DEBUG"
        setup_logging()

    if args.test:
        logger.info("🧪 TEST MODE - AGE-BASED DECAY")
        start_time = datetime.now()
        recalculate_post_rankings()
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Test complete in {duration:.1f}s")
        sys.exit(0)

    # Production cron: Every 5 hours
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
