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
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))


def validate_env():
    required = ["DB_NAME", "DB_USER", "DB_PASS", "DB_HOST"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing env vars: {missing}")
        sys.exit(1)
    logger.info(f"DB: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")


@contextmanager
def get_db_transaction():
    conn = None
    try:
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        logger.info("Database connected!")
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Rollback: {e}")
        raise
    finally:
        if conn:
            conn.close()


def test_db_connection():
    try:
        with get_db_transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as healthy")
                result = cur.fetchone()
                logger.info(f"DB healthy: {result}")
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise


def recalculate_post_rankings():
    logger.info("🚀 Gradual decay ranking update...")

    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM newsfeed_post")
            total_posts = cur.fetchone()["total"]
            logger.info(f"📊 Processing {total_posts} posts")

    start = datetime.now()
    total_processed = 0
    offset = 0

    while True:
        try:
            processed = process_chunk(offset)
            if not processed:
                break
            total_processed += processed
            offset += CHUNK_SIZE
            logger.info(f"✅ {total_processed}/{total_posts}")
        except Exception as e:
            logger.error(f"❌ Chunk {offset//CHUNK_SIZE}: {e}")
            offset += CHUNK_SIZE

    duration = (datetime.now() - start).total_seconds()
    logger.info(
        f"🎉 GRADUAL DECAY COMPLETE: {total_processed} posts in {duration:.1f}s"
    )


def process_chunk(offset):
    with get_db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.post_id, p.date_posted,
                    COALESCE(likes.likes_count, 0) as post_likes_count,
                    COALESCE(comments.comment_count, 0) as post_comment_count,
                    COALESCE(shares.share_count, 0) as post_share_count,
                    COALESCE(ps.ranking_score, 1.0) as current_score
                FROM newsfeed_post p
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) as likes_count 
                    FROM newsfeed_reaction r 
                    WHERE r.post_id = p.post_id
                ) likes ON true
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) as comment_count 
                    FROM newsfeed_comment c 
                    WHERE c.post_id = p.post_id AND c.deleted_at IS NULL
                ) comments ON true
                LEFT JOIN LATERAL (
                    SELECT MAX(CASE WHEN ac.count_type = 'share' THEN ac.count END) as share_count
                    FROM newsfeed_activitycount ac 
                    WHERE ac.post_id = p.post_id
                ) shares ON true
                LEFT JOIN newsfeed_postscore ps ON ps.post_id = p.post_id
                ORDER BY p.post_id LIMIT %s OFFSET %s
            """,
                (CHUNK_SIZE, offset),
            )

            rows = cur.fetchall()
            if not rows:
                return 0

            now_utc = datetime.now(timezone.utc)
            data = []

            for row in rows:
                age_hours = max(
                    0.1, (now_utc - row["date_posted"]).total_seconds() / 3600
                )
                current_score = float(row["current_score"])

                # Calculate engagement CHANGE since last run (much smaller boost)
                likes = float(row["post_likes_count"])
                comments = float(row["post_comment_count"])
                shares = float(row["post_share_count"])
                total_engagement = comments * 3 + likes * 1 + shares * 5

                # ✅ TRUE DECAY: Apply decay FIRST, then tiny engagement boost
                decay_factor = 0.995**age_hours  # 0.5% decay per hour
                decayed_score = current_score * decay_factor

                # TINY boost only for NEW engagement (0.1% of engagement)
                engagement_boost = total_engagement * 0.001

                # FINAL: Decayed score + tiny boost
                new_score = decayed_score + engagement_boost
                new_score = max(0.001, new_score)

                data.append(
                    (
                        row["post_id"],
                        1.0,
                        current_score * 0.99,
                        1.0,  # Keep other fields stable
                        int(likes),
                        int(comments),
                        int(shares),
                        float(new_score),
                    )
                )

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

            before = float(rows[0]["current_score"]) if rows else 0
            after = data[0][7] if data else 0
            logger.info(
                f"✅ {len(rows)} posts (sample: {before:.3f} → {after:.3f} {'↓' if after < before else '↑'})"
            )
            return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Newsfeed Gradual Decay Ranking")
    parser.add_argument("--test", action="store_true", help="Test single run")
    args = parser.parse_args()

    validate_env()
    test_db_connection()

    if args.test:
        logger.info("🧪 TEST MODE - Gradual decay")
        recalculate_post_rankings()
        logger.info("✅ Test complete!")
        sys.exit(0)

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        recalculate_post_rankings,
        CronTrigger(hour="*/5"),
        id="decay_ranking",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info("⏰ GRADUAL DECAY CRON active (every 5h)")

    def shutdown(signum=None, frame=None):
        logger.info("🛑 Shutting down...")
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
