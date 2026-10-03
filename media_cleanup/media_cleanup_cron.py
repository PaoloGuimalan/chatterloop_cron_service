#!/usr/bin/env python3
"""
Find stored files that should probably go, and hand them to the worker.

WHAT IT FINDS
-------------
  pending  upload links handed out over 24h ago and never finished
  unused   uploads finished over 7 days ago that nothing ever used
  missed   files whose every user (post / comment / message) is deleted -
           an instant delete that got lost
  legacy   files of posts, comments and messages deleted before instant
           deletes existed. Only with --legacy; meant for the first run.

WHY IT DELETES NOTHING ITSELF
-----------------------------
Whether a file may go is decided in exactly one place: worker_service's
media_release consumer (internal/services/media). It keeps anything still
used - another post, a message, an avatar, a moment poster - holds anything
whose content was reported, and never touches a key outside Chatterloop's
folders of the bucket it shares with NeonSystems. Writing those rules a
second time here would let the two drift, and a drift here deletes people's
files. So this job is a FINDER: it publishes candidates to media_release,
the same queue the instant deletes use, and the worker decides.

That also means this job needs no storage credentials.

RUNS ONCE AND EXITS
-------------------
Like post_scores and interest_trending: the scheduler re-runs the container
(daily is plenty). Exit 0 on success, non-zero on failure.

MODES
-----
  (default)   list the candidates, publish nothing
  --preview   publish them as DRY-RUN jobs: the worker decides each one and
              logs "media_release preview" with its decision, but writes and
              deletes nothing. How to review a first --legacy run.
  --apply     publish them for real
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import pika
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
os.environ["PYTHONUNBUFFERED"] = "1"

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

QUEUE = "media_release"
PENDING_AFTER = timedelta(hours=float(os.getenv("MEDIA_PENDING_AFTER_HOURS", "24")))
UNUSED_AFTER = timedelta(days=float(os.getenv("MEDIA_UNUSED_AFTER_DAYS", "7")))
# URLs per published job - an account's worth of files would otherwise be one
# job that runs past the worker's deadline.
JOB_SIZE = 50
SAMPLE = 20

# messageType values that are never a file.
NOT_FILES = ["text", "notif", "post"]


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=int(os.getenv("DB_PORT", 5432)),
        connect_timeout=30,
        sslmode="require",
        application_name="media-cleanup-cron",
    )


def mongo_db():
    # The same URI the worker builds (internal/connections/mongo.go).
    uri = "mongodb+srv://{}:{}@{}/?retryWrites=true&w=majority".format(
        quote_plus(os.environ["MONGODB_CLUSTER_USER"]),
        quote_plus(os.environ["MONGODB_CLUSTER_PASS"]),
        os.environ["MONGODB_CLUSTER_HOST"],
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=30000)
    return client, client[os.getenv("MONGODB_DB", "chatterloop")]


def url_of(value):
    """The URL half of a stored value ("url%%%name" in old rows)."""
    return value.split("%%%")[0].strip() if isinstance(value, str) else ""


def item(url, target=None, conversation_id=None):
    entry = {"target": target, "urls": [url]}
    if conversation_id:
        entry["context"] = {"conversationID": conversation_id}
    return entry


# ---- finders ----


def find_pending(files, now):
    cursor = files.find(
        {"version": 2, "status": "pending", "createdAt": {"$lt": now - PENDING_AFTER}},
        {"fileDetails.data": 1},
    )
    return [item(doc["fileDetails"]["data"]) for doc in cursor]


def find_unused(files, now):
    cursor = files.find(
        {"version": 2, "status": "ready", "completedAt": {"$lt": now - UNUSED_AFTER}},
        {"fileDetails.data": 1},
    )
    return [item(doc["fileDetails"]["data"]) for doc in cursor]


def live_targets(pg, messages, targets):
    """The subset of (type, id) targets that still exist and aren't deleted."""
    by_type = {}
    for t in targets:
        by_type.setdefault(t.get("type"), set()).add(str(t.get("id")))
    live = set()
    with pg.cursor() as cur:
        if by_type.get("post"):
            cur.execute(
                "SELECT post_id FROM newsfeed_post WHERE post_id = ANY(%s) AND deleted_at IS NULL",
                (list(by_type["post"]),),
            )
            live |= {("post", r[0]) for r in cur.fetchall()}
        if by_type.get("comment"):
            cur.execute(
                "SELECT comment_id FROM newsfeed_comment "
                "WHERE comment_id = ANY(%s) AND deleted_at IS NULL",
                (list(by_type["comment"]),),
            )
            live |= {("comment", r[0]) for r in cur.fetchall()}
    if by_type.get("message"):
        for doc in messages.find(
            {"messageID": {"$in": list(by_type["message"])}, "isDeleted": {"$ne": True}},
            {"messageID": 1},
        ):
            live.add(("message", str(doc["messageID"])))
    # Any other kind (a realm's avatar...) counts as live: only the worker
    # decides those, and it keeps them.
    for kind, ids in by_type.items():
        if kind not in ("post", "comment", "message"):
            live |= {(kind, i) for i in ids}
    return live


def find_missed(pg, files, messages):
    """Attached files none of whose users is alive any more."""
    docs = list(files.find({"version": 2, "status": "attached"}, {"fileDetails.data": 1, "attachedTo": 1}))
    targets = [t for doc in docs for t in doc.get("attachedTo") or []]
    live = live_targets(pg, messages, targets) if targets else set()
    found = []
    for doc in docs:
        users = {(t.get("type"), str(t.get("id"))) for t in doc.get("attachedTo") or []}
        if not users & live:
            found.append(item(doc["fileDetails"]["data"]))
    return found


def find_legacy(pg, messages):
    found = []
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT r.post_id, r.reference
              FROM newsfeed_postreference r
              JOIN newsfeed_post p ON p.post_id = r.post_id
             WHERE p.deleted_at IS NOT NULL AND r.reference LIKE 'http%%'
            UNION
            SELECT p.post_id, p.details -> 'poster' ->> 'url'
              FROM newsfeed_post p
             WHERE p.deleted_at IS NOT NULL AND p.details -> 'poster' ->> 'url' IS NOT NULL
            """
        )
        for post_id, url in cur.fetchall():
            found.append(item(url_of(url), {"type": "post", "id": str(post_id)}))

        cur.execute(
            "SELECT comment_id, attachment FROM newsfeed_comment "
            "WHERE deleted_at IS NOT NULL AND attachment LIKE 'http%%'"
        )
        for comment_id, url in cur.fetchall():
            found.append(item(url_of(url), {"type": "comment", "id": str(comment_id)}))

    for doc in messages.find(
        {
            "isDeleted": True,
            "messageType": {"$nin": NOT_FILES},
            "content": {"$regex": r"^https?://"},
        },
        {"messageID": 1, "conversationID": 1, "content": 1, "attachment.url": 1},
    ):
        url = (doc.get("attachment") or {}).get("url") or url_of(doc.get("content"))
        found.append(
            item(url, {"type": "message", "id": str(doc["messageID"])}, str(doc["conversationID"]))
        )
    return found


# ---- publishing ----


def publish(sweeps, dry_run):
    params = pika.URLParameters(
        "{}://{}:{}@{}:{}/{}?heartbeat=60".format(
            os.getenv("RABBITMQ_PROTOCOL", "amqp"),
            quote_plus(os.environ["RABBITMQ_USER"]),
            quote_plus(os.environ["RABBITMQ_PASS"]),
            os.environ["RABBITMQ_HOST"],
            os.environ["RABBITMQ_PORT"],
            quote_plus(os.environ["RABBITMQ_VHOST"]),
        )
    )
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        jobs = 0
        for sweep, items in sweeps.items():
            for i in range(0, len(items), JOB_SIZE):
                body = {
                    "items": items[i : i + JOB_SIZE],
                    "dryRun": dry_run,
                    "source": f"media_cleanup:{sweep}",
                }
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE,
                    body=json.dumps(body),
                    properties=pika.BasicProperties(
                        delivery_mode=2, content_type="application/json"
                    ),
                )
                jobs += 1
        return jobs
    finally:
        connection.close()


def run(apply, preview, legacy, verbose):
    now = datetime.now(timezone.utc)
    mongo, db = mongo_db()
    pg = pg_connect()
    try:
        files, messages = db["files"], db["messages"]
        sweeps = {
            "pending": find_pending(files, now),
            "unused": find_unused(files, now),
            "missed": find_missed(pg, files, messages),
        }
        if legacy:
            sweeps["legacy"] = find_legacy(pg, messages)
    finally:
        pg.close()
        mongo.close()

    for sweep, items in sweeps.items():
        logger.info(f"{sweep}: {len(items)} candidate file(s)")
        for entry in items if verbose else items[:SAMPLE]:
            target = entry["target"]
            label = f"{target['type']} {target['id']}" if target else "no user"
            logger.info(f"    {entry['urls'][0]}  ({label})")
        if not verbose and len(items) > SAMPLE:
            logger.info(f"    ... and {len(items) - SAMPLE} more (--verbose lists all)")

    total = sum(len(items) for items in sweeps.values())
    if not (apply or preview):
        logger.info(f"LIST ONLY: {total} candidate(s), nothing published")
        return
    if total == 0:
        logger.info("nothing to publish")
        return
    jobs = publish(sweeps, dry_run=not apply)
    if apply:
        logger.info(f"published {jobs} job(s) - the worker deletes what is unused")
    else:
        logger.info(
            f"published {jobs} PREVIEW job(s) - see the worker's 'media_release "
            "preview' log lines for each decision; nothing is deleted"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stored file cleanup (finder)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="publish real release jobs")
    mode.add_argument(
        "--preview", action="store_true", help="publish dry-run jobs the worker only logs"
    )
    parser.add_argument(
        "--legacy", action="store_true", help="also files of content deleted before instant deletes"
    )
    parser.add_argument("--verbose", action="store_true", help="list every candidate")
    args = parser.parse_args()

    start = datetime.now()
    logger.info("🚀 SINGLE RUN - media cleanup")
    run(args.apply, args.preview, args.legacy, args.verbose)
    logger.info(f"✅ COMPLETE in {(datetime.now() - start).total_seconds():.1f}s")
    sys.exit(0)
