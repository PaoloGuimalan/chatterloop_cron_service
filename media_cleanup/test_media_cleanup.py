"""
The finder's logic against fake databases - no network.

    python -m unittest test_media_cleanup
"""

import json
import sys
import types
import unittest
from datetime import datetime, timezone

# The job imports these at module level; the tests never reach them.
for name in ("pika", "psycopg2", "pymongo", "dotenv"):
    if name not in sys.modules:
        try:
            __import__(name)
        except ImportError:
            stub = types.ModuleType(name)
            stub.MongoClient = object
            stub.load_dotenv = lambda *a, **k: None
            sys.modules[name] = stub

import media_cleanup_cron as job  # noqa: E402


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs
        self.queries = []

    def find(self, query, projection=None):
        self.queries.append(query)
        return [d for d in self.docs if self._match(d, query)]

    def _match(self, doc, query):
        for key, want in query.items():
            value = doc.get(key)
            if isinstance(want, dict):
                if "$in" in want and value not in want["$in"]:
                    return False
                if "$ne" in want and value == want["$ne"]:
                    return False
                if "$lt" in want and not (value and value < want["$lt"]):
                    return False
            elif value != want:
                return False
        return True


class FakeCursor:
    def __init__(self, live_posts):
        self.live_posts = live_posts
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if "FROM newsfeed_post" in sql:
            self.rows = [(p,) for p in params[0] if p in self.live_posts]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows


class FakePG:
    def __init__(self, live_posts=()):
        self.live_posts = set(live_posts)

    def cursor(self):
        return FakeCursor(self.live_posts)


OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 3, tzinfo=timezone.utc)


def record(url, status, attached=(), **extra):
    return {
        "version": 2,
        "status": status,
        "fileDetails": {"data": url},
        "attachedTo": [{"type": t, "id": i} for t, i in attached],
        **extra,
    }


class FinderTests(unittest.TestCase):
    def test_pending_and_unused_are_old_v2_records_only(self):
        files = FakeCollection(
            [
                record("u/old-pending", "pending", createdAt=OLD),
                record("u/new-pending", "pending", createdAt=NOW),
                record("u/old-ready", "ready", completedAt=OLD),
                record("u/new-ready", "ready", completedAt=NOW),
            ]
        )
        self.assertEqual([i["urls"] for i in job.find_pending(files, NOW)], [["u/old-pending"]])
        self.assertEqual([i["urls"] for i in job.find_unused(files, NOW)], [["u/old-ready"]])
        # Candidates carry no user: the worker treats them as unused.
        self.assertIsNone(job.find_pending(files, NOW)[0]["target"])

    def test_missed_skips_files_with_any_live_user(self):
        files = FakeCollection(
            [
                record("u/orphan", "attached", [("post", "dead")]),
                record("u/shared", "attached", [("post", "dead"), ("post", "alive")]),
                record("u/realm", "attached", [("realm_media", "r1")]),
                record("u/msg", "attached", [("message", "m-dead")]),
            ]
        )
        messages = FakeCollection([{"messageID": "m-dead", "isDeleted": True}])
        found = job.find_missed(FakePG(live_posts={"alive"}), files, messages)
        self.assertEqual(sorted(i["urls"][0] for i in found), ["u/msg", "u/orphan"])

    def test_url_of_drops_the_old_name_suffix(self):
        self.assertEqual(job.url_of("https://a/x.pdf%%%x.pdf"), "https://a/x.pdf")
        self.assertEqual(job.url_of(None), "")

    def test_jobs_are_batched_and_flagged(self):
        sent = []

        class Channel:
            def queue_declare(self, **kw):
                pass

            def basic_publish(self, exchange, routing_key, body, properties):
                sent.append((routing_key, json.loads(body)))

        class Connection:
            def __init__(self, params):
                pass

            def channel(self):
                return Channel()

            def close(self):
                pass

        job.pika.BlockingConnection = Connection
        job.pika.URLParameters = lambda url: url
        job.pika.BasicProperties = lambda **kw: kw
        env = {
            "RABBITMQ_USER": "u",
            "RABBITMQ_PASS": "p",
            "RABBITMQ_HOST": "h",
            "RABBITMQ_PORT": "5671",
            "RABBITMQ_VHOST": "v",
        }
        old = {k: job.os.environ.get(k) for k in env}
        job.os.environ.update(env)
        try:
            items = [job.item(f"u/{n}") for n in range(120)]
            jobs = job.publish({"unused": items}, dry_run=True)
        finally:
            for k, v in old.items():
                if v is None:
                    job.os.environ.pop(k, None)
                else:
                    job.os.environ[k] = v
        self.assertEqual(jobs, 3)  # 50 + 50 + 20
        self.assertEqual([len(body["items"]) for _, body in sent], [50, 50, 20])
        self.assertTrue(all(q == "media_release" for q, _ in sent))
        self.assertTrue(all(body["dryRun"] and body["source"] == "media_cleanup:unused" for _, body in sent))


if __name__ == "__main__":
    unittest.main()
