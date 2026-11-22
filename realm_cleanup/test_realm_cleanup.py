import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path


class FinderTests(unittest.TestCase):
    def test_lists_and_republishes_exact_markers_without_mutation(self):
        spec = importlib.util.spec_from_file_location('realm_cleanup_cron', Path(__file__).with_name('realm_cleanup_cron.py'))
        job = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(job)
        stamp = datetime(2026, 11, 22, tzinfo=timezone.utc)

        class Cursor:
            def execute(self, sql, params):
                self.last = params[0]
                self.assert_read(sql)
            def assert_read(self, sql):
                if not sql.lstrip().upper().startswith('SELECT'):
                    raise AssertionError('finder must be read only')
            def fetchall(self):
                return [('r1', stamp)] if not self.last else []

        sent = []
        listed = job.reconcile(Cursor(), apply=False, publish=sent.append)
        self.assertEqual(listed, 1)
        self.assertEqual(sent, [])
        self.assertEqual(job.reconcile(Cursor(), apply=True, publish=sent.append), 1)
        self.assertEqual(sent, [{'realm_id': 'r1', 'deletion_requested_at': stamp.isoformat()}])
        job.reconcile(Cursor(), apply=True, publish=sent.append)
        self.assertEqual(sent[0], sent[1])

    def test_failed_publish_is_not_success(self):
        spec = importlib.util.spec_from_file_location('realm_cleanup_cron', Path(__file__).with_name('realm_cleanup_cron.py'))
        job = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(job)
        class Cursor:
            def execute(self, *args): pass
            def fetchall(self): return [('r', datetime.now(timezone.utc))]
        def unavailable(payload): raise OSError('broker unavailable')
        with self.assertRaises(OSError): job.reconcile(Cursor(), apply=True, publish=unavailable)


if __name__ == '__main__': unittest.main()
