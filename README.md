build:

- docker build -t johnpauloramil187/chatterloop_cron_service .

test:

- docker run --rm johnpauloramil187/chatterloop_cron_service python post_scores_cron.py --test

Realm deletion reconciliation:

- Build the `realm_cleanup/` context; pass the existing DB and RabbitMQ environment at runtime.
- Default invocation only lists committed deletion markers. It has no storage credentials and never changes database records.
- Register `python realm_cleanup_cron.py --apply` with the existing cron manager at `* * * * *` (one minute). The worker rechecks the exact persisted marker and safely handles duplicate or completed jobs.
- A broker/storage outage leaves the marker available for the next run; do not clear it manually to indicate success.
- Test offline: `python -m unittest discover -s realm_cleanup -p 'test_*.py'`.
