# media_cleanup

Finds stored files that should probably go and hands them to the worker,
which decides and deletes.

```
docker build -t johnpauloramil187/chatterloop_media_cleanup .
docker run --rm johnpauloramil187/chatterloop_media_cleanup python media_cleanup_cron.py              # list only
docker run --rm johnpauloramil187/chatterloop_media_cleanup python media_cleanup_cron.py --preview    # worker logs decisions
docker run --rm johnpauloramil187/chatterloop_media_cleanup                                          # --apply (the default CMD)
```

Tests: `python -m unittest test_media_cleanup`

## How it runs

Once, then exits, like `post_scores` and `interest_trending`. The scheduler
re-runs it; daily is plenty.

## What it finds

| sweep     | candidates                                                                       |
| --------- | -------------------------------------------------------------------------------- |
| `pending` | upload links handed out over 24h ago and never finished                          |
| `unused`  | uploads finished over 7 days ago that nothing ever used                          |
| `missed`  | files whose every user (post / comment / message) is deleted                     |
| `legacy`  | files of content deleted before instant deletes existed (`--legacy`, first run) |

## Why it deletes nothing itself

Whether a file may go is decided in **one place**: `worker_service`'s
`media_release` consumer (`internal/services/media`). It:

- keeps anything still used, whether by another post, a message, an avatar or a moment poster
- holds anything whose content was reported
- never touches a key outside Chatterloop's folders of the bucket shared with NeonSystems

A second copy of those rules here could drift, and a drift here deletes people's
files. So this job only **publishes candidates** to `media_release`, the same
queue the instant deletes use. It needs no storage credentials.

## Modes

| flag        | effect                                                                                                                                         |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| _(none)_    | list candidates, publish nothing                                                                                                               |
| `--preview` | publish **dry-run** jobs: the worker decides each file and logs `media_release preview` with the decision, but writes and deletes nothing      |
| `--apply`   | publish real jobs                                                                                                                              |

**First run:** run `--legacy --preview` and read the worker's preview lines
before ever running `--legacy --apply`. Only then put `--apply` on the schedule.

## Configuration

See `.env.example`: Postgres (same as the other jobs), plus Mongo and RabbitMQ
(same as `worker_service`). Optional `MEDIA_PENDING_AFTER_HOURS` (24) and
`MEDIA_UNUSED_AFTER_DAYS` (7).
