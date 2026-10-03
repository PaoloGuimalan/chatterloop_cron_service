FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .env* media_cleanup_cron.py ./

# Runs once and exits, like post_scores and interest_trending - the scheduler
# re-runs the container. Without --apply it only lists; the schedule should
# run it with --apply once a first --preview has been reviewed.
CMD python media_cleanup_cron.py --apply
