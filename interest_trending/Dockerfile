FROM python:3.11-slim

RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .env* interest_trending_cron.py ./

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pg_isready -U "${DB_USER}" -h "${DB_HOST}" -p "${DB_PORT}" || exit 1

# Runs once and exits, like post_scores. DECAY_INTERVAL_HOURS must match the
# schedule that invokes this container - the decay is applied per run.
CMD python interest_trending_cron.py
