FROM python:3.11-slim

RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .env* post_scores_cron.py ./

# ✅ Fix: Use shell form to pass arguments
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pg_isready -U "${DB_USER}" -h "${DB_HOST}" -p "${DB_PORT}" || exit 1

# Shell form CMD accepts arguments
CMD python post_scores_cron.py
