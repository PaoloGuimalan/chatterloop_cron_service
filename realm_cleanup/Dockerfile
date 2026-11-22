FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY realm_cleanup_cron.py .
# Default is read-only; schedule with --apply after deployment checks.
CMD ["python", "realm_cleanup_cron.py"]
