# Crawl worker, Silver sink, and later the ops tools (Phase 8 plan 7.1).
# No ENTRYPOINT: each Compose service sets its own command.
FROM python:3.12.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY requirements-marketplace.txt /tmp/requirements-marketplace.txt
RUN pip install --no-cache-dir -r /tmp/requirements-marketplace.txt && rm /tmp/requirements-marketplace.txt

# The source is mounted read-only at /app; data goes to MinIO or volumes.
WORKDIR /app
USER nobody
