# Crawl worker, Silver sink, and the ops tools (Phase 8 plan 7.1).
# No ENTRYPOINT: each Compose service sets its own command.
FROM python:3.12.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# pg_dump and pg_restore for `ops backup` / `ops restore` (plan section 12).
# They must run inside this container, not from mp.ps1: the backup holds the
# batch advisory lock for its whole duration, and a lock belongs to the
# session that took it — splitting the dump into a second container would
# drop it. Pinned to major 18 to match the pinned postgres:18.3 server;
# pg_dump refuses a server newer than itself, so Debian's own client (17) is
# not enough. Change both pins together.
ARG PG_MAJOR=18
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo "$VERSION_CODENAME")-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends "postgresql-client-${PG_MAJOR}"; \
    apt-get purge -y --auto-remove curl gnupg; \
    rm -rf /var/lib/apt/lists/*; \
    pg_dump --version

COPY requirements-marketplace.txt /tmp/requirements-marketplace.txt
RUN pip install --no-cache-dir -r /tmp/requirements-marketplace.txt && rm /tmp/requirements-marketplace.txt

# The source is mounted read-only at /app; data goes to MinIO or volumes.
WORKDIR /app
USER nobody
