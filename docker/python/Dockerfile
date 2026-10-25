# Crawl worker (HTTP sources), ES projector, ops tools, the Tiki stub.
# Tag: ecommerce/marketplace-python:2. No ENTRYPOINT: each service sets its command.
FROM python:3.12.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app

# pg_dump and pg_restore for `ops backup` / `ops restore`. They must run inside
# this container: the backup holds the batch advisory lock for its whole
# duration, and a lock belongs to the session that took it. Pinned to major 18
# to match the postgres:18.3 server; change both pins together.
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

# Installed, so a container started without the source mount (an Airflow task)
# still runs `python -m ecomdata...`. Compose mounts the working tree over /app
# for development; PYTHONPATH then prefers it to the installed copy.
COPY pyproject.toml constraints.txt README.md /build/
COPY src /build/src
RUN pip install --no-cache-dir -c /build/constraints.txt /build && rm -rf /build

WORKDIR /app
USER nobody
