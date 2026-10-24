#!/bin/sh
# Full Lambda stack with log observability. Usage: sh run-fullstack.sh [compose args...]
cd "$(dirname "$0")"
set -a; . env/tiki-html.env; set +a
export CRAWL_REQUEST_DELAY_SECONDS=3.5
exec docker compose -f docker-compose.yml -f stream.override.yml -f observability/docker-compose.logging.yml \
  --profile crawl --profile ingest --profile speed --profile batch --profile ops --profile serve "$@"
