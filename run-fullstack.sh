#!/bin/sh
# Full Lambda stack with log observability. Usage: sh run-fullstack.sh [compose args...]
cd "$(dirname "$0")"
set -a; . env/base.env; . env/ports.tiki-html.env; . env/tiki-html.env; set +a
export CRAWL_REQUEST_DELAY_SECONDS=3.5
exec docker compose -f compose/docker-compose.yml -f compose/docker-compose.observability.yml \
  --profile crawl --profile speed --profile batch --profile ops --profile serve "$@"
