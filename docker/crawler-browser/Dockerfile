# Crawl worker for sources that need a browser (Lazada). Tag: ecommerce/crawler-browser:1.
# The base ships Chromium and the Playwright package at the version
# constraints.txt pins; one worker runs one browser, one tab at a time.
FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app

COPY pyproject.toml constraints.txt README.md /build/
COPY src /build/src
RUN pip install --no-cache-dir --break-system-packages -c /build/constraints.txt "/build[browser]" \
 && rm -rf /build

WORKDIR /app
USER pwuser
