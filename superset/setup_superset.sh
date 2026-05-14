#!/bin/bash
set -e

echo "[Superset Setup] Waiting for PostgreSQL..."
until pip install psycopg2-binary > /dev/null && python -c "import psycopg2; psycopg2.connect(host='postgres-dw', port=5432, user='admin', password='password', dbname='data_warehouse')" 2>/dev/null; do
  echo "Postgres is unavailable - sleeping"
  sleep 5
done
echo "Postgres is up - executing command"

echo "[Superset Setup] Upgrading DB..."
superset db upgrade

echo "[Superset Setup] Creating Admin user..."
superset fab create-admin \
  --username admin \
  --firstname Data \
  --lastname Admin \
  --email admin@ecommerce.local \
  --password admin

echo "[Superset Setup] Initializing roles and permissions..."
superset init

echo "[Superset Setup] Importing Postgres connection and datasets..."
superset import_datasources -p /app/superset/datasources.yaml

echo "[Superset Setup] Setup complete! You can now access Apache Superset at http://localhost:8088"
