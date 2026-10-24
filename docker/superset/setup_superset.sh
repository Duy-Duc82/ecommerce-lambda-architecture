#!/bin/bash
set -e

echo "[Superset Setup] Waiting for PostgreSQL..."
until pip install psycopg2-binary > /dev/null && python -c "import psycopg2; psycopg2.connect(host='postgres-dw', port=5432, user='admin', password='admin123', dbname='data_warehouse')" 2>/dev/null; do
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
if [ -f /app/custom_config/datasources.yaml ]; then
  superset import_datasources -p /app/custom_config/datasources.yaml || \
    echo "[Superset Setup] Datasource import skipped due to validation/runtime issue."
else
  echo "[Superset Setup] datasources.yaml not found at /app/custom_config/datasources.yaml"
fi

echo "[Superset Setup] Syncing dataset columns/metrics from source..."
superset shell -c "
from superset import db
from superset.connectors.sqla.models import SqlaTable
for t in db.session.query(SqlaTable).all():
    try:
        t.fetch_metadata()
        print(f'  [OK] synced columns for {t.schema}.{t.table_name}')
    except Exception as e:
        print(f'  [WARN] failed to sync {t.schema}.{t.table_name}: {e}')
db.session.commit()
" || echo "[Superset Setup] Column sync skipped due to runtime issue."

echo "[Superset Setup] Curating UI for report mode..."
if [ -f /app/custom_config/curate_ui.py ]; then
  python /app/custom_config/curate_ui.py || \
    echo "[Superset Setup] UI curation skipped due to runtime issue."
else
  echo "[Superset Setup] curate_ui.py not found at /app/custom_config/curate_ui.py"
fi

echo "[Superset Setup] Setup complete! You can now access Apache Superset at http://localhost:8088"
