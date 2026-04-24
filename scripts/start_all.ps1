# ============================================================
# start_all.ps1 — Khoi dong toan bo he thong Big Data
# ============================================================
# Chay: .\scripts\start_all.ps1
# ============================================================

param(
    [switch]$Full,        # Bao gom Elasticsearch + Kibana
    [switch]$SkipDocker,  # Bo qua docker compose
    [switch]$ProducerOnly # Chi chay producer
)

$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  BIG DATA LAMBDA ARCHITECTURE" -ForegroundColor Cyan
Write-Host "  E-Commerce Analytics Platform" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# ── 1. DOCKER COMPOSE ────────────────────────────────────────
if (-not $SkipDocker) {
    Write-Host "[1/6] Khoi dong Docker services..." -ForegroundColor Yellow

    if ($Full) {
        docker compose --profile full up -d
    } else {
        docker compose up -d
    }

    Write-Host "  Doi services san sang..." -ForegroundColor Gray
    Start-Sleep -Seconds 15

    # Kiem tra services
    Write-Host "`n  Docker services status:" -ForegroundColor Gray
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
}

# ── 2. TAO KAFKA TOPICS ─────────────────────────────────────
Write-Host "`n[2/6] Tao Kafka Topics..." -ForegroundColor Yellow

$kafkaTopics = @("ecommerce_events", "ecommerce_orders", "ecommerce_prices")
foreach ($topic in $kafkaTopics) {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --create --bootstrap-server localhost:9092 `
        --topic $topic --partitions 3 --replication-factor 1 `
        --if-not-exists 2>$null
    Write-Host "  + Topic: $topic" -ForegroundColor Green
}

# Xac nhan topics
Write-Host "`n  Danh sach topics:" -ForegroundColor Gray
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# ── 3. KIEM TRA MINIO ────────────────────────────────────────
Write-Host "`n[3/6] Kiem tra MinIO buckets..." -ForegroundColor Yellow
try {
    & .\.venv\Scripts\python.exe -c @"
from minio import Minio
c = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
for b in c.list_buckets():
    print(f'  + Bucket: {b.name}')
"@ 2>$null
} catch {
    Write-Host "  MinIO chua san sang, buckets se duoc tao boi minio-init container" -ForegroundColor Gray
}

# ── 4. KIEM TRA POSTGRES ─────────────────────────────────────
Write-Host "`n[4/6] Kiem tra Postgres Data Warehouse..." -ForegroundColor Yellow
docker exec postgres-dw psql -U admin -d data_warehouse -c "\dt" 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Postgres DW san sang!" -ForegroundColor Green
} else {
    Write-Host "  Postgres dang khoi dong..." -ForegroundColor Gray
}

# ── 5. KIEM TRA REDIS ────────────────────────────────────────
Write-Host "`n[5/6] Kiem tra Redis..." -ForegroundColor Yellow
$redisPing = docker exec redis redis-cli ping 2>$null
if ($redisPing -eq "PONG") {
    Write-Host "  Redis PONG - San sang!" -ForegroundColor Green
} else {
    Write-Host "  Redis dang khoi dong..." -ForegroundColor Gray
}

# ── 6. TONG KET ──────────────────────────────────────────────
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  HE THONG DA SAN SANG!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Services:" -ForegroundColor White
Write-Host "    Kafka:          localhost:9092"
Write-Host "    Spark Master:   http://localhost:8080"
Write-Host "    Spark Worker:   http://localhost:8081"
Write-Host "    MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)"
Write-Host "    MinIO S3 API:   http://localhost:9000"
Write-Host "    Redis:          localhost:6379"
Write-Host "    Postgres DW:    localhost:5432  (admin/password)"
if ($Full) {
    Write-Host "    Elasticsearch:  http://localhost:9200"
    Write-Host "    Kibana:         http://localhost:5601"
}
Write-Host ""
Write-Host "  Buoc tiep theo:" -ForegroundColor Yellow
Write-Host "    1. Chay producer:    .\.venv\Scripts\python.exe -m data_ingestion.producer"
Write-Host "    2. Chay speed layer: spark-submit speed_layer/speed_layer.py"
Write-Host "    3. Chay batch ETL:   .\.venv\Scripts\python.exe -m batch_layer.etl_job"
Write-Host "    4. Chay dashboard:   .\.venv\Scripts\streamlit run dashboard/app.py"
Write-Host ""
