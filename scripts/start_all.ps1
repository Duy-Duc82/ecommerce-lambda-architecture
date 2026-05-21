# ============================================================
# start_all.ps1 — Khoi dong toan bo he thong Big Data
# ============================================================
# Chay: .\scripts\start_all.ps1
# ============================================================

param(
    [switch]$Full,        # Bao gom Elasticsearch + Kibana
    [switch]$SkipDocker,  # Bo qua docker compose
    [switch]$ProducerOnly, # Chi chay producer
    [switch]$RunAllLayers, # Tu dong chay day du layer de test
    [switch]$UseKaggle,    # Doc du lieu that tu Kaggle CSV
    [string]$KaggleCsv,    # Duong dan CSV (mac dinh: data_kaggle/2019-Oct.csv)
    [int]$KaggleEps = 200  # Toc do gui events/giay
)

$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

$SparkSubmit = Join-Path $ProjectRoot ".venv\Scripts\spark-submit.cmd"
if (-not (Test-Path $SparkSubmit)) { $SparkSubmit = "spark-submit" }

$LogsDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

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

# ── 6. CHAY DAY DU LAYER (OPTIONAL) ─────────────────────────
if ($RunAllLayers) {
    Write-Host "`n[6/6] Khoi dong cac layer..." -ForegroundColor Yellow

    if (-not $UseKaggle) { $UseKaggle = $true }

    $datePath = Get-Date -Format "yyyy\\MM\\dd"
    $exportPath = Join-Path $ProjectRoot "data_lake_raw\events\$datePath\events.jsonl"
    $kaggleCsvPath = if ($KaggleCsv) { $KaggleCsv } else { Join-Path $ProjectRoot "data_kaggle\2019-Oct.csv" }

    Write-Host "  + ES Indexer (Kafka -> Elasticsearch)" -ForegroundColor Green
    Start-Process -FilePath $Python -ArgumentList "-m","data_ingestion.es_indexer" `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput (Join-Path $LogsDir "es_indexer.out.log") `
        -RedirectStandardError (Join-Path $LogsDir "es_indexer.err.log")

    Write-Host "  + Speed Layer (Spark Streaming)" -ForegroundColor Green
    Start-Process -FilePath $SparkSubmit -ArgumentList "--packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1","speed_layer/speed_layer.py" `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput (Join-Path $LogsDir "speed_layer.out.log") `
        -RedirectStandardError (Join-Path $LogsDir "speed_layer.err.log")

    if ($UseKaggle) {
        Write-Host "  + Kaggle Producer (Kafka + JSONL)" -ForegroundColor Green
        & $Python -m data_ingestion.kaggle_producer --csv $kaggleCsvPath --eps $KaggleEps --export-json $exportPath
    }

    Write-Host "  + Batch ETL" -ForegroundColor Green
    & $Python -m batch_layer.etl_job --source $exportPath

    Write-Host "  + Batch Views" -ForegroundColor Green
    & $Python -m batch_layer.batch_views

    Write-Host "  + Batch Models" -ForegroundColor Green
    & $Python -m batch_layer.models.trend_analysis
    & $Python -m batch_layer.models.anomaly_detection
    & $Python -m batch_layer.models.price_forecast
    & $Python -m batch_layer.models.fraud_detection
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
Write-Host "    5. Tu dong chay day du: .\scripts\start_all.ps1 -Full -RunAllLayers"
Write-Host ""
