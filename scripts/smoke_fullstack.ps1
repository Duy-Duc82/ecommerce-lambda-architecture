# One-command full-stack smoke test for Lambda Architecture project.
# Usage:
#   .\scripts\smoke_fullstack.ps1
#   .\scripts\smoke_fullstack.ps1 -ProducerCount 500 -ProducerEps 400

param(
    [int]$ProducerCount = 300,
    [int]$ProducerEps = 300,
    [string]$SampleSource = ".\tests\fixtures\events.csv",
    [switch]$SkipRealtime,
    [switch]$SkipBatch
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

function Run-Cmd {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][scriptblock]$Script
    )
    Write-Host "`n==> $Title" -ForegroundColor Cyan
    & $Script
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Title (exit=$LASTEXITCODE)"
    }
}

function New-RealtimeContainers {
    docker rm -f es-indexer speed-layer 2>$null | Out-Null
    docker run -d --name es-indexer --network data_bigdata `
      -v C:/code/data:/app -w /app `
      -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 `
      -e ES_HOST=http://elasticsearch:9200 `
      python:3.12-slim /bin/sh -lc "pip install -r requirements.txt -q && python -m data_ingestion.es_indexer" | Out-Null

    docker run -d --name speed-layer --network data_bigdata `
      -v C:/code/data:/app -w /app `
      -e HOME=/tmp `
      -e _JAVA_OPTIONS=-Duser.home=/tmp `
      -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 `
      -e REDIS_HOST=redis `
      apache/spark:3.5.1 /bin/bash -lc "pip install -r /app/requirements.txt -q && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 speed_layer/speed_layer.py" | Out-Null
}

function Assert-Service {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Url
    )
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 15
        Write-Host "  [OK] $Name => $($resp.StatusCode)" -ForegroundColor Green
    } catch {
        Write-Host "  [FAIL] $Name => $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

Run-Cmd -Title "Start Docker stack" -Script { docker compose up -d }

Run-Cmd -Title "Create Kafka topics" -Script {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_events --partitions 3 --replication-factor 1 --if-not-exists | Out-Null
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_orders --partitions 3 --replication-factor 1 --if-not-exists | Out-Null
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic ecommerce_prices --partitions 1 --replication-factor 1 --if-not-exists | Out-Null
}

if (-not $SkipRealtime) {
    Run-Cmd -Title "Start realtime consumers (es-indexer + speed-layer)" -Script { New-RealtimeContainers }
    Write-Host "  Waiting for realtime services to warm up..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 20

    Run-Cmd -Title "Produce test events to Kafka" -Script {
        & $Python -m data_ingestion.producer --eps $ProducerEps -n $ProducerCount
    }
    Write-Host "  Waiting for realtime processing..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 20
}

if (-not $SkipBatch) {
    Run-Cmd -Title "Warehouse V2 EtLT" -Script {
        & (Join-Path $ProjectRoot "scripts\run_warehouse.ps1") -Source $SampleSource
    }
}

Write-Host "`n==> Validate endpoints" -ForegroundColor Cyan
Assert-Service -Name "Elasticsearch" -Url "http://localhost:9200"
Assert-Service -Name "Kibana" -Url "http://localhost:5601/api/status"
Assert-Service -Name "Superset" -Url "http://localhost:8088/health"

Write-Host "`n==> Validate data counts" -ForegroundColor Cyan
$esCount = Invoke-RestMethod -Method Get -Uri "http://localhost:9200/ecommerce-events/_count"
Write-Host "  [INFO] ecommerce-events count: $($esCount.count)"

docker exec postgres-dw psql -U admin -d data_warehouse -At -c @"
select 'cache.funnel_daily', count(*) from cache.funnel_daily
union all select 'cache.product_daily', count(*) from cache.product_daily
union all select 'cache.category_daily', count(*) from cache.category_daily
union all select 'cache.session_daily', count(*) from cache.session_daily
union all select 'cache.daily_revenue', count(*) from cache.daily_revenue
union all select 'cache.predictions', count(*) from cache.predictions
union all select 'cache.anomalies', count(*) from cache.anomalies;
"@

Write-Host "`nSmoke test completed." -ForegroundColor Green
