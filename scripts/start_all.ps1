# ============================================================
# start_all.ps1 - One-command launcher for the full project
# ============================================================
# Examples:
#   .\scripts\start_all.ps1
#   .\scripts\start_all.ps1 -RunBatch -Source .\tests\fixtures\events.csv
#   .\scripts\start_all.ps1 -RunRealtime -RunProducer -ProducerCount 500
#   .\scripts\start_all.ps1 -RunEverything -Source .\data\data_kaggle\2019-Oct.csv
# ============================================================

param(
    [switch]$RunEverything,
    [switch]$RunRealtime,
    [switch]$RunProducer,
    [switch]$RunBatch,
    [switch]$RefreshBI,
    [switch]$SmokeCheck,
    [switch]$SkipDocker,
    [switch]$SkipSupersetInit,
    [switch]$BuildWarehouseImage,
    [switch]$SkipPostgresPublish,
    [string]$Source = ".\tests\fixtures\events.csv",
    [int]$ProducerCount = 300,
    [int]$ProducerEps = 300,
    [int]$WarmupSeconds = 20,

    # Backward-compatible aliases from older scripts.
    [switch]$Full,
    [switch]$RunAllLayers,
    [switch]$UseKaggle,
    [string]$KaggleCsv,
    [int]$KaggleEps = 200,
    [switch]$ProducerOnly
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

if ($Full -or $RunAllLayers) { $RunEverything = $true }
if ($ProducerOnly) {
    $SkipDocker = $true
    $RunProducer = $true
    $RunRealtime = $false
    $RunBatch = $false
    $RefreshBI = $false
}
if ($UseKaggle) {
    $Source = if ($KaggleCsv) { $KaggleCsv } else { ".\data\data_kaggle\2019-Oct.csv" }
    $ProducerEps = $KaggleEps
}
if ($RunEverything) {
    $RunRealtime = $true
    $RunProducer = $true
    $RunBatch = $true
    $RefreshBI = $true
    $SmokeCheck = $true
}

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = "python" }

$SparkSubmit = Join-Path $ProjectRoot ".venv\Scripts\spark-submit.cmd"
if (-not (Test-Path $SparkSubmit)) { $SparkSubmit = "spark-submit" }

$LogsDir = Join-Path $ProjectRoot "data\logs"
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

function Invoke-Step {
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

function Wait-Http {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSeconds = 90
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 10
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
                Write-Host "  [OK] $Name ready: $Url" -ForegroundColor Green
                return
            }
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "$Name is not ready: $Url"
}

function Start-DockerStack {
    Invoke-Step -Title "Start Docker stack" -Script {
        docker compose up -d
    }
    Start-Sleep -Seconds 10
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
}

function Initialize-KafkaTopics {
    Invoke-Step -Title "Create Kafka topics" -Script {
        $topics = @("ecommerce_events", "marketplace.observations.v1", "marketplace.observations.v1.dlq", "marketplace.changes.v1")
        foreach ($topic in $topics) {
            docker exec kafka /opt/kafka/bin/kafka-topics.sh `
                --create --bootstrap-server localhost:9092 `
                --topic $topic --partitions 3 --replication-factor 1 --if-not-exists | Out-Null
            Write-Host "  [OK] $topic" -ForegroundColor Green
        }
    }
}

function Initialize-PostgresCache {
    Invoke-Step -Title "Initialize PostgreSQL cache schema" -Script {
        Get-Content -LiteralPath (Join-Path $ProjectRoot "scripts\init_postgres.sql") |
            docker exec -i postgres-dw psql -U admin -d data_warehouse
    }
}

function Start-RealtimeLayer {
    Invoke-Step -Title "Start realtime layer" -Script {
        Start-Process -FilePath $Python -ArgumentList "-m","data_ingestion.es_indexer" `
            -WorkingDirectory $ProjectRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $LogsDir "es_indexer.out.log") `
            -RedirectStandardError (Join-Path $LogsDir "es_indexer.err.log")

        Start-Process -FilePath $SparkSubmit `
            -ArgumentList "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1","speed_layer/speed_layer.py" `
            -WorkingDirectory $ProjectRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $LogsDir "speed_layer.out.log") `
            -RedirectStandardError (Join-Path $LogsDir "speed_layer.err.log")
    }
    Write-Host "  Waiting $WarmupSeconds seconds for realtime consumers..." -ForegroundColor DarkGray
    Start-Sleep -Seconds $WarmupSeconds
}

function Start-Producer {
    Invoke-Step -Title "Run Kafka producer" -Script {
        $producerArgs = @("-m", "data_ingestion.producer", "--source", $Source, "--eps", "$ProducerEps")
        if ($ProducerCount) { $producerArgs += @("-n", "$ProducerCount") }
        & $Python @producerArgs
    }
}

function Start-BatchWarehouse {
    Invoke-Step -Title "Run batch warehouse EtLT (Spark on MinIO)" -Script {
        $whArgs = @("-Source", $Source)
        if ($BuildWarehouseImage) { $whArgs += "-Build" }
        if ($SkipPostgresPublish) { $whArgs += "-SkipPostgres" }
        & (Join-Path $ProjectRoot "scripts\run_warehouse.ps1") @whArgs
    }

    if (-not $SkipPostgresPublish) {
        Invoke-Step -Title "Run batch ML (Darts N-BEATS/LSTM + PyOD AutoEncoder)" -Script {
            & $Python -m batch_layer.ml_job
        }
        Invoke-Step -Title "Create/refresh cache convenience views" -Script {
            & $Python -m serving_layer.postgres_views create
        }
    }
}

function Refresh-SupersetBI {
    if (-not $SkipSupersetInit) {
        Invoke-Step -Title "Import Superset datasets" -Script {
            docker compose run --rm superset-init
        }
    }

    Write-Host "`n==> Create/refresh batch ML BI dashboard" -ForegroundColor Cyan
    try {
        & $Python (Join-Path $ProjectRoot "display\superset\create_batch_ml_dashboard.py")
        if ($LASTEXITCODE -ne 0) { throw "dashboard build exited $LASTEXITCODE" }
    } catch {
        Write-Host "  [WARN] Dashboard build skipped: $_" -ForegroundColor Yellow
    }
}

function Invoke-SmokeCheck {
    Invoke-Step -Title "Validate endpoints" -Script {
        Wait-Http -Name "Elasticsearch" -Url "http://localhost:9200"
        Wait-Http -Name "Kibana" -Url "http://localhost:5601/api/status"
        Wait-Http -Name "Superset" -Url "http://localhost:8088/health"
    }

    Invoke-Step -Title "Show cache table counts" -Script {
        docker exec postgres-dw psql -U admin -d data_warehouse -At -c @"
select 'cache.funnel_daily', count(*) from cache.funnel_daily
union all select 'cache.product_daily', count(*) from cache.product_daily
union all select 'cache.category_daily', count(*) from cache.category_daily
union all select 'cache.session_daily', count(*) from cache.session_daily
union all select 'cache.daily_revenue', count(*) from cache.daily_revenue
union all select 'cache.predictions', count(*) from cache.predictions
union all select 'cache.anomalies', count(*) from cache.anomalies;
"@
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  E-Commerce Lambda Architecture Runner" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project: $ProjectRoot"
Write-Host "Source:  $Source"

if (-not $SkipDocker) {
    Start-DockerStack
    Initialize-KafkaTopics
    Initialize-PostgresCache
}

if ($RunRealtime) { Start-RealtimeLayer }
if ($RunProducer) { Start-Producer }
if ($RunBatch) { Start-BatchWarehouse }
if ($RefreshBI) { Refresh-SupersetBI }
if ($SmokeCheck) { Invoke-SmokeCheck }

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  PROJECT READY" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Kafka:          localhost:9092"
Write-Host "Spark Master:   http://localhost:8080"
Write-Host "MinIO Console:  http://localhost:9001"
Write-Host "Redis:          localhost:6379"
Write-Host "Postgres:       localhost:5433"
Write-Host "Elasticsearch:  http://localhost:9200"
Write-Host "Kibana:         http://localhost:5601"
Write-Host "Superset:       http://localhost:8088"
Write-Host "Batch ML BI:    http://localhost:8088/superset/dashboard/batch-ml-forecast-anomalies/"
Write-Host ""
