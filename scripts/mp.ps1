# ============================================================
# mp.ps1 - one command for the marketplace stack (Phase 8 plan section 9)
# ============================================================
# A thin wrapper. Docker itself (up, down, the one-shot Spark batch) is driven
# from here; everything that reads or writes the stack runs as
# `python -m ops ...` inside the `ops` container, so it works from any shell.
#
#   .\scripts\mp.ps1 up [-With crawl,ingest,speed,batch,serve,ops]
#   .\scripts\mp.ps1 down [-Volumes]
#   .\scripts\mp.ps1 status
#   .\scripts\mp.ps1 migrate
#   .\scripts\mp.ps1 seed --category 1846 --pages 2
#   .\scripts\mp.ps1 smoke [-TimeoutSeconds 900]
#   .\scripts\mp.ps1 validate [-Json path]
#   .\scripts\mp.ps1 batch -AsOf 2026-10-02T00:00:00Z [-AllowBackfill] [-QualityOnly]
# ============================================================

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("up", "down", "status", "migrate", "seed", "smoke", "validate", "batch")]
    [string]$Command,
    [string[]]$With = @("crawl", "ingest", "speed", "batch"),
    [switch]$Volumes,
    [int]$TimeoutSeconds = 900,
    [string]$Json,
    [string]$AsOf,
    [switch]$AllowBackfill,
    [switch]$QualityOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

# Not "Stop": Windows PowerShell 5.1 turns a native command's stderr into an
# error record once output is redirected, and docker reports progress on
# stderr. Every docker call checks $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

# Explicit file: an old untracked docker-compose.override.yml must not apply.
$Compose = @("compose", "-f", "docker-compose.yml")
$OpsRun = $Compose + @("--profile", "ops", "run", "--rm", "--no-deps", "ops")

# Each helper writes docker's output with Write-Host, never to the pipeline,
# so what a function returns is only its exit code.
function Invoke-Docker([string[]]$Arguments) {
    & docker @Arguments | Write-Host
    if ($LASTEXITCODE -ne 0) { throw "docker $($Arguments -join ' ') failed (exit=$LASTEXITCODE)" }
}

function Get-ProfileArgs([string[]]$Profiles) {
    $list = @()
    foreach ($p in ($Profiles -join ",").Split(",")) {
        if ($p.Trim()) { $list += @("--profile", $p.Trim()) }
    }
    return ,$list
}

function Invoke-Ops([string[]]$Arguments) {
    & docker @($OpsRun + @("python", "-m", "ops") + $Arguments) | Write-Host
    return $LASTEXITCODE
}

function Get-OpsOutput([string[]]$Arguments) {
    $out = & docker @($OpsRun + $Arguments)
    if ($LASTEXITCODE -ne 0) { throw "ops $($Arguments -join ' ') failed (exit=$LASTEXITCODE)" }
    return ($out -join "`n").Trim()
}

function Invoke-Batch([string]$RunId, [string]$Instant, [string[]]$Extra) {
    $batch = $Compose + @("--profile", "batch", "run", "--rm", "--no-deps", "batch-once",
        "python3", "-m", "batch_layer.marketplace_warehouse", "--run-id", $RunId, "--as-of", $Instant)
    & docker @($batch + $Extra) | Write-Host
    return $LASTEXITCODE
}

switch ($Command) {
    "up" {
        Invoke-Docker ($Compose + (Get-ProfileArgs $With) + @("up", "-d", "--build"))
    }
    "down" {
        if ($Volumes) {
            $answer = Read-Host "Delete every named volume (MinIO, PostgreSQL, Elasticsearch, Redis, checkpoints)? Type 'yes'"
            if ($answer -ne "yes") { Write-Host "Aborted."; exit 1 }
            Invoke-Docker ($Compose + @("--profile", "*", "down", "--volumes"))
        } else {
            Invoke-Docker ($Compose + @("--profile", "*", "down"))
        }
    }
    "status" {
        Invoke-Docker ($Compose + @("--profile", "*", "ps", "--format", "table {{.Name}}`t{{.Status}}"))
        exit (Invoke-Ops @("status"))
    }
    "migrate" { exit (Invoke-Ops @("migrate")) }
    "seed" { exit (Invoke-Ops (@("seed") + $Rest)) }
    "validate" {
        if ($Json) {
            New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data\ops") | Out-Null
            $code = Invoke-Ops @("validate", "--json", "/reports/validate.json")
            Copy-Item (Join-Path $ProjectRoot "data\ops\validate.json") $Json -Force
            exit $code
        }
        exit (Invoke-Ops @("validate"))
    }
    "batch" {
        if (-not $AsOf) { throw "batch needs -AsOf <ISO instant>" }
        $runId = Get-OpsOutput @("python", "-c",
            "import sys; from datetime import datetime; from batch_layer.marketplace_scheduler import run_id_for; print(run_id_for(datetime.fromisoformat(sys.argv[1].replace('Z','+00:00'))))",
            $AsOf)
        $extra = @()
        if ($AllowBackfill) { $extra += "--allow-backfill" }
        if ($QualityOnly) { $extra += "--quality-only" }
        exit (Invoke-Batch $runId $AsOf $extra)
    }
    "smoke" {
        # 1. Every marketplace profile plus the stub. The crawler reads the stub
        #    and recrawls the ACTIVE smoke tasks every minute; the batch judges a
        #    window minutes old, so it settles for one minute instead of 15.
        $env:TIKI_LISTING_URL = "http://stub-source:8000/api/personalish/v1/blocks/listings"
        $env:CRAWL_ACTIVE_CADENCE_MINUTES = "1"
        $env:MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS = "60"
        New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data\ops") | Out-Null
        Invoke-Docker ($Compose + (Get-ProfileArgs @("smoke", "crawl", "ingest", "speed", "batch", "ops")) +
            @("up", "-d", "--build", "stub-source", "crawl-worker", "silver-sink", "speed", "batch-scheduler"))
        # 2. Migrate, then seed the fixed smoke universe.
        if ((Invoke-Ops @("migrate")) -ne 0) { throw "migrate failed" }
        if ((Invoke-Ops @("seed-smoke")) -ne 0) { throw "seed-smoke failed" }
        # 3. Wait for crawl -> Bronze -> Kafka -> Silver -> speed -> ES/Redis.
        if ((Invoke-Ops @("smoke-wait", "--timeout", "$TimeoutSeconds")) -ne 0) {
            Write-Host "SMOKE FAILED: the streaming slice did not complete" -ForegroundColor Red
            exit 1
        }
        # 4. A quiet period: stop the crawler and let the Silver sink catch up,
        #    or validate's lag check would measure a stream still flowing.
        Invoke-Docker ($Compose + @("--profile", "crawl", "stop", "crawl-worker"))
        if ((Invoke-Ops @("wait-quiet", "--timeout", "180")) -ne 0) {
            Write-Host "SMOKE FAILED: the Silver sink did not catch up" -ForegroundColor Red
            exit 1
        }
        # 5. One batch over the window just crawled: Silver -> Gold -> quality -> PostgreSQL -> pointer.
        $plan = (Get-OpsOutput @("python", "-m", "ops", "batch-plan", "--after-last-crawl")) | ConvertFrom-Json
        if ((Invoke-Batch $plan.run_id $plan.as_of @()) -ne 0) {
            Write-Host "SMOKE FAILED: batch $($plan.run_id)" -ForegroundColor Red
            exit 1
        }
        # 6. The smoke passes only if validate does.
        $code = Invoke-Ops @("validate", "--json", "/reports/smoke-validate.json")
        if ($code -eq 0) { Write-Host "SMOKE PASSED" -ForegroundColor Green } else { Write-Host "SMOKE FAILED: validate" -ForegroundColor Red }
        exit $code
    }
}
