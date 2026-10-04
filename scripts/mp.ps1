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
#   .\scripts\mp.ps1 drill d1..d11|all              (after a passing smoke)
#       d11 takes the stack down and restores into a second project; it runs
#       last in `drill all` and rebuilds the stack before it returns.
#   .\scripts\mp.ps1 batch -AsOf 2026-10-02T00:00:00Z [-AllowBackfill] [-QualityOnly]
#   .\scripts\mp.ps1 backup
#   .\scripts\mp.ps1 restore -BackupId bk-... -Project mp-restore
#
# -EnvFile env/<stack>.env loads that stack's variables for this one call
# (project name, container prefix, ports) and restores the caller's
# environment afterwards. Phase 9 plan section 5:
#   env/live.env    live collection; smoke, drill and `down -Volumes` refuse it
#   env/bench.env   benchmarks (`bench crawl|ingest|speed|batch|all|report`),
#                   smoke and drills, beside the live stack
#   env/demo.env    the offline demo
# ============================================================

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("up", "down", "status", "migrate", "seed", "smoke", "validate", "batch", "drill",
                 "backup", "restore", "bench", "evaluate")]
    [string]$Command,
    [string[]]$With = @("crawl", "ingest", "speed", "batch"),
    [switch]$Volumes,
    [int]$TimeoutSeconds = 900,
    [string]$Json,
    [string]$AsOf,
    [switch]$AllowBackfill,
    [switch]$QualityOnly,
    [string]$BackupId,
    [string]$Project,
    [string]$EnvFile,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

# Not "Stop": Windows PowerShell 5.1 turns a native command's stderr into an
# error record once output is redirected, and docker reports progress on
# stderr. Every docker call checks $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
# Popped before the single exit at the end: the caller's shell keeps its directory.
Push-Location $ProjectRoot

# The live collection stack (Phase 9 plan section 4, decision D1).
$LiveProject = "mp-live"

# A script run as .\scripts\mp.ps1 shares the caller's process, so every
# variable set here is put back in the final `finally`.
$SavedEnv = @{}
if ($EnvFile) {
    $envPath = if ([System.IO.Path]::IsPathRooted($EnvFile)) { $EnvFile } else { Join-Path $ProjectRoot $EnvFile }
    if (-not (Test-Path $envPath)) { Write-Host "no such env file: $EnvFile" -ForegroundColor Red; Pop-Location; exit 1 }
    foreach ($line in Get-Content $envPath) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
            $SavedEnv[$Matches[1]] = [Environment]::GetEnvironmentVariable($Matches[1], "Process")
            [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2].Trim(), "Process")
        }
    }
}

function Get-CurrentProject {
    if ($env:COMPOSE_PROJECT_NAME) { return $env:COMPOSE_PROJECT_NAME }
    return (Split-Path -Leaf $ProjectRoot).ToLower()
}

# Refuses a command that injects faults or stub traffic, or deletes data,
# when it would act on the live stack: by the project this call targets, and
# by the project label of the containers it would actually reach -- without
# -EnvFile, an unprefixed `kafka` is the live stack's.
function Assert-NotLive([string]$What) {
    $project = Get-CurrentProject
    $kafka = "$($env:MP_CONTAINER_PREFIX)kafka"
    # Labels as JSON: Windows PowerShell 5.1 strips the inner double quotes of
    # a native argument, so an `index ... "com.docker.compose.project"`
    # template never reaches docker intact.
    $owner = ""
    $labels = & docker inspect -f '{{json .Config.Labels}}' $kafka 2>$null
    if ($LASTEXITCODE -eq 0 -and $labels) {
        $owner = (($labels -join "") | ConvertFrom-Json)."com.docker.compose.project"
    }
    if ($project -eq $LiveProject -or "$owner".Trim() -eq $LiveProject) {
        throw [System.InvalidOperationException]::new(
            "refusing '$What': it would act on '$LiveProject', the live collection stack. Use -EnvFile env/bench.env")
    }
}

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

function Invoke-Smoke {
    # 1. Every marketplace profile plus the stub. The crawler reads the stub
    #    and recrawls the ACTIVE smoke tasks every minute; the batch judges a
    #    window minutes old, so it settles for one minute instead of 15. These
    #    are process variables: they are restored in `finally`, or a later
    #    command in the same shell would inherit them.
    $smokeEnv = [ordered]@{
        TIKI_LISTING_URL = "http://stub-source:8000/api/personalish/v1/blocks/listings"
        CRAWL_ACTIVE_CADENCE_MINUTES = "1"
        MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS = "60"
    }
    $saved = @{}
    foreach ($name in $smokeEnv.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
        [Environment]::SetEnvironmentVariable($name, $smokeEnv[$name], "Process")
    }
    $seeded = $false
    try {
        New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data\ops") | Out-Null
        Invoke-Docker ($Compose + (Get-ProfileArgs @("smoke", "crawl", "ingest", "speed", "batch", "ops")) +
            @("up", "-d", "--build", "stub-source", "crawl-worker", "silver-sink", "speed", "batch-scheduler"))
        # 2. Migrate, then seed the fixed smoke universe (re-enabling parked tasks).
        if ((Invoke-Ops @("migrate")) -ne 0) { Write-Host "SMOKE FAILED: migrate" -ForegroundColor Red; return 1 }
        $seeded = $true
        if ((Invoke-Ops @("seed-smoke")) -ne 0) { Write-Host "SMOKE FAILED: seed" -ForegroundColor Red; return 1 }
        # 3. Wait for crawl -> Bronze -> Kafka -> Silver -> speed -> ES/Redis.
        if ((Invoke-Ops @("smoke-wait", "--timeout", "$TimeoutSeconds")) -ne 0) {
            Write-Host "SMOKE FAILED: the streaming slice did not complete" -ForegroundColor Red
            return 1
        }
        # 4. A quiet period: stop the crawler and let the Silver sink catch up,
        #    or validate's lag check would measure a stream still flowing.
        Invoke-Docker ($Compose + @("--profile", "crawl", "stop", "crawl-worker"))
        if ((Invoke-Ops @("wait-quiet", "--timeout", "180")) -ne 0) {
            Write-Host "SMOKE FAILED: the Silver sink did not catch up" -ForegroundColor Red
            return 1
        }
        # 5. One batch over the window just crawled: Silver -> Gold -> quality -> PostgreSQL -> pointer.
        $plan = (Get-OpsOutput @("python", "-m", "ops", "batch-plan", "--after-last-crawl")) | ConvertFrom-Json
        if ((Invoke-Batch $plan.run_id $plan.as_of @()) -ne 0) {
            Write-Host "SMOKE FAILED: batch $($plan.run_id)" -ForegroundColor Red
            return 1
        }
        # 6. The smoke passes only if validate does.
        $code = Invoke-Ops @("validate", "--json", "/reports/smoke-validate.json")
        if ($code -eq 0) { Write-Host "SMOKE PASSED" -ForegroundColor Green } else { Write-Host "SMOKE FAILED: validate" -ForegroundColor Red }
        return $code
    } finally {
        # The smoke's categories are made up: never leave them due, or a later
        # `mp up` with the real TIKI_LISTING_URL would crawl the live site.
        if ($seeded) {
            & docker @($Compose + @("--profile", "crawl", "stop", "crawl-worker")) | Write-Host
            if ((Invoke-Ops @("park-smoke")) -ne 0) {
                Write-Host "WARNING: could not park the smoke tasks; run: .\scripts\mp.ps1 status, then python -m ops park-smoke" -ForegroundColor Yellow
            }
        }
        foreach ($name in $smokeEnv.Keys) {
            [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
        }
    }
}

function Invoke-Restore([string]$Id, [string]$Target) {
    # Plan 12.3: restore into a SEPARATE project with fresh volumes, never
    # over the running stack. The project name is the only thing keeping the
    # two apart, so it is required and checked rather than defaulted.
    if (-not $Id) { throw "restore needs -BackupId <bk-...>" }
    if (-not $Target) { throw "restore needs -Project <name>, a project that is not the running one" }
    $current = $env:COMPOSE_PROJECT_NAME
    if (-not $current) { $current = (Split-Path -Leaf $ProjectRoot) }
    if ($Target -eq $current) {
        throw "refusing to restore over the running project '$current'; pass a different -Project"
    }
    # container_name is global, so the two projects cannot both be up.
    $running = & docker @($Compose + @("--profile", "*", "ps", "-q"))
    if ($LASTEXITCODE -eq 0 -and $running) {
        throw "the '$current' stack is up; `mp down` it first, or its fixed container names collide with '$Target'"
    }

    $restoreCompose = @("compose", "-f", "docker-compose.yml", "-p", $Target)
    $restoreOps = $restoreCompose + @("--profile", "ops", "run", "--rm", "--no-deps", "ops")
    try {
        Invoke-Docker ($restoreCompose + @("up", "-d"))
        # The core's healthchecks gate everything else; `up` already waited on
        # the init containers, so this only needs PostgreSQL to answer.
        & docker @($restoreOps + @("python", "-m", "ops", "migrate")) | Write-Host
        if ($LASTEXITCODE -ne 0) { Write-Host "RESTORE FAILED: migrate" -ForegroundColor Red; return 1 }
        # Keep the output: its last line names the window the quality-only
        # batch must judge, so the host never has to find the backup manifest
        # on disk and guess where the backup root is mounted.
        $restoreOut = & docker @($restoreOps + @("python", "-m", "ops", "restore", "--from", $Id))
        $restoreOut | Write-Host
        if ($LASTEXITCODE -ne 0) { Write-Host "RESTORE FAILED: the restore checks did not pass" -ForegroundColor Red; return 1 }
        $planLine = $restoreOut | Where-Object { $_ -match '"event": "quality_only_batch_next"' } | Select-Object -Last 1
        if (-not $planLine) { Write-Host "RESTORE FAILED: the restore printed no batch window" -ForegroundColor Red; return 1 }
        $plan = $planLine | ConvertFrom-Json

        # The fourth check of plan 12.3 needs Spark, so it runs here, as every
        # other Spark job does: a quality-only batch at the pointer's as_of,
        # under a new run id, over the restored Silver and audit.
        $runId = "$($plan.pointer_run_id)-restorecheck"
        $batch = $restoreCompose + @("--profile", "batch", "run", "--rm", "--no-deps", "batch-once",
            "python3", "-m", "batch_layer.marketplace_warehouse", "--run-id", $runId, "--as-of", $plan.as_of, "--quality-only")
        & docker @($batch) | Write-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "RESTORE FAILED: the quality-only batch $runId did not succeed" -ForegroundColor Red
            return 1
        }

        # --restored tolerates exactly the checks that must fail here: Kafka,
        # Elasticsearch and Redis are not backed up (plan 12.1), so a stack
        # that has not crawled yet has all three empty and its broker has
        # never seen the Silver consumer group. Anything else is real.
        & docker @($restoreOps + @("python", "-m", "ops", "validate", "--restored")) | Write-Host
        if ($LASTEXITCODE -ne 0) { Write-Host "RESTORE FAILED: validate" -ForegroundColor Red; return 1 }
        Write-Host "RESTORE PASSED into project '$Target'" -ForegroundColor Green
        Write-Host "Kafka, Elasticsearch and Redis are empty by design; they refill from new crawls." -ForegroundColor Yellow
        return 0
    } finally {
        Write-Host "The restored stack is still up as project '$Target'. Remove it with:" -ForegroundColor Yellow
        Write-Host "  docker compose -f docker-compose.yml -p $Target --profile * down --volumes" -ForegroundColor Yellow
    }
}

$ExitCode = 0
try {
    switch ($Command) {
        "up" {
            Invoke-Docker ($Compose + (Get-ProfileArgs $With) + @("up", "-d", "--build"))
        }
        "down" {
            if ($Volumes) {
                Assert-NotLive "down -Volumes"
                $answer = Read-Host "Delete every named volume (Kafka, MinIO, PostgreSQL, Elasticsearch, Redis, checkpoints)? Type 'yes'"
                if ($answer -ne "yes") { Write-Host "Aborted."; $ExitCode = 1 }
                else { Invoke-Docker ($Compose + @("--profile", "*", "down", "--volumes")) }
            } else {
                Invoke-Docker ($Compose + @("--profile", "*", "down"))
            }
        }
        "status" {
            Invoke-Docker ($Compose + @("--profile", "*", "ps", "--format", "table {{.Name}}`t{{.Status}}"))
            $ExitCode = Invoke-Ops @("status")
        }
        "migrate" { $ExitCode = Invoke-Ops @("migrate") }
        "seed" { $ExitCode = Invoke-Ops (@("seed") + $Rest) }
        "validate" {
            if ($Json) {
                $report = Join-Path $ProjectRoot "data\ops\validate.json"
                New-Item -ItemType Directory -Force -Path (Split-Path $report) | Out-Null
                # Never hand back an earlier run's report as this one's.
                Remove-Item $report -Force -ErrorAction SilentlyContinue
                $ExitCode = Invoke-Ops @("validate", "--json", "/reports/validate.json")
                if (Test-Path $report) { Copy-Item $report $Json -Force }
                else { Write-Host "validate wrote no report (exit=$ExitCode)" -ForegroundColor Red; if ($ExitCode -eq 0) { $ExitCode = 1 } }
            } else {
                $ExitCode = Invoke-Ops @("validate")
            }
        }
        "batch" {
            if (-not $AsOf) { throw "batch needs -AsOf <ISO instant>" }
            $runId = Get-OpsOutput @("python", "-c",
                "import sys; from datetime import datetime; from batch_layer.marketplace_scheduler import run_id_for; print(run_id_for(datetime.fromisoformat(sys.argv[1].replace('Z','+00:00'))))",
                $AsOf)
            $extra = @()
            if ($AllowBackfill) { $extra += "--allow-backfill" }
            if ($QualityOnly) { $extra += "--quality-only" }
            $ExitCode = Invoke-Batch $runId $AsOf $extra
        }
        "smoke" { Assert-NotLive "smoke"; $ExitCode = Invoke-Smoke }
        "backup" {
            New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data/ops/backups") | Out-Null
            $ExitCode = Invoke-Ops @("backup")
        }
        "restore" { $ExitCode = Invoke-Restore $BackupId $Project }
        "evaluate" {
            # Phase 9 plan section 9: read-only, meant for the live stack. The
            # frozen universe comes from the env file, so stub traffic in the
            # audit is refused rather than evaluated.
            New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data\ops\evaluation") | Out-Null
            $ExitCode = Invoke-Ops (@("evaluate") + $Rest + @("--universe", "$env:TIKI_CATEGORIES"))
        }
        "bench" {
            # Phase 9 plan section 8. Like the drills: on the host, through the
            # isolated project's published ports, with the lake on MinIO.
            Assert-NotLive "bench"
            $python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
            if (-not (Test-Path $python)) { $python = "python" }
            $savedProfile = $env:DATA_LAKE_PROFILE
            $env:DATA_LAKE_PROFILE = "minio"
            try {
                & $python -m ops.bench @Rest | Write-Host
                $ExitCode = $LASTEXITCODE
            } finally {
                $env:DATA_LAKE_PROFILE = $savedProfile
            }
        }
        "drill" {
            # Drills drive Docker, so they run on the host, against the
            # published ports from .env, with the lake on MinIO.
            Assert-NotLive "drill"
            $name = if ($Rest.Count -gt 0) { $Rest[0] } else { "all" }
            $python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
            if (-not (Test-Path $python)) { $python = "python" }
            $savedProfile = $env:DATA_LAKE_PROFILE
            $env:DATA_LAKE_PROFILE = "minio"
            try {
                & $python -m ops.drills $name | Write-Host
                $ExitCode = $LASTEXITCODE
            } finally {
                $env:DATA_LAKE_PROFILE = $savedProfile
            }
        }
    }
} catch [System.InvalidOperationException] {
    Write-Host $_ -ForegroundColor Red
    $ExitCode = 2
} catch {
    Write-Host $_ -ForegroundColor Red
    $ExitCode = 1
} finally {
    foreach ($name in $SavedEnv.Keys) {
        [Environment]::SetEnvironmentVariable($name, $SavedEnv[$name], "Process")
    }
    Pop-Location
}
exit $ExitCode
