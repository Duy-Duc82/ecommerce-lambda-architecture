# ============================================================
# h7.ps1 - the H7 end-to-end check (docs/archive/PLAN_PIPELINE_HARDENING.md, section 7)
# ============================================================
# One command, on the machine that has the refactor stack's Bronze:
#
#   .\scripts\h7.ps1                       # steps 1-2 of the plan, about 30-60 min
#   .\scripts\h7.ps1 -CrawlHours 3         # and step 3: crawl 3 h, batch it, backfill an old day
#   .\scripts\h7.ps1 -SkipBuild -SkipAirflowTests
#
# What it does, in order (it stops at the first step that fails):
#   0. preflight: Docker answers, enough free disk, not the live stack
#   1. build every image (the source is baked into them), start the core
#   2. ops migrate, ops audit-export (the old crawl audit into the lake)
#   3. batch A  --rebuild           over the window after the last crawl
#      (the old lake was Parquet and dim_seller/dim_brand changed columns:
#      their Delta history shows the replace)
#   4. batch B  the same window, a new run id: a windowed MERGE over
#      what A wrote; no star table may lose rows
#   5. batch C  as_of -OldDays days earlier: no star table may lose rows,
#      and C must not become the serving version
#   6. batch C again with --force: the same MERGE twice changes no count
#      (this and 4 are the Delta MERGE smoke on MinIO)
#   7. tests/airflow inside ecommerce/airflow:3.3.2-r2
#   8. with -CrawlHours > 0: crawl that long, stop the crawler, wait until
#      no task is leased, batch the new window, then backfill one old day
#
# After every batch it records the run's JSON line (status, stage_seconds,
# silver_rows, dataset_counts, quality) and a snapshot of every star table
# (rows, Delta version, last operation, from ecomdata.processing.batch.dw_inspect).
#
# Output:
#   docs/evidence/hardening-<yyyyMMdd>/summary.json  everything above
#   docs/evidence/hardening-<yyyyMMdd>/summary.md    the tables for the report
#   data/ops/h7/<stamp>/*.log                        raw docker output (not committed)
#
# The stack is stopped (not removed, volumes kept) at the end unless -KeepUp.
# ============================================================

param(
    [string]$EnvFile = "env/refactor.env",
    [double]$CrawlHours = 0,
    [string]$CrawlProfiles = "crawl,crawl-browser,speed",
    [string]$CrawlSources = "tiki,lazada",
    [int]$OldDays = 1,
    [int]$MinFreeGB = 15,
    [switch]$SkipBuild,
    [switch]$SkipAirflowTests,
    [switch]$KeepUp
)

# Not "Stop": Windows PowerShell 5.1 turns docker's stderr progress into
# error records. Every docker call checks $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Push-Location $ProjectRoot

$Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$Day = (Get-Date).ToString("yyyyMMdd")
$LogDir = Join-Path $ProjectRoot "data\ops\h7\$Stamp"
$OutDir = Join-Path $ProjectRoot "docs\evidence\hardening-$Day"
New-Item -ItemType Directory -Force -Path $LogDir, $OutDir | Out-Null

# ── environment, as scripts/mp.ps1 loads it ──────────────────
$SavedEnv = @{}
function ConvertFrom-EnvValue([string]$Raw) {
    $value = $Raw.Trim()
    if ($value -match '^"([^"]*)"' -or $value -match "^'([^']*)'") { return $Matches[1] }
    return ($value -replace '\s+#.*$', '').Trim()
}
function Set-ProcessEnv([string]$Name, [string]$Value) {
    if (-not $SavedEnv.ContainsKey($Name)) {
        $SavedEnv[$Name] = [Environment]::GetEnvironmentVariable($Name, "Process")
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}
function Import-EnvFile([string]$Path) {
    foreach ($line in Get-Content $Path) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { Set-ProcessEnv $Matches[1] (ConvertFrom-EnvValue $Matches[2]) }
    }
}

$BaseEnv = Join-Path $ProjectRoot "env/base.env"
if (Test-Path $BaseEnv) { Import-EnvFile $BaseEnv }
$envPath = Join-Path $ProjectRoot $EnvFile
if (-not (Test-Path $envPath)) { Write-Host "no such env file: $EnvFile" -ForegroundColor Red; Pop-Location; exit 1 }
$stackName = [System.IO.Path]::GetFileNameWithoutExtension($envPath)
$portsPath = Join-Path (Split-Path -Parent $envPath) "ports.$stackName.env"
if (Test-Path $portsPath) { Import-EnvFile $portsPath }
Import-EnvFile $envPath

$Compose = @("compose", "-f", "compose/docker-compose.yml")
if (Test-Path (Join-Path $ProjectRoot ".env")) { $Compose = @("compose", "--env-file", ".env", "-f", "compose/docker-compose.yml") }
$OpsRun = $Compose + @("--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ecomdata.ops")
$SparkRun = $Compose + @("--profile", "batch", "run", "--rm", "--no-deps", "batch-once", "python3", "-m")

# ── the record ───────────────────────────────────────────────
$Summary = [ordered]@{
    started_at = (Get-Date).ToUniversalTime().ToString("o")
    project = $env:COMPOSE_PROJECT_NAME
    commit = (& git rev-parse --short HEAD).Trim()
    branch = (& git rev-parse --abbrev-ref HEAD).Trim()
    options = [ordered]@{ env_file = $EnvFile; crawl_hours = $CrawlHours; old_days = $OldDays }
    steps = @()
    batches = @()
    snapshots = [ordered]@{}
    checks = @()
    result = "RUNNING"
}

function Write-Step([string]$Text) { Write-Host ""; Write-Host "== $Text" -ForegroundColor Cyan }

# Runs docker, keeps its output in data/ops/h7/<stamp>/<name>.log, returns the lines.
function Invoke-Logged([string]$Name, [string[]]$Arguments, [switch]$AllowFail) {
    $log = Join-Path $LogDir "$Name.log"
    $started = Get-Date
    $lines = @(& docker @Arguments 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $log)
    $code = $LASTEXITCODE
    $script:Summary.steps += [ordered]@{
        name = $Name; exit_code = $code; seconds = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
        log = "data/ops/h7/$Stamp/$Name.log"
    }
    if ($code -ne 0 -and -not $AllowFail) { throw "step '$Name' failed (exit=$code); see $log" }
    return ,$lines
}

# The last line of a command's output that is a JSON object holding $Key.
function Get-JsonLine([string[]]$Lines, [string]$Key) {
    $match = $Lines | Where-Object { $_.TrimStart().StartsWith("{") -and $_.Contains("`"$Key`"") } | Select-Object -Last 1
    if (-not $match) { return $null }
    return ($match | ConvertFrom-Json)
}

function Get-Snapshot([string]$Label) {
    $lines = Invoke-Logged "dw-$Label" ($SparkRun + @("ecomdata.processing.batch.dw_inspect"))
    $snap = Get-JsonLine $lines "tables"
    if (-not $snap) { throw "dw_inspect printed no snapshot ($Label)" }
    $script:Summary.snapshots[$Label] = $snap
    return $snap
}

function Add-Check([string]$Name, [bool]$Passed, [string]$Detail) {
    $script:Summary.checks += [ordered]@{ name = $Name; passed = $Passed; detail = $Detail }
    if ($Passed) { Write-Host "  PASS $Name $Detail" -ForegroundColor Green }
    else { Write-Host "  FAIL $Name $Detail" -ForegroundColor Red }
}

# No star table holds fewer rows in $After than in $Before. With -Equal, the same count.
function Test-NoLoss([string]$Name, $Before, $After, [switch]$Equal) {
    $bad = @()
    foreach ($table in $Before.tables.PSObject.Properties.Name) {
        $old = $Before.tables.$table.rows
        $new = $After.tables.$table.rows
        if ($null -eq $old) { continue }
        if ($null -eq $new) { $new = 0 }
        if ($new -lt $old -or ($Equal -and $new -ne $old)) { $bad += "$table $old->$new" }
    }
    $detail = "every star table kept its rows"
    if ($Equal) { $detail = "every star table has the same count" }
    if ($bad.Count -gt 0) { $detail = ($bad -join ", ") }
    Add-Check $Name ($bad.Count -eq 0) $detail
    return ($bad.Count -eq 0)
}

function Invoke-BatchRun([string]$Label, [string]$RunId, [string]$AsOf, [string[]]$Extra) {
    $lines = Invoke-Logged "batch-$Label" ($SparkRun + @("ecomdata.processing.batch", "run", "--run-id", $RunId, "--as-of", $AsOf, "--all") + $Extra) -AllowFail
    $result = Get-JsonLine $lines "run_id"
    $code = $script:Summary.steps[-1].exit_code
    $script:Summary.batches += [ordered]@{ label = $Label; run_id = $RunId; as_of = $AsOf; flags = ($Extra -join " "); exit_code = $code; result = $result }
    if ($code -ne 0) { throw "batch $Label ($RunId) failed (exit=$code); see data/ops/h7/$Stamp/batch-$Label.log" }
    Write-Host ("  {0}: status={1} quality={2} promoted={3} silver_rows={4}" -f $RunId, $result.status, $result.quality_status, $result.manifest_promoted, $result.silver_rows)
    return $result
}

function Get-Plan([string]$Name) {
    $plan = Get-JsonLine (Invoke-Logged $Name ($OpsRun + @("batch-plan", "--after-last-crawl"))) "as_of"
    if (-not $plan) { throw "batch-plan printed no window" }
    return $plan
}

function Shift-Days([string]$Instant, [int]$Days) {
    $at = [DateTimeOffset]::Parse($Instant, [Globalization.CultureInfo]::InvariantCulture).ToUniversalTime()
    return $at.AddDays(-$Days).ToString("yyyy-MM-ddTHH:mm:ssZ")
}

function Write-Report {
    $Summary.finished_at = (Get-Date).ToUniversalTime().ToString("o")
    $Summary | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8 (Join-Path $OutDir "summary.json")

    $md = @("# H7 end-to-end, $($Summary.started_at)", "",
        "Commit ``$($Summary.commit)`` ($($Summary.branch)), project ``$($Summary.project)``. Result: **$($Summary.result)**.", "",
        "## Batch runs", "",
        "| Run | as_of | Flags | Status | Quality | Promoted | Silver rows | Stage seconds |",
        "|---|---|---|---|---|---|---|---|")
    foreach ($b in $Summary.batches) {
        $r = $b.result
        $secs = ""
        if ($r -and $r.stage_seconds) { $secs = ($r.stage_seconds.PSObject.Properties | ForEach-Object { "$($_.Name) $($_.Value)" }) -join ", " }
        $md += "| $($b.label) ``$($b.run_id)`` | $($b.as_of) | $($b.flags) | $($r.status) | $($r.quality_status) | $($r.manifest_promoted) | $($r.silver_rows) | $secs |"
    }
    if ($Summary.snapshots.Count -gt 0) {
        $labels = @($Summary.snapshots.Keys)
        $md += @("", "## Star tables: rows (Delta version, last operation)", "", "| Table | " + ($labels -join " | ") + " |", "|---" * ($labels.Count + 1) + "|")
        $tables = $Summary.snapshots[$labels[-1]].tables.PSObject.Properties.Name
        foreach ($t in $tables) {
            $cells = foreach ($l in $labels) {
                $x = $Summary.snapshots[$l].tables.$t
                if ($null -eq $x -or $null -eq $x.rows) { "-" } else { "$($x.rows) (v$($x.version), $($x.operation))" }
            }
            $md += "| $t | " + ($cells -join " | ") + " |"
        }
    }
    $md += @("", "## Checks", "")
    foreach ($c in $Summary.checks) {
        $mark = "FAIL"
        if ($c.passed) { $mark = "PASS" }
        $md += "- **$mark** $($c.name): $($c.detail)"
    }
    $md += @("", "## Steps", "", "| Step | Exit | Seconds |", "|---|---|---|")
    foreach ($s in $Summary.steps) { $md += "| $($s.name) | $($s.exit_code) | $($s.seconds) |" }
    $md | Set-Content -Encoding utf8 (Join-Path $OutDir "summary.md")
    Write-Host ""
    Write-Host "Report: $OutDir\summary.md" -ForegroundColor Cyan
}

# ── the run ──────────────────────────────────────────────────
$ExitCode = 0
try {
    Write-Step "0. preflight"
    if ($env:COMPOSE_PROJECT_NAME -eq "mp-live") { throw "refusing to run H7 on mp-live, the live collection stack" }
    & docker info --format "{{.ServerVersion}}" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Docker does not answer; start Docker Desktop first" }
    foreach ($drive in @("C", (Split-Path -Qualifier $ProjectRoot).TrimEnd(":")) | Select-Object -Unique) {
        $free = [math]::Round((Get-PSDrive $drive).Free / 1GB, 1)
        Write-Host "  free on ${drive}: $free GB"
        if ($free -lt $MinFreeGB) { throw "only $free GB free on ${drive}: (need $MinFreeGB; a full disk crashed Docker before)" }
    }

    Write-Step "1. build and start the core"
    if (-not $SkipBuild) { Invoke-Logged "build" ($Compose + @("--profile", "*", "build")) | Out-Null }
    Invoke-Logged "up-core" ($Compose + @("up", "-d")) | Out-Null

    Write-Step "2. migrate, crawl audit into the lake"
    Invoke-Logged "migrate" ($OpsRun + @("migrate")) | Out-Null
    $export = Get-JsonLine (Invoke-Logged "audit-export" ($OpsRun + @("audit-export"))) "event"
    $Summary.audit_export = $export
    Write-Host "  exported runs=$($export.runs) attempts=$($export.attempts)"

    $plan = Get-Plan "plan"
    $asOf = $plan.as_of
    $runStamp = $Stamp.Substring(0, 13)
    $before = Get-Snapshot "0-before"

    Write-Step "3. batch A: --rebuild at $asOf"
    $a = Invoke-BatchRun "A-rebuild" "h7-rebuild-$runStamp" $asOf @("--rebuild")
    $afterA = Get-Snapshot "1-after-rebuild"
    Add-Check "rebuild_status" ($a.status -eq "PROMOTED") "status $($a.status), $($a.promotion_reason)"
    Add-Check "star_written" (@($afterA.tables.PSObject.Properties | Where-Object { $null -eq $_.Value.rows }).Count -eq 0) "every star table exists after the rebuild"

    Write-Step "4. batch B: windowed, same window"
    Invoke-BatchRun "B-window" "h7-window-$runStamp" $asOf @() | Out-Null
    $afterB = Get-Snapshot "2-after-window"
    Test-NoLoss "window_keeps_rows" $afterA $afterB | Out-Null

    $oldAsOf = Shift-Days $asOf $OldDays
    Write-Step "5. batch C: older as_of $oldAsOf"
    $c = Invoke-BatchRun "C-older" "h7-older-$runStamp" $oldAsOf @()
    $afterC = Get-Snapshot "3-after-older"
    Test-NoLoss "older_as_of_keeps_rows" $afterB $afterC | Out-Null
    Add-Check "older_not_serving" (-not $c.manifest_promoted) "promoted=$($c.manifest_promoted), $($c.promotion_reason)"

    Write-Step "6. batch C again (--force): the same MERGE twice"
    Invoke-BatchRun "C-again" "h7-older-$runStamp" $oldAsOf @("--force") | Out-Null
    $afterC2 = Get-Snapshot "4-after-older-again"
    Test-NoLoss "merge_twice_same_counts" $afterC $afterC2 -Equal | Out-Null

    if (-not $SkipAirflowTests) {
        Write-Step "7. tests/airflow in ecommerce/airflow:3.3.2-r2"
        $lines = Invoke-Logged "airflow-tests" @("run", "--rm", "-v", "${ProjectRoot}:/repo:ro", "-e", "PYTHONDONTWRITEBYTECODE=1",
            "ecommerce/airflow:3.3.2-r2", "bash", "-c", "cd /repo && python -m pytest tests/airflow -m airflow -q -p no:cacheprovider") -AllowFail
        $tail = ($lines | Where-Object { $_ -match "passed|failed|error" } | Select-Object -Last 1)
        Add-Check "airflow_dags" ($Summary.steps[-1].exit_code -eq 0) "$tail"
    }

    if ($CrawlHours -gt 0) {
        Write-Step "8. crawl $CrawlHours h ($CrawlSources), then batch it and backfill an old day"
        Set-ProcessEnv "CRAWL_SOURCES" $CrawlSources
        $profiles = @()
        foreach ($p in $CrawlProfiles.Split(",")) { if ($p.Trim()) { $profiles += @("--profile", $p.Trim()) } }
        $crawlStart = (Get-Date).ToUniversalTime()
        Invoke-Logged "up-crawl" ($Compose + $profiles + @("up", "-d")) | Out-Null
        $until = (Get-Date).AddHours($CrawlHours)
        while ((Get-Date) -lt $until) {
            $left = [math]::Round(($until - (Get-Date)).TotalMinutes)
            Write-Host "  crawling, $left min left ($((Get-Date).ToString('HH:mm')))"
            Start-Sleep -Seconds ([math]::Min(600, [math]::Max(1, ($until - (Get-Date)).TotalSeconds)))
        }
        Invoke-Logged "stop-crawl" ($Compose + @("--profile", "crawl", "--profile", "crawl-browser", "stop", "crawl-worker", "crawl-worker-lazada")) | Out-Null
        Invoke-Logged "wait-quiet" ($OpsRun + @("wait-quiet", "--timeout", "600")) | Out-Null
        $Summary.crawl = [ordered]@{ started_at = $crawlStart.ToString("o"); stopped_at = (Get-Date).ToUniversalTime().ToString("o") }

        $plan2 = Get-Plan "plan-crawl"
        $e = Invoke-BatchRun "E-crawled" "h7-crawled-$runStamp" $plan2.as_of @()
        $afterE = Get-Snapshot "5-after-crawl"
        Test-NoLoss "crawl_window_keeps_rows" $afterC2 $afterE | Out-Null
        Add-Check "crawl_window_promoted" ($e.status -eq "PROMOTED") "status $($e.status), silver_rows $($e.silver_rows)"

        $backfillAsOf = Shift-Days $plan2.as_of $OldDays
        Invoke-BatchRun "F-backfill" "h7-backfill-$runStamp" $backfillAsOf @() | Out-Null
        $afterF = Get-Snapshot "6-after-backfill"
        Test-NoLoss "backfill_keeps_rows" $afterE $afterF | Out-Null
        Invoke-Logged "stop-speed" ($Compose + @("--profile", "speed", "stop", "speed")) -AllowFail | Out-Null
    }

    $failed = @($Summary.checks | Where-Object { -not $_.passed })
    if ($failed.Count -gt 0) { $Summary.result = "FAILED_CHECKS"; $ExitCode = 1 } else { $Summary.result = "PASSED" }
} catch {
    Write-Host $_ -ForegroundColor Red
    $Summary.result = "ERROR: $_"
    $ExitCode = 1
} finally {
    if (-not $KeepUp) {
        Write-Step "stop the stack (volumes kept)"
        & docker @($Compose + @("--profile", "*", "stop")) 2>&1 | Out-Null
    }
    try { Write-Report } catch { Write-Host "could not write the report: $_" -ForegroundColor Red }
    foreach ($name in $SavedEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $SavedEnv[$name], "Process") }
    Pop-Location
}
if ($ExitCode -eq 0) { Write-Host "H7 PASSED" -ForegroundColor Green } else { Write-Host "H7: $($Summary.result)" -ForegroundColor Red }
exit $ExitCode
