# Waits for this morning's scheduled Claude jobs, stops the stacks cleanly
# (containers and volumes are kept), quits Docker Desktop and shuts Windows down.
param([datetime]$Deadline = [datetime]"2026-10-05 11:00")

$here = $PSScriptRoot
$root = Split-Path $here
$logs = Join-Path $here "logs"
New-Item -ItemType Directory -Force $logs | Out-Null
$log = Join-Path $logs ("{0}-shutdown.log" -f (Get-Date -Format "yyyyMMdd-HHmm"))
function say($m) { "$(Get-Date -Format o) $m" | Out-File $log -Append -Encoding utf8 }

# 1. Wait until neither report job is running (both have already fired by 08:50).
while ((Get-Date) -lt $Deadline) {
    $busy = Get-ScheduledTask -TaskName "mp-bench-check", "mp-daily-report" | Where-Object State -eq "Running"
    if (-not $busy) { break }
    say "waiting for: $($busy.TaskName -join ', ')"
    Start-Sleep -Seconds 120
}
say "report jobs done or deadline reached"

# 2. A benchmark still running is abandoned; its stack is throwaway.
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*ops.bench*" } |
    ForEach-Object { say "killing bench pid $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# 3. Stop every compose container gracefully. stop, not down: volumes and containers stay.
foreach ($project in "mp-bench", "mp-live") {
    $ids = docker ps -q --filter "label=com.docker.compose.project=$project"
    if ($ids) { say "stopping $project"; docker stop -t 60 $ids 2>&1 | Out-File $log -Append -Encoding utf8 }
}

# 4. The collection is over for now: do not keep the machine awake or report on a stopped stack.
Disable-ScheduledTask -TaskName "mp-daily-report", "mp-keep-awake" | Out-Null
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*keep-awake.ps1*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# 5. Leave a note in today's report so it shows on GitHub.
$report = Join-Path $root ("reports\{0}.md" -f (Get-Date -Format "yyyy-MM-dd"))
Add-Content $report -Encoding UTF8 -Value "`n## Tắt máy ($(Get-Date -Format 'HH:mm'))`n`nĐã dừng gọn mp-live và mp-bench (container và volume vẫn giữ), thoát Docker Desktop, rồi tắt máy theo yêu cầu. Đợt thu thập dừng ở đây. Tác vụ mp-daily-report và mp-keep-awake đã bị tắt.`n"
git -C $root add reports 2>&1 | Out-File $log -Append -Encoding utf8
git -C $root commit -m "report: machine shut down after the morning report" 2>&1 | Out-File $log -Append -Encoding utf8
git -C $root pull --rebase origin daily-reports 2>&1 | Out-File $log -Append -Encoding utf8
git -C $root push origin daily-reports 2>&1 | Out-File $log -Append -Encoding utf8

# 6. Docker Desktop, then Windows.
docker desktop stop 2>&1 | Out-File $log -Append -Encoding utf8
if ($LASTEXITCODE -ne 0) { Stop-Process -Name "Docker Desktop" -Force -ErrorAction SilentlyContinue }
wsl --shutdown 2>&1 | Out-File $log -Append -Encoding utf8
say "shutting down"
shutdown /s /t 60 /c "Tự tắt máy sau báo cáo sáng (huỷ: shutdown /a)"
