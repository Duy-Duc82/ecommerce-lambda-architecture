# Runs one headless Claude Code job from Windows Task Scheduler.
# Usage: run-claude.ps1 -Prompt daily.md
param([Parameter(Mandatory)][string]$Prompt)

$utf8 = New-Object System.Text.UTF8Encoding $false
$OutputEncoding = $utf8
[Console]::OutputEncoding = $utf8

$here = $PSScriptRoot
$logs = Join-Path $here "logs"
New-Item -ItemType Directory -Force $logs | Out-Null
$log = Join-Path $logs ("{0}-{1}.log" -f (Get-Date -Format "yyyyMMdd-HHmm"), [IO.Path]::GetFileNameWithoutExtension($Prompt))

$common = Get-Content (Join-Path $here "prompts\_common.md") -Raw -Encoding UTF8
$task = Get-Content (Join-Path $here "prompts\$Prompt") -Raw -Encoding UTF8
$text = "$common`n`n$task`n`nCurrent local time: $(Get-Date -Format 'yyyy-MM-dd HH:mm') (UTC+7)."

Set-Location "C:\code\data"
"=== start $(Get-Date -Format o) prompt=$Prompt" | Out-File $log -Encoding utf8
$text | & "C:\Users\Admin\.local\bin\claude.exe" -p --dangerously-skip-permissions 2>&1 | Out-File $log -Append -Encoding utf8
"=== exit=$LASTEXITCODE finished=$(Get-Date -Format o)" | Out-File $log -Append -Encoding utf8
