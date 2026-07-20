param(
    [Parameter(Mandatory = $true)][string]$Source,
    [switch]$SkipPostgres,
    [switch]$Build
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$resolvedRoot = (Resolve-Path $ProjectRoot).Path.TrimEnd('\')
$resolvedSource = (Resolve-Path $Source).Path

if (-not $resolvedSource.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Source must be inside the project workspace: $resolvedRoot"
}

$relativeSource = $resolvedSource.Substring($resolvedRoot.Length).TrimStart('\').Replace('\', '/')
$containerSource = "/app/$relativeSource"

if ($Build) {
    docker compose --profile jobs build warehouse-job
    if ($LASTEXITCODE -ne 0) { throw "Failed to build warehouse-job image" }
}

$arguments = @("--source", $containerSource)
if ($SkipPostgres) { $arguments += "--skip-postgres" }

docker compose --profile jobs run --rm warehouse-job @arguments
if ($LASTEXITCODE -ne 0) { throw "Warehouse job failed" }
