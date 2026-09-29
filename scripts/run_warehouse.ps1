param(
    [string]$Source = ".\\tests\\fixtures\\events.csv",
    [switch]$SkipPostgres,
    [switch]$Build,
    [switch]$Marketplace,
    [string]$RunId,
    [string]$AsOf,
    [string]$SilverUri
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
if ($Marketplace) {
    if (-not $RunId -or -not $AsOf) { throw "Marketplace mode requires -RunId and -AsOf" }
    $arguments = @("-m", "batch_layer.marketplace_warehouse", "--run-id", $RunId, "--as-of", $AsOf)
    if ($SilverUri) { $arguments += @("--silver-uri", $SilverUri) }
}
if ($SkipPostgres) { $arguments += "--skip-postgres" }

if ($Marketplace) {
    docker compose --profile jobs run --rm warehouse-job python @arguments
} else {
    docker compose --profile jobs run --rm warehouse-job @arguments
}
if ($LASTEXITCODE -ne 0) { throw "Warehouse job failed" }
