param(
    [string]$RepoRoot = "C:\code\kalshi-v3"
)

$ErrorActionPreference = "Stop"

$services = @(
    "api",
    "ingest",
    "feature",
    "decision",
    "execution",
    "training",
    "notifier"
)

foreach ($service in $services) {
    Get-Process -Name $service -ErrorAction SilentlyContinue | Stop-Process -Force
}

Get-Process -Name $services -ErrorAction SilentlyContinue | Select-Object ProcessName, Id | Sort-Object ProcessName
