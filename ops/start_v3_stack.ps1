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

$logDir = Join-Path $RepoRoot "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

foreach ($service in $services) {
    Get-Process -Name $service -ErrorAction SilentlyContinue | Stop-Process -Force
}

foreach ($service in $services) {
    $stdout = Join-Path $logDir "$service.out.log"
    $stderr = Join-Path $logDir "$service.err.log"
    if (Test-Path $stdout) { Remove-Item $stdout -Force }
    if (Test-Path $stderr) { Remove-Item $stderr -Force }

    Start-Process `
        -FilePath (Join-Path $RepoRoot "target\debug\$service.exe") `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr | Out-Null
}

Start-Sleep -Seconds 3
Get-Process -Name $services | Select-Object ProcessName, Id | Sort-Object ProcessName
