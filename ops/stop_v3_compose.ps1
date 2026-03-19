param(
    [string]$RepoRoot = "C:\code\kalshi-v3"
)

$ErrorActionPreference = "Stop"

Push-Location $RepoRoot
try {
    docker compose down
}
finally {
    Pop-Location
}

