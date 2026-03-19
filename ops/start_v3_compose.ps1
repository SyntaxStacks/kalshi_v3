param(
    [string]$RepoRoot = "C:\code\kalshi-v3"
)

$ErrorActionPreference = "Stop"

Push-Location $RepoRoot
try {
    docker compose build api
    docker compose up -d
    docker compose ps
}
finally {
    Pop-Location
}
