param(
    [string]$RepoRoot = "C:\code\kalshi-v3"
)

$ErrorActionPreference = "Stop"

Push-Location $RepoRoot
try {
    docker compose up -d --build
    docker compose ps
}
finally {
    Pop-Location
}

