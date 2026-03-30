param(
    [string]$ApiPort = "8000"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$backend = Join-Path $repoRoot "backend"
$frontend = Join-Path $repoRoot "frontend"
$runner = Join-Path $repoRoot "scripts\althea-local.ps1"

if (!(Test-Path $runner)) {
    Write-Error "Missing runner script at $runner"
    exit 1
}

Write-Host "Starting ALTHEA local stack in three terminals..."

Start-Process powershell -ArgumentList "-NoExit", "-File", $runner, "-Role", "api", "-Port", $ApiPort
Start-Sleep -Milliseconds 700
Start-Process powershell -ArgumentList "-NoExit", "-File", $runner, "-Role", "worker"
Start-Sleep -Milliseconds 700
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$frontend'; npm run dev"

Write-Host "Started: backend API, pipeline worker, frontend"
