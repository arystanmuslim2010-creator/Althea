# ALTHEA - Run All Services in One Terminal (as background jobs)

$AltheaPath = "c:\Users\Users\OneDrive\Рабочий стол\Althea"
$BackendPath = "$AltheaPath\backend"
$FrontendPath = "$AltheaPath\frontend"

Write-Host "`n" + ("=" * 70)
Write-Host "ALTHEA - Starting All Services"
Write-Host ("=" * 70 + "`n")

# Job 1: Backend API
Write-Host "[1] Starting Backend API..."
$backendJob = Start-Job -ScriptBlock {
    cd "$using:BackendPath"
    $env:ALTHEA_ENV="development"
    $env:ALTHEA_DEFAULT_TENANT_ID="default-bank"
    $env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"
    $env:ALTHEA_REDIS_URL="redis://localhost:6379"
    $env:ALTHEA_JWT_SECRET="your-32-character-secret-key-here-min32chars"
    $env:ALTHEA_OBJECT_STORAGE_DIR="object_storage"
    python -m uvicorn main:app --reload --port 8000
}

Start-Sleep -Seconds 2

# Job 2: Pipeline Worker
Write-Host "[2] Starting Pipeline Worker..."
$workerJob = Start-Job -ScriptBlock {
    cd "$using:BackendPath"
    $env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"
    $env:ALTHEA_REDIS_URL="redis://localhost:6379"
    python windows_worker.py
}

Start-Sleep -Seconds 2

# Job 3: Frontend
Write-Host "[3] Starting Frontend..."
$frontendJob = Start-Job -ScriptBlock {
    cd "$using:FrontendPath"
    npm run dev
}

Write-Host "`n" + ("=" * 70)
Write-Host "All services started as background jobs"
Write-Host ("=" * 70 + "`n")

Write-Host "Jobs running:"
Write-Host "  1. Backend API (Job #$($backendJob.Id))"
Write-Host "  2. Worker (Job #$($workerJob.Id))"
Write-Host "  3. Frontend (Job #$($frontendJob.Id))`n"

Write-Host "To view output from any service, use:"
Write-Host "  Receive-Job -Id <job-id> -Follow`n"

Write-Host "Example:"
Write-Host "  Receive-Job -Id $($backendJob.Id) -Follow`n"

Write-Host "To stop all services:"
Write-Host "  Stop-Job -Id $($backendJob.Id),$($workerJob.Id),$($frontendJob.Id)`n"

Write-Host "=" * 70 + "`n"

# Keep the terminal open and show job status
while ($true) {
    Clear-Host
    Write-Host "ALTHEA Services Status:`n"

    $backend = Get-Job -Id $backendJob.Id
    $worker = Get-Job -Id $workerJob.Id
    $frontend = Get-Job -Id $frontendJob.Id

    Write-Host "Backend API  ($($backend.State))" -ForegroundColor $(if ($backend.State -eq "Running") { "Green" } else { "Red" })
    Write-Host "Worker       ($($worker.State))" -ForegroundColor $(if ($worker.State -eq "Running") { "Green" } else { "Red" })
    Write-Host "Frontend     ($($frontend.State))" -ForegroundColor $(if ($frontend.State -eq "Running") { "Green" } else { "Red" })

    Write-Host "`nOpen browser to: http://localhost:5173"
    Write-Host "Login: analyst@bank.com / Password123!`n"

    Write-Host "Commands:"
    Write-Host "  Backend:  Receive-Job -Id $($backendJob.Id) -Follow"
    Write-Host "  Worker:   Receive-Job -Id $($workerJob.Id) -Follow"
    Write-Host "  Frontend: Receive-Job -Id $($frontendJob.Id) -Follow"
    Write-Host "`nPress Ctrl+C to stop all services`n"

    Start-Sleep -Seconds 5
}
