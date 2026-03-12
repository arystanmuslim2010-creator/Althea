# ALTHEA Complete Setup Script for Windows PowerShell
# Run this script to setup everything in one go

$ErrorActionPreference = "Stop"

Write-Host "`n" + ("=" * 70)
Write-Host "ALTHEA SYSTEM SETUP"
Write-Host ("=" * 70 + "`n")

# Colors
$Green = [System.ConsoleColor]::Green
$Red = [System.ConsoleColor]::Red
$Yellow = [System.ConsoleColor]::Yellow

# Check Redis
Write-Host "[1] Checking Redis..." -ForegroundColor $Yellow
try {
    $redis_test = python -c "from redis import Redis; r = Redis.from_url('redis://localhost:6379'); r.ping(); print('OK')" 2>&1
    if ($redis_test -like "*OK*") {
        Write-Host "[OK] Redis is running`n" -ForegroundColor $Green
    } else {
        throw "Redis not responding"
    }
} catch {
    Write-Host "[FAIL] Redis is not running!" -ForegroundColor $Red
    Write-Host "   Fix: Open terminal and run: redis-server`n" -ForegroundColor $Red
    exit 1
}

# Reset system
Write-Host "[2] Resetting system (clearing stuck jobs)..." -ForegroundColor $Yellow
try {
    cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"
    python reset_system.py 2>&1 | Select-String -Pattern "\[OK\]|\[WARN\]|\[FAIL\]" | ForEach-Object { Write-Host $_ }
    Write-Host ""
} catch {
    Write-Host "[WARN] System reset had issues, continuing anyway`n" -ForegroundColor $Yellow
}

# Create users
Write-Host "[3] Creating test users..." -ForegroundColor $Yellow
try {
    python create_users.py 2>&1 | tail -5
    Write-Host "[OK] Users created`n" -ForegroundColor $Green
} catch {
    Write-Host "[WARN] User creation issue (may already exist)`n" -ForegroundColor $Yellow
}

# Summary
Write-Host ("=" * 70)
Write-Host "SETUP COMPLETE - READY TO START SYSTEM`n" -ForegroundColor $Green
Write-Host ("=" * 70) + "`n"

Write-Host "START 3 TERMINALS IN THIS ORDER:`n" -ForegroundColor $Green

Write-Host "TERMINAL 1 - BACKEND:" -ForegroundColor $Yellow
Write-Host "`$env:ALTHEA_ENV=`"development`"; `$env:ALTHEA_DEFAULT_TENANT_ID=`"default-bank`"; `$env:ALTHEA_DATABASE_URL=`"sqlite:///data/althea.db`"; `$env:ALTHEA_REDIS_URL=`"redis://localhost:6379`"; `$env:ALTHEA_JWT_SECRET=`"your-32-character-secret-key-here-min32chars`"; `$env:ALTHEA_OBJECT_STORAGE_DIR=`"./data/models`"; python -m uvicorn main:app --reload --port 8000"
Write-Host "   WAIT FOR: 'Uvicorn running on http://0.0.0.0:8000'`n"

Write-Host "TERMINAL 2 - WORKER:" -ForegroundColor $Yellow
Write-Host "`$env:ALTHEA_DATABASE_URL=`"sqlite:///data/althea.db`"; `$env:ALTHEA_REDIS_URL=`"redis://localhost:6379`"; python windows_worker.py"
Write-Host "   WAIT FOR: '[LISTENING] Listening for jobs...'`n"

Write-Host "TERMINAL 3 - FRONTEND:" -ForegroundColor $Yellow
Write-Host "cd `"c:\Users\Users\OneDrive\Рабочий стол\Althea\frontend`"; npm run dev"
Write-Host "   WAIT FOR: '➜  Local:   http://localhost:5173/'`n"

Write-Host "THEN RUN PIPELINE TEST (in 4th temp terminal):" -ForegroundColor $Yellow
Write-Host "cd backend; python simple_run.py"
Write-Host "   WAIT FOR: 'Pipeline complete!'" -ForegroundColor $Yellow
Write-Host ""

Write-Host "LOGIN AT http://localhost:5173" -ForegroundColor $Green
Write-Host "   Email: analyst@bank.com"
Write-Host "   Password: Password123!`n"

Write-Host ("=" * 70) + "`n"
