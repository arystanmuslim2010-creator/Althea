# ALTHEA - Start All 3 Terminals with One Command

$AltheaPath = "c:\Users\Users\OneDrive\Рабочий стол\Althea"
$BackendPath = "$AltheaPath\backend"
$FrontendPath = "$AltheaPath\frontend"

Write-Host "`n" + ("=" * 70)
Write-Host "STARTING ALL 3 TERMINALS"
Write-Host ("=" * 70 + "`n")

# Terminal 1: Backend API
Write-Host "[1] Starting Backend API (Terminal 1)..."
$BackendCmd = @"
cd `"$BackendPath`"; `
`$env:ALTHEA_ENV=`"development`"; `
`$env:ALTHEA_DEFAULT_TENANT_ID=`"default-bank`"; `
`$env:ALTHEA_DATABASE_URL=`"sqlite:///data/althea.db`"; `
`$env:ALTHEA_REDIS_URL=`"redis://localhost:6379`"; `
`$env:ALTHEA_JWT_SECRET=`"your-32-character-secret-key-here-min32chars`"; `
`$env:ALTHEA_OBJECT_STORAGE_DIR=`"./data/models`"; `
python -m uvicorn main:app --reload --port 8000
"@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $BackendCmd

# Terminal 2: Pipeline Worker
Write-Host "[2] Starting Pipeline Worker (Terminal 2)..."
$WorkerCmd = @"
cd `"$BackendPath`"; `
`$env:ALTHEA_DATABASE_URL=`"sqlite:///data/althea.db`"; `
`$env:ALTHEA_REDIS_URL=`"redis://localhost:6379`"; `
python windows_worker.py
"@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $WorkerCmd

# Terminal 3: Frontend
Write-Host "[3] Starting Frontend (Terminal 3)..."
$FrontendCmd = @"
cd `"$FrontendPath`"; `
npm run dev
"@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $FrontendCmd

Write-Host "`n" + ("=" * 70)
Write-Host "ALL 3 TERMINALS STARTED!"
Write-Host ("=" * 70)
Write-Host "`nWait for:"
Write-Host "  [Terminal 1] 'Uvicorn running on http://0.0.0.0:8000'"
Write-Host "  [Terminal 2] '[LISTENING] Listening for jobs...'"
Write-Host "  [Terminal 3] 'Local:   http://localhost:5173/'"
Write-Host "`nThen open browser to: http://localhost:5173"
Write-Host "Login: analyst@bank.com / Password123!"
Write-Host "`n" + ("=" * 70 + "`n")
