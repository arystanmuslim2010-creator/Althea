@echo off
REM ALTHEA - Start All 3 Terminals

setlocal enabledelayedexpansion

set ALTHEA_PATH=c:\Users\Users\OneDrive\Рабочий стол\Althea
set BACKEND_PATH=%ALTHEA_PATH%\backend
set FRONTEND_PATH=%ALTHEA_PATH%\frontend

cls
echo.
echo ======================================================================
echo STARTING ALL 3 TERMINALS
echo ======================================================================
echo.

REM Terminal 1: Backend API
echo [1] Starting Backend API (Terminal 1)...
start "ALTHEA Backend" cmd /k "cd /d %BACKEND_PATH% && set ALTHEA_ENV=development && set ALTHEA_DEFAULT_TENANT_ID=default-bank && set ALTHEA_DATABASE_URL=sqlite:///data/althea.db && set ALTHEA_REDIS_URL=redis://localhost:6379 && set ALTHEA_JWT_SECRET=your-32-character-secret-key-here-min32chars && set ALTHEA_OBJECT_STORAGE_DIR=object_storage && python -m uvicorn main:app --reload --port 8000"

timeout /t 2 /nobreak

REM Terminal 2: Pipeline Worker
echo [2] Starting Pipeline Worker (Terminal 2)...
start "ALTHEA Worker" cmd /k "cd /d %BACKEND_PATH% && set ALTHEA_DATABASE_URL=sqlite:///data/althea.db && set ALTHEA_REDIS_URL=redis://localhost:6379 && python windows_worker.py"

timeout /t 2 /nobreak

REM Terminal 3: Frontend
echo [3] Starting Frontend (Terminal 3)...
start "ALTHEA Frontend" cmd /k "cd /d %FRONTEND_PATH% && npm run dev"

echo.
echo ======================================================================
echo ALL 3 TERMINALS STARTED!
echo ======================================================================
echo.
echo Wait for:
echo   [Terminal 1] "Uvicorn running on http://0.0.0.0:8000"
echo   [Terminal 2] "[LISTENING] Listening for jobs..."
echo   [Terminal 3] "Local:   http://localhost:5173/"
echo.
echo Then open browser to: http://localhost:5173
echo Login: analyst@bank.com / Password123!
echo.
echo ======================================================================
echo.
pause
