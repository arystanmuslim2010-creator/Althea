# ALTHEA - Complete 3-Terminal Startup (Windows)

## Prerequisites
- Redis running: `redis-server`
- Python 3.10+
- Node.js installed
- All dependencies: `pip install -r requirements.txt` (backend), `npm install` (frontend)

---

## STEP 1: System Reset (Run Once)

Open a **temporary terminal** and run:

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"
python reset_system.py
```

Expected output:
```
[OK] Redis connected
[OK] Cleared 3 stuck job(s)
[OK] Database state cleared
SYSTEM RESET COMPLETE
```

This clears any stuck jobs from previous runs.

---

## STEP 2: Start 3 Terminals in Order

### Terminal 1: Backend API

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"; `
$env:ALTHEA_ENV="development"; `
$env:ALTHEA_DEFAULT_TENANT_ID="default-bank"; `
$env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"; `
$env:ALTHEA_REDIS_URL="redis://localhost:6379"; `
$env:ALTHEA_JWT_SECRET="your-32-character-secret-key-here-min32chars"; `
$env:ALTHEA_OBJECT_STORAGE_DIR="object_storage"; `
python -m uvicorn main:app --reload --port 8000
```

**WAIT FOR:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
```

Then proceed to Terminal 2 (keep Terminal 1 running).

---

### Terminal 2: Pipeline Worker

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"; `
$env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"; `
$env:ALTHEA_REDIS_URL="redis://localhost:6379"; `
python windows_worker.py
```

**WAIT FOR:**
```
[READY] Worker initialized!
[LISTENING] Listening for jobs...
```

Then proceed to Terminal 3 (keep Terminal 1 & 2 running).

---

### Terminal 3: Frontend

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\frontend"; `
npm run dev
```

**WAIT FOR:**
```
➜  Local:   http://localhost:5173/
```

All 3 terminals should now be running.

---

## STEP 3: Create Users & Run Pipeline (One-Time Setup)

Open a **4th temporary terminal**:

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"; `
python create_users.py; `
python cleanup_db.py
```

Wait for completion, then close this terminal.

---

## STEP 4: Run Pipeline Test

Open a **5th temporary terminal**:

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"; `
python simple_run.py
```

**WATCH Terminal 2** - you'll see:
```
[Job 1] Processing: job_xxxxx
  Function: run_pipeline
[OK] Job job_xxxxx completed successfully
```

Wait for simple_run.py to complete.

---

## STEP 5: Login & Use System

Open browser: **http://localhost:5173**

Login with:
- **Email:** analyst@bank.com
- **Password:** Password123!

Other test accounts:
- investigator@bank.com / Password123!
- manager@bank.com / Password123!
- admin@bank.com / Password123!

---

## Troubleshooting

### Problem: "Connection refused" on port 8000
**Fix:** Terminal 1 (Backend) not running. Check the output.

### Problem: Worker shows errors
**Fix:**
1. Ensure Redis is running: `redis-cli ping` → should say PONG
2. Kill Terminal 2 worker, restart it

### Problem: "Pipeline job stuck"
**Fix:**
```powershell
cd backend
python reset_system.py
# Then restart all 3 terminals
```

### Problem: Frontend shows "Cannot GET /"
**Fix:** Terminal 3 not started properly. Check output.

---

## Quick Reference URLs

- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Redis: localhost:6379
- Database: data/althea.db (SQLite)

---

## Architecture

```
Terminal 1: Backend API (uvicorn)
  ├─ HTTP server on port 8000
  ├─ Connects to: Redis, Database
  └─ Exposes: /api/*, /health, /docs

Terminal 2: Pipeline Worker (windows_worker.py)
  ├─ Synchronous job processor
  ├─ No forking (Windows compatible)
  ├─ Polls Redis queue every 0.5s
  └─ Executes: feature engineering → inference → governance

Terminal 3: Frontend (npm run dev)
  ├─ Vite dev server on port 5173
  ├─ React application
  └─ Communicates with Backend API
```

---

## Summary

1. **Reset:** `python reset_system.py`
2. **Terminal 1:** Backend API
3. **Terminal 2:** Worker
4. **Terminal 3:** Frontend
5. **Temp:** Create users
6. **Temp:** Run pipeline
7. **Browser:** Login and use!

✅ System fully operational.
