# ALTHEA Proper System Startup & Pipeline Execution

## Prerequisites

1. **Redis running** (needed for job queue)
```powershell
redis-server
```

2. **All services stopped** (Ctrl+C in all terminals)

---

## Complete Startup Sequence

### Terminal 1: Backend API

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"
$env:ALTHEA_ENV="development"
$env:ALTHEA_DEFAULT_TENANT_ID="default-bank"
$env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"
$env:ALTHEA_REDIS_URL="redis://localhost:6379"
$env:ALTHEA_JWT_SECRET="your-32-character-secret-key-here-min32chars"
$env:ALTHEA_OBJECT_STORAGE_DIR="./data/models"
python -m uvicorn main:app --reload --port 8000
```

**Expected output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
```

**Wait 3 seconds, then proceed to next terminal.**

---

### Terminal 2: Setup Database & Create Users

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"

# Create test users
python create_users.py

# Clean database for fresh pipeline run
python cleanup_db.py
```

**Expected output:**
```
✓ Created user: analyst@bank.com
...
✅ DATABASE CLEANED!
```

---

### Terminal 3: Worker (Processes Pipeline Jobs)

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"
$env:ALTHEA_DATABASE_URL="sqlite:///data/althea.db"
$env:ALTHEA_REDIS_URL="redis://localhost:6379"
python -m workers.pipeline_worker
```

**Expected output:**
```
*** Listening on althea-pipeline...
```

**Keep this running in the background.**

---

### Terminal 4: Run Complete Pipeline (Upload + Process)

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\backend"
python simple_run.py
```

**Expected output:**
```
======================================================================
ALTHEA SIMPLE PIPELINE RUN
======================================================================

✓ Found sample_data.csv (1234 bytes)

STEP 1: LOGIN
✅ Logged in successfully

📌 TOKEN: eyJ0eXAiOiJKV1QiLCJhbGc...

STEP 2: UPLOAD CSV
✅ CSV uploaded successfully
   Dataset Hash: abc123def456
   Row Count: 10

STEP 3: VERIFY FILE STORAGE
✓ File found at correct location:
   data/object_storage/datasets/default-bank/public/abc123def456.csv

STEP 4: START PIPELINE
✅ Pipeline started
   Run ID: run-xyz789

STEP 5: WAITING FOR PROCESSING
⏳ Waiting for worker to process...
   Generated 10 alerts...

✅ Pipeline complete!
   Generated 10 alerts

======================================================================
✅ SETUP COMPLETE!
======================================================================

📱 Frontend: http://localhost:5173
👤 Login: analyst@bank.com / Password123!
🔑 Token: eyJ0eXAiOiJKV1QiLCJhbGc...
```

---

### Terminal 5: Frontend

```powershell
cd "c:\Users\Users\OneDrive\Рабочий стол\Althea\frontend"
npm run dev
```

**Expected output:**
```
  ➜  Local:   http://localhost:5173/
```

---

## Access the System

1. **Open browser:** http://localhost:5173
2. **Login:**
   - Email: `analyst@bank.com`
   - Password: `Password123!`

3. **View alerts:** They should appear in the dashboard after pipeline completes

---

## What Each Component Does

| Component | Terminal | Purpose |
|-----------|----------|---------|
| **Backend API** | 1 | Handles all HTTP requests, exposes pipeline API |
| **Database + Users** | 2 | One-time setup to prepare database and test users |
| **Worker** | 3 | Processes queued pipeline jobs, generates ML scores |
| **Pipeline Run** | 4 | Single complete run: upload CSV → generate alerts → save results |
| **Frontend** | 5 | User interface for investigation |

---

## Key Fixes Applied

### 1. **Database Cleanup** (`cleanup_db.py`)
- Removes stale runtime contexts that point to wrong file paths
- Clears failed pipeline jobs
- Ensures fresh state for pipeline execution

### 2. **Simplified Pipeline** (`simple_run.py`)
- Handles complete flow: login → upload → process → verify
- Checks file storage location to verify uploads work
- Provides detailed progress output
- Saves token and login info for reuse

### 3. **Correct Path Resolution**
- Ensures `ALTHEA_OBJECT_STORAGE_DIR` correctly resolves to `data/object_storage`
- Upload stores files at: `data/object_storage/datasets/{tenant}/{scope}/{hash}.csv`
- Worker loads files from same location: `data/object_storage/datasets/{tenant}/{scope}/{hash}.csv`
- **NO PATH MISMATCH**

---

## Troubleshooting

### Problem: Worker says "Dataset artifact is missing"

**Solution:**
1. Stop all terminals (Ctrl+C)
2. Run Terminal 2 setup again (cleanup_db.py)
3. Run Terminal 3 (worker) again
4. Run Terminal 4 (simple_run.py) again

### Problem: "Connection refused" on port 8000

**Solution:** Backend (Terminal 1) is not running. Check Terminal 1 output for errors.

### Problem: Worker not processing jobs

**Solution:**
1. Check Redis is running: `redis-cli ping` (should say `PONG`)
2. Check Terminal 3 shows: `*** Listening on althea-pipeline...`
3. If not, restart Terminal 3

### Problem: CSV upload fails

**Solution:**
1. Ensure `sample_data.csv` exists in backend directory
2. Check Terminal 1 for API errors
3. Run `python simple_run.py` again (handles retries)

---

## Success Indicators

✅ **Backend Terminal (1):**
```
INFO:     Application startup complete
```

✅ **Worker Terminal (3):**
```
*** Listening on althea-pipeline...
```

✅ **Pipeline Terminal (4):**
```
✅ Pipeline complete!
   Generated 10 alerts
```

✅ **Frontend Terminal (5):**
```
➜  Local:   http://localhost:5173/
```

✅ **Browser at http://localhost:5173:**
- Login page appears
- Alerts dashboard shows after login

---

## That's It!

Once you see all success indicators, the system is **fully operational** with:
- ✅ Database initialized
- ✅ Users created
- ✅ Test data loaded
- ✅ ML pipeline processed
- ✅ Frontend running
- ✅ Ready to investigate alerts

**Now you can use the investigation intelligence APIs:**
- GET `/api/alerts/{id}/investigation-summary`
- GET `/api/alerts/{id}/risk-explanation`
- GET `/api/alerts/{id}/network-graph`
- GET `/api/alerts/{id}/investigation-steps`
- GET `/api/alerts/{id}/sar-draft`
- GET `/api/alerts/{id}/investigation-context` (all in one!)
- POST `/api/alerts/{id}/outcome` (record analyst decision)

Happy investigating! 🚀
