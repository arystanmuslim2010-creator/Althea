#!/usr/bin/env python
"""Reset system: clear stuck jobs, database, prepare for fresh start."""
import os
import sys
from redis import Redis
from rq import Queue

print("\n" + "=" * 70)
print("ALTHEA SYSTEM RESET")
print("=" * 70 + "\n")

# Connect to Redis
print("[1] Connecting to Redis...")
try:
    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()
    print(f"[OK] Redis connected: {redis_url}\n")
except Exception as e:
    print(f"[FAIL] Redis: {e}")
    sys.exit(1)

# Clear stuck jobs
print("[2] Clearing stuck pipeline jobs...")
try:
    q = Queue("althea-pipeline", connection=redis_conn)
    job_ids = q.get_job_ids()

    if job_ids:
        print(f"Found {len(job_ids)} stuck job(s):")
        for job_id in job_ids:
            job = q.fetch_job(job_id)
            if job:
                print(f"  - Deleting: {job_id}")
                job.delete()
        print(f"[OK] Cleared {len(job_ids)} stuck job(s)\n")
    else:
        print("[OK] No stuck jobs found\n")
except Exception as e:
    print(f"[FAIL] Could not clear jobs: {e}")
    sys.exit(1)

# Clear database
print("[3] Clearing database state...")
try:
    from storage.postgres_repository import EnterpriseRepository
    db_url = os.getenv("ALTHEA_DATABASE_URL", "sqlite:///data/althea.db")
    repo = EnterpriseRepository(db_url)

    # Clear runtime contexts (cause artifact not found errors)
    from sqlalchemy import text
    engine = repo.engine
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM runtime_context"))
        conn.execute(text("DELETE FROM pipeline_runs WHERE status != 'completed'"))

    print("[OK] Database state cleared\n")
except Exception as e:
    print(f"[WARN] Could not clear database: {e}\n")

# Create test users
print("[4] Creating test users...")
try:
    from core.security import hash_password

    users_data = [
        {"email": "analyst@bank.com", "password": "Password123!", "role": "analyst"},
        {"email": "investigator@bank.com", "password": "Password123!", "role": "investigator"},
        {"email": "manager@bank.com", "password": "Password123!", "role": "manager"},
        {"email": "admin@bank.com", "password": "Password123!", "role": "admin"},
    ]

    for user in users_data:
        repo.create_user(
            tenant_id="default-bank",
            email=user["email"],
            password_hash=hash_password(user["password"]),
            role=user["role"]
        )
        print(f"  - Created: {user['email']} ({user['role']})")

    print(f"[OK] Created {len(users_data)} test users\n")
except Exception as e:
    print(f"[WARN] Could not create users: {e}\n")

# Summary
print("=" * 70)
print("SYSTEM RESET COMPLETE")
print("=" * 70)
print("\nNext steps:")
print("1. Terminal 1: Start Backend API")
print("   cd backend && python -m uvicorn main:app --reload --port 8000")
print("\n2. Terminal 2: Start Pipeline Worker")
print("   cd backend && python windows_worker.py")
print("\n3. Terminal 3: Start Frontend")
print("   cd frontend && npm run dev")
print("\n4. Run pipeline test (in separate terminal):")
print("   cd backend && python simple_run.py")
print("\n" + "=" * 70 + "\n")
