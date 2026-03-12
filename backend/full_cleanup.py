#!/usr/bin/env python
"""Complete cleanup: clear Redis, database, and files."""
import os
import sys
import shutil
from redis import Redis

print("\n" + "=" * 70)
print("ALTHEA FULL CLEANUP")
print("=" * 70 + "\n")

# 1. Clear Redis
print("[1] Clearing Redis...")
try:
    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()

    # Get queue and clear all jobs
    from rq import Queue
    q = Queue("althea-pipeline", connection=redis_conn)
    job_ids = q.get_job_ids()

    for job_id in job_ids:
        job = q.fetch_job(job_id)
        if job:
            job.delete()

    # Clear all Redis keys
    redis_conn.flushdb()
    print(f"[OK] Redis cleared\n")
except Exception as e:
    print(f"[WARN] Redis clear failed: {e}\n")

# 2. Clear database tables
print("[2] Clearing database tables...")
try:
    from storage.postgres_repository import EnterpriseRepository
    db_url = os.getenv("ALTHEA_DATABASE_URL", "sqlite:///data/althea.db")
    repo = EnterpriseRepository(db_url)

    from sqlalchemy import text
    engine = repo.engine

    # Tables to clear (in dependency order)
    tables_to_clear = [
        "runtime_context",
        "alerts",
        "pipeline_runs",
        "cases",
        "investigations"
    ]

    with engine.begin() as conn:
        for table in tables_to_clear:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
                print(f"   - Cleared {table}")
            except:
                pass  # Table might not exist

    print("[OK] Database tables cleared\n")
except Exception as e:
    print(f"[WARN] Database clear failed: {e}\n")

# 3. Clear artifact files
print("[3] Clearing artifact files...")
try:
    artifact_dirs = [
        "data/object_storage/datasets/default-bank",
        "data/models",
        ".rq_work"
    ]

    for dir_path in artifact_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"   - Removed {dir_path}")

    # Recreate directories
    os.makedirs("data/object_storage/datasets/default-bank/public", exist_ok=True)
    os.makedirs("data/models", exist_ok=True)

    print("[OK] Artifacts cleared\n")
except Exception as e:
    print(f"[WARN] Artifact clear failed: {e}\n")

# 4. Recreate test users
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
        try:
            # Try to delete existing user first
            from sqlalchemy import text
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM users WHERE tenant_id = :tid AND email = :email"),
                    {"tid": "default-bank", "email": user["email"]}
                )

            # Create new user
            repo.create_user(
                tenant_id="default-bank",
                email=user["email"],
                password_hash=hash_password(user["password"]),
                role=user["role"]
            )
            print(f"   - Created: {user['email']}")
        except Exception as e:
            print(f"   - Error creating {user['email']}: {e}")

    print("[OK] Test users ready\n")
except Exception as e:
    print(f"[WARN] User creation: {e}\n")

print("=" * 70)
print("CLEANUP COMPLETE - SYSTEM RESET")
print("=" * 70 + "\n")
print("Next: Start 3 terminals (Backend, Worker, Frontend)")
print("Then: Run python simple_run.py in temp terminal")
print("")
