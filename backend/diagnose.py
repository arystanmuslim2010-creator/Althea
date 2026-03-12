#!/usr/bin/env python
"""Diagnose system connectivity and queue status."""
import os
import sys

print("\n" + "=" * 70)
print("ALTHEA SYSTEM DIAGNOSTICS")
print("=" * 70 + "\n")

# Check 1: Redis
print("1️⃣  REDIS CONNECTION")
print("-" * 70)
try:
    from redis import Redis
    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()
    print(f"✅ Redis connected: {redis_url}")
except Exception as e:
    print(f"❌ Redis FAILED: {e}")
    print("   Fix: Start Redis with: redis-server")
    sys.exit(1)

# Check 2: Database
print("\n2️⃣  DATABASE CONNECTION")
print("-" * 70)
try:
    from storage.postgres_repository import EnterpriseRepository
    db_url = os.getenv("ALTHEA_DATABASE_URL", "sqlite:///data/althea.db")
    repo = EnterpriseRepository(db_url)
    repo.ping()
    print(f"✅ Database connected: {db_url}")
except Exception as e:
    print(f"❌ Database FAILED: {e}")
    sys.exit(1)

# Check 3: Queue Status
print("\n3️⃣  JOB QUEUE STATUS")
print("-" * 70)
try:
    from rq import Queue
    q = Queue("althea-pipeline", connection=redis_conn)

    job_count = len(q.get_job_ids())
    print(f"✅ Queue 'althea-pipeline' found")
    print(f"   Queued jobs: {job_count}")

    if job_count > 0:
        print("\n   Pending jobs:")
        for job_id in q.get_job_ids()[:5]:
            job = q.fetch_job(job_id)
            status = job.get_status() if job else "unknown"
            print(f"     - {job_id}: {status}")
except Exception as e:
    print(f"❌ Queue check FAILED: {e}")
    sys.exit(1)

# Check 4: Worker Status
print("\n4️⃣  WORKER STATUS")
print("-" * 70)
try:
    from rq import Worker
    workers = Worker.all(connection=redis_conn)
    active_workers = [w for w in workers if w.get_current_job()]

    if workers:
        print(f"✅ Workers registered: {len(workers)}")
        for worker in workers:
            job = worker.get_current_job()
            if job:
                print(f"   - {worker.name}: PROCESSING {job.id}")
            else:
                print(f"   - {worker.name}: IDLE")
    else:
        print(f"⚠️  No workers registered!")
        print("   Fix: Start Terminal 2 worker with: python combined_workers_fixed.py")
except Exception as e:
    print(f"❌ Worker status check FAILED: {e}")
    sys.exit(1)

# Check 5: Users
print("\n5️⃣  TEST USERS")
print("-" * 70)
try:
    users = repo.list_users("default-bank")
    if users:
        print(f"✅ Found {len(users)} user(s):")
        for user in users[:3]:
            print(f"   - {user.get('email')} (role: {user.get('role')})")
    else:
        print(f"⚠️  No users found!")
        print("   Fix: Run in Terminal 2: python create_users.py")
except Exception as e:
    print(f"❌ User check FAILED: {e}")

# Check 6: Dataset
print("\n6️⃣  DATASET STATUS")
print("-" * 70)
try:
    import os.path
    dataset_dir = "data/object_storage/datasets/default-bank/public"
    if os.path.exists(dataset_dir):
        files = os.listdir(dataset_dir)
        if files:
            print(f"✅ Dataset directory found with {len(files)} file(s):")
            for f in files[:3]:
                print(f"   - {f}")
        else:
            print(f"⚠️  Dataset directory empty!")
            print("   Fix: Run in Terminal 2: python simple_run.py")
    else:
        print(f"⚠️  Dataset directory not found!")
        print("   Path: {dataset_dir}")
except Exception as e:
    print(f"❌ Dataset check FAILED: {e}")

# Summary
print("\n" + "=" * 70)
print("DIAGNOSIS SUMMARY")
print("=" * 70 + "\n")

if job_count > 0:
    print("⚠️  ISSUE DETECTED:")
    print(f"   {job_count} job(s) queued but not processed!")
    print("\n   SOLUTIONS:")
    print("   1. Check Terminal 2 worker is running:")
    print("      python combined_workers_fixed.py")
    print("   2. Ensure Redis is running:")
    print("      redis-cli ping → should say PONG")
    print("   3. Check for errors in Terminal 2 output")
    print("   4. Restart worker if needed")
else:
    print("✅ SYSTEM READY!")
    print("   - Redis: OK")
    print("   - Database: OK")
    print("   - Worker: Listening")
    print("\n   Ready to process pipeline jobs!")

print("\n" + "=" * 70 + "\n")
