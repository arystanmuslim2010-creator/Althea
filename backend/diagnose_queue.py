#!/usr/bin/env python
"""Diagnose queue and worker issues."""
import os
from redis import Redis
from rq import Queue

print("\n" + "=" * 70)
print("ALTHEA QUEUE DIAGNOSTICS")
print("=" * 70 + "\n")

# Connect to Redis
print("[1] Connecting to Redis...")
try:
    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()
    print(f"[OK] Redis connected\n")
except Exception as e:
    print(f"[FAIL] Redis error: {e}\n")
    exit(1)

# Check queue
print("[2] Checking queue...")
try:
    q = Queue("althea-pipeline", connection=redis_conn)
    job_ids = q.get_job_ids()

    print(f"[OK] Queue found")
    print(f"   Queued jobs: {len(job_ids)}\n")

    if job_ids:
        print("   Pending jobs:")
        for job_id in job_ids[:5]:
            job = q.fetch_job(job_id)
            if job:
                status = job.get_status()
                print(f"     - {job_id}")
                print(f"       Status: {status}")
                print(f"       Function: {job.func_name}")
                if job.exc_info:
                    print(f"       Error: {job.exc_info[:100]}")
            else:
                print(f"     - {job_id} (job not found)")
        print()
except Exception as e:
    print(f"[FAIL] Queue error: {e}\n")
    exit(1)

# Check workers
print("[3] Checking active workers...")
try:
    from rq import Worker
    workers = Worker.all(connection=redis_conn)

    if workers:
        print(f"[OK] Found {len(workers)} worker(s):")
        for worker in workers:
            current_job = worker.get_current_job()
            if current_job:
                print(f"   - {worker.name}: PROCESSING {current_job.id}")
            else:
                print(f"   - {worker.name}: IDLE")
    else:
        print(f"[WARN] No workers registered!")
        print(f"   Worker is not running!")
        print(f"   Start Terminal 2: python windows_worker.py\n")
except Exception as e:
    print(f"[FAIL] Worker check error: {e}\n")

# Check dataset file
print("[4] Checking dataset files...")
try:
    dataset_dir = "../data/object_storage/datasets/default-bank/public"
    if os.path.exists(dataset_dir):
        files = os.listdir(dataset_dir)
        if files:
            print(f"[OK] Dataset directory has {len(files)} file(s):")
            for f in files:
                print(f"     - {f}")
        else:
            print(f"[WARN] Dataset directory is empty!")
    else:Error: Pipeline failedм
        print(f"[WARN] Dataset directory doesn't exist!
        print(f"   Path: {os.path.abspath(dataset_dir)}")
except Exception as e:
    print(f"[FAIL] Dataset check error: {e}\n")

print("\n" + "=" * 70)
print("DIAGNOSIS COMPLETE")
print("=" * 70 + "\n")

print("ISSUES & SOLUTIONS:\n")

if not workers:
    print("1. WORKER NOT RUNNING")
    print("   Fix: Start Terminal 2 with: python windows_worker.py\n")

if job_ids:
    print("2. JOBS STUCK IN QUEUE")
    print("   Possible causes:")
    print("   - Worker not running (see above)")
    print("   - Worker crashed with error")
    print("   - Dataset file missing\n")
    print("   Solution:")
    print("   - Check worker terminal for error messages")
    print("   - Verify dataset files exist")
    print("   - Restart worker: python windows_worker.py\n")

print("\n")
