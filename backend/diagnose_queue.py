#!/usr/bin/env python
"""Diagnose queue and worker issues."""

from __future__ import annotations

import os
from pathlib import Path

from redis import Redis
from rq import Queue, Worker


def main() -> int:
    print("\n" + "=" * 70)
    print("ALTHEA QUEUE DIAGNOSTICS")
    print("=" * 70 + "\n")

    print("[1] Connecting to Redis...")
    try:
        redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
        redis_conn = Redis.from_url(redis_url)
        redis_conn.ping()
        print("[OK] Redis connected\n")
    except Exception as exc:
        print(f"[FAIL] Redis error: {exc}\n")
        return 1

    job_ids: list[str] = []
    print("[2] Checking queue...")
    try:
        queue = Queue("althea-pipeline", connection=redis_conn)
        job_ids = list(queue.get_job_ids())

        print("[OK] Queue found")
        print(f"   Queued jobs: {len(job_ids)}\n")

        if job_ids:
            print("   Pending jobs:")
            for job_id in job_ids[:5]:
                job = queue.fetch_job(job_id)
                if job is None:
                    print(f"     - {job_id} (job not found)")
                    continue
                print(f"     - {job_id}")
                print(f"       Status: {job.get_status()}")
                print(f"       Function: {job.func_name}")
                if job.exc_info:
                    print(f"       Error: {job.exc_info[:100]}")
            print()
    except Exception as exc:
        print(f"[FAIL] Queue error: {exc}\n")
        return 1

    workers: list[Worker] = []
    print("[3] Checking active workers...")
    try:
        workers = list(Worker.all(connection=redis_conn))
        if workers:
            print(f"[OK] Found {len(workers)} worker(s):")
            for worker in workers:
                current_job = worker.get_current_job()
                if current_job:
                    print(f"   - {worker.name}: PROCESSING {current_job.id}")
                else:
                    print(f"   - {worker.name}: IDLE")
        else:
            print("[WARN] No workers registered!")
            print("   Worker is not running!")
            print("   Start a worker: python -m workers.pipeline_worker\n")
    except Exception as exc:
        print(f"[FAIL] Worker check error: {exc}\n")

    print("[4] Checking dataset files...")
    try:
        dataset_dir = (
            Path(__file__).resolve().parent.parent
            / "data"
            / "object_storage"
            / "datasets"
            / "default-bank"
            / "public"
        )
        if dataset_dir.exists():
            files = sorted(path.name for path in dataset_dir.iterdir() if path.is_file())
            if files:
                print(f"[OK] Dataset directory has {len(files)} file(s):")
                for filename in files:
                    print(f"     - {filename}")
            else:
                print("[WARN] Dataset directory is empty!")
        else:
            print("[WARN] Dataset directory doesn't exist!")
            print(f"   Path: {dataset_dir}")
    except Exception as exc:
        print(f"[FAIL] Dataset check error: {exc}\n")

    print("\n" + "=" * 70)
    print("DIAGNOSIS COMPLETE")
    print("=" * 70 + "\n")

    print("ISSUES & SOLUTIONS:\n")

    if not workers:
        print("1. WORKER NOT RUNNING")
        print("   Fix: Start a worker with: python -m workers.pipeline_worker\n")

    if job_ids:
        print("2. JOBS STUCK IN QUEUE")
        print("   Possible causes:")
        print("   - Worker not running (see above)")
        print("   - Worker crashed with error")
        print("   - Dataset file missing\n")
        print("   Solution:")
        print("   - Check worker logs for error messages")
        print("   - Verify dataset files exist")
        print("   - Restart the worker: python -m workers.pipeline_worker\n")

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
