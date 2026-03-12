#!/usr/bin/env python
"""Windows-compatible pipeline worker (synchronous mode, no forking)."""
import os
import sys
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("windows-worker")

print("\n" + "=" * 70)
print("ALTHEA PIPELINE WORKER (Windows Compatible)")
print("=" * 70 + "\n")

# Check Redis
try:
    from redis import Redis
    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")
    redis_conn = Redis.from_url(redis_url)
    redis_conn.ping()
    logger.info(f"[OK] Redis connected: {redis_url}")
except Exception as e:
    logger.error(f"[FAIL] Redis FAILED: {e}")
    sys.exit(1)

# Import RQ and worker components
try:
    from rq import Queue, Worker
    from rq.job import JobStatus
    logger.info("[OK] RQ imported successfully")
except ImportError as e:
    logger.error(f"[FAIL] RQ not found: {e}")
    sys.exit(1)

# Create queue reference
queue_name = "althea-pipeline"
q = Queue(queue_name, connection=redis_conn)

logger.info("=" * 70)
logger.info("[READY] Worker initialized!")
logger.info(f"   Queue: {queue_name}")
logger.info(f"   Mode: Synchronous (Windows compatible)")
logger.info("=" * 70)
logger.info("\n[LISTENING] Listening for jobs...\n")

# Main loop: Poll for jobs and process them
try:
    job_count = 0
    while True:
        try:
            # Get next queued job
            job_ids = q.get_job_ids()
            job = None

            if job_ids:
                # Fetch first queued job
                for job_id in job_ids:
                    candidate = q.fetch_job(job_id)
                    if candidate and candidate.get_status() == 'queued':
                        job = candidate
                        break

            if job:
                job_count += 1
                logger.info(f"\n{'='*70}")
                logger.info(f"[Job {job_count}] Processing: {job.id}")
                logger.info(f"  Function: {job.func_name}")
                logger.info(f"  Args: {job.args}")
                logger.info(f"  Kwargs: {job.kwargs}")
                logger.info(f"{'='*70}\n")

                try:
                    # Execute the job
                    job.set_status(JobStatus.STARTED)
                    result = job.perform()

                    # Mark as complete
                    job.set_status(JobStatus.FINISHED)
                    job.result = result
                    job.save()

                    logger.info(f"[OK] Job {job.id} completed successfully\n")

                except Exception as job_error:
                    # Mark as failed
                    job.set_status(JobStatus.FAILED)
                    job.save()

                    logger.error(f"[FAIL] Job {job.id} failed:")
                    logger.error(f"   Error: {job_error}\n")

            else:
                # No job available, keep listening
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error in job loop: {e}")
            time.sleep(1)

except KeyboardInterrupt:
    logger.info("\n\n" + "=" * 70)
    logger.info(f"Shutdown complete. Processed {job_count} job(s).")
    logger.info("=" * 70 + "\n")
    sys.exit(0)
