#!/usr/bin/env python
"""Run pipeline worker directly with RQ job processing."""
import logging
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("pipeline-worker")

print("=" * 70)
print("ALTHEA PIPELINE WORKER (RQ Job Processor)")
print("=" * 70)

# Import RQ and required components
try:
    from redis import Redis
    from rq import Worker
    from rq.job import JobStatus
    import time

    logger.info("✓ Imported RQ and Redis")
except ImportError as e:
    logger.error(f"❌ Missing required library: {e}")
    logger.error("Install with: pip install rq redis")
    sys.exit(1)

# Setup Redis connection
REDIS_URL = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379")

try:
    redis_conn = Redis.from_url(REDIS_URL)
    redis_conn.ping()
    logger.info(f"✓ Connected to Redis: {REDIS_URL}")
except Exception as e:
    logger.error(f"❌ Cannot connect to Redis: {e}")
    logger.error(f"   Ensure Redis is running!")
    logger.error(f"   Run: redis-server")
    sys.exit(1)

# Create worker that listens to pipeline queue
try:
    queue_name = "althea-pipeline"
    worker = Worker([queue_name], connection=redis_conn)

    logger.info("=" * 70)
    logger.info(f"✅ Worker initialized!")
    logger.info(f"   Queue: {queue_name}")
    logger.info(f"   Redis: {REDIS_URL}")
    logger.info("=" * 70)
    logger.info("\n🎯 Listening for jobs...")
    logger.info("   Press Ctrl+C to stop\n")

    # Start processing jobs
    # work() is blocking and will process jobs as they arrive
    worker.work(with_scheduler=True)

except KeyboardInterrupt:
    logger.info("\n\n" + "=" * 70)
    logger.info("Shutting down worker...")
    logger.info("=" * 70)
    sys.exit(0)
except Exception as e:
    logger.error(f"❌ Worker error: {e}")
    logger.error("Check that:")
    logger.error("  1. Redis is running")
    logger.error("  2. Database is initialized")
    logger.error("  3. Backend API is running")
    sys.exit(1)
