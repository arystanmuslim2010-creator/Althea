#!/usr/bin/env python
"""Run all workers in one terminal: Pipeline, Streaming, Event."""
import logging
import threading
import time
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(name)s] %(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("althea-workers")

print("=" * 70)
print("ALTHEA COMBINED WORKERS")
print("=" * 70)
print("\nStarting all workers in single terminal...\n")

# Worker 1: Pipeline Worker
def run_pipeline_worker():
    """Run the pipeline job processor."""
    try:
        logger.info("Pipeline Worker: Starting...")
        from workers.pipeline_worker import main as pipeline_main
        pipeline_main()
    except Exception as e:
        logger.error(f"Pipeline Worker failed: {e}")

# Worker 2: Streaming Worker (if available)
def run_streaming_worker():
    """Run the streaming event processor."""
    try:
        logger.info("Streaming Worker: Starting...")
        time.sleep(1)  # Stagger startup
        # Check if streaming worker exists
        try:
            from workers.streaming_worker import main as streaming_main
            streaming_main()
        except ImportError:
            logger.warning("Streaming Worker: Not found, skipping")
    except Exception as e:
        logger.error(f"Streaming Worker failed: {e}")

# Worker 3: Event Subscriber Worker
def run_event_worker():
    """Run the event subscriber processor."""
    try:
        logger.info("Event Worker: Starting...")
        time.sleep(2)  # Stagger startup
        # Check if event worker exists
        try:
            from workers.event_subscriber_worker import main as event_main
            event_main()
        except ImportError:
            logger.warning("Event Worker: Not found, skipping")
    except Exception as e:
        logger.error(f"Event Worker failed: {e}")

# Main launcher
if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("Initializing all workers...")
    logger.info("=" * 70)

    # Create threads for each worker
    threads = []

    # Pipeline worker (always runs)
    logger.info("Creating Pipeline Worker thread...")
    t1 = threading.Thread(target=run_pipeline_worker, daemon=True, name="PipelineWorker")
    threads.append(t1)

    # Streaming worker (optional)
    logger.info("Creating Streaming Worker thread...")
    t2 = threading.Thread(target=run_streaming_worker, daemon=True, name="StreamingWorker")
    threads.append(t2)

    # Event worker (optional)
    logger.info("Creating Event Worker thread...")
    t3 = threading.Thread(target=run_event_worker, daemon=True, name="EventWorker")
    threads.append(t3)

    # Start all threads
    logger.info("\nStarting all workers...")
    for t in threads:
        t.start()
        time.sleep(0.5)

    logger.info("=" * 70)
    logger.info("✅ All workers initialized!")
    logger.info("=" * 70)
    logger.info("\nPress Ctrl+C to stop all workers\n")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n\nShutting down all workers...")
        logger.info("Waiting for graceful shutdown...")
        time.sleep(2)
        logger.info("✅ All workers stopped")
        exit(0)
