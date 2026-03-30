from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from core.dependencies import get_cache, get_streaming_orchestrator

logger = logging.getLogger("althea.streaming_worker")


def run_streaming_worker(poll_interval: float = 0.5) -> None:
    orchestrator = get_streaming_orchestrator()
    cache = get_cache()
    logger.info("Starting streaming consumer worker")
    while True:
        try:
            cache.set_json(
                "heartbeat:worker:streaming",
                {"worker": "streaming", "ts": datetime.now(timezone.utc).isoformat()},
                ttl_seconds=30,
            )
            orchestrator.process_once(batch_size=1000)
        except Exception:
            logger.exception("Streaming worker cycle failed")
        time.sleep(max(0.05, poll_interval))


if __name__ == "__main__":
    run_streaming_worker()
