from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from core.dependencies import get_cache
from workers.event_subscriber_worker import run_event_subscriber
from workers.pipeline_worker import run_rq_worker
from workers.streaming_worker import run_streaming_worker

logger = logging.getLogger("althea.all_in_one_worker")


def _supervise(name: str, fn, *args, **kwargs) -> None:
    while True:
        try:
            logger.info("starting worker=%s", name)
            fn(*args, **kwargs)
        except Exception:
            logger.exception("worker crashed worker=%s; restarting in 2s", name)
            time.sleep(2.0)


def run_all_in_one_worker() -> None:
    print("[all_in_one_worker] starting pipeline + event + streaming", flush=True)
    cache = get_cache()

    def _heartbeat() -> None:
        while True:
            cache.set_json(
                "heartbeat:worker:all_in_one",
                {"worker": "all_in_one", "ts": datetime.now(timezone.utc).isoformat()},
                ttl_seconds=30,
            )
            time.sleep(10)

    heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True, name="all-in-one-heartbeat")
    heartbeat_thread.start()

    event_thread = threading.Thread(
        target=_supervise,
        args=("event", run_event_subscriber),
        daemon=True,
        name="althea-event-worker",
    )
    streaming_thread = threading.Thread(
        target=_supervise,
        args=("streaming", run_streaming_worker),
        kwargs={"poll_interval": 0.5},
        daemon=True,
        name="althea-streaming-worker",
    )
    event_thread.start()
    streaming_thread.start()

    # Keep RQ worker in foreground so process lifetime follows queue consumer lifecycle.
    _supervise("pipeline", run_rq_worker)


if __name__ == "__main__":
    run_all_in_one_worker()
