from __future__ import annotations

import time
from typing import Any

from core.observability import record_event
from storage.redis_cache import RedisCache


class EventBus:
    """Redis Streams abstraction with a transparent in-memory fallback."""

    def __init__(self, cache: RedisCache, stream_name: str = "althea.events") -> None:
        self._cache = cache
        self._stream_name = stream_name

    def publish(self, event_name: str, tenant_id: str, payload: dict[str, Any]) -> str:
        envelope = {
            "event_name": event_name,
            "tenant_id": tenant_id,
            "payload": payload,
        }
        record_event(event_name)
        return self._cache.publish_event(self._stream_name, envelope)

    def recent(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._cache.read_events(self._stream_name, limit=limit)

    def consume_after(self, last_event_id: str = "0-0", limit: int = 100, block_ms: int = 1000) -> list[dict[str, Any]]:
        return self._cache.read_events_after(self._stream_name, last_event_id=last_event_id, limit=limit, block_ms=block_ms)

    def subscribe(self, handler, start_from: str = "0-0", poll_sleep: float = 0.2) -> None:
        cursor = start_from
        while True:
            events = self.consume_after(last_event_id=cursor, limit=200, block_ms=1000)
            if not events:
                time.sleep(poll_sleep)
                continue
            for event in events:
                cursor = event.get("id", cursor)
                payload = event.get("payload", {})
                handler(payload)
