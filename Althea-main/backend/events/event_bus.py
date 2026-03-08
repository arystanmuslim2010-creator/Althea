from __future__ import annotations

from typing import Any

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
        return self._cache.publish_event(self._stream_name, envelope)

    def recent(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._cache.read_events(self._stream_name, limit=limit)
