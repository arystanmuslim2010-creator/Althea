from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from typing import Any

try:
    import redis
except Exception:  # pragma: no cover - optional dependency
    redis = None


class RedisCache:
    """Redis-first cache with in-memory fallback for local development and tests."""

    def __init__(self, url: str) -> None:
        self._lock = threading.RLock()
        self._memory: dict[str, Any] = {}
        self._streams: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._client = None
        if redis is not None:
            try:
                client = redis.Redis.from_url(url, decode_responses=True)
                client.ping()
                self._client = client
            except Exception:
                self._client = None

    def get_json(self, key: str, default: Any = None) -> Any:
        if self._client is not None:
            raw = self._client.get(key)
            return json.loads(raw) if raw else default
        with self._lock:
            return self._memory.get(key, default)

    def set_json(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if self._client is not None:
            payload = json.dumps(value)
            if ttl_seconds:
                self._client.setex(key, ttl_seconds, payload)
            else:
                self._client.set(key, payload)
            return
        with self._lock:
            self._memory[key] = value

    def delete(self, key: str) -> None:
        if self._client is not None:
            self._client.delete(key)
            return
        with self._lock:
            self._memory.pop(key, None)

    def publish_event(self, stream: str, payload: dict[str, Any]) -> str:
        if self._client is not None:
            return str(self._client.xadd(stream, {"payload": json.dumps(payload)}))
        with self._lock:
            event_id = f"{int(time.time() * 1000)}-{len(self._streams[stream])}"
            self._streams[stream].append({"id": event_id, "payload": payload})
            return event_id

    def read_events(self, stream: str, limit: int = 100) -> list[dict[str, Any]]:
        if self._client is not None:
            items = self._client.xrevrange(stream, count=limit)
            out: list[dict[str, Any]] = []
            for event_id, fields in items:
                out.append({"id": event_id, "payload": json.loads(fields.get("payload", "{}"))})
            return list(reversed(out))
        with self._lock:
            return list(self._streams.get(stream, []))[-limit:]
