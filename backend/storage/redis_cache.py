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
        self._memory_expirations: dict[str, float] = {}
        self._counters: dict[str, int] = {}
        self._counter_expirations: dict[str, float] = {}
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
            expiry = float(self._memory_expirations.get(key, 0.0) or 0.0)
            if expiry and expiry <= time.time():
                self._memory.pop(key, None)
                self._memory_expirations.pop(key, None)
                return default
            return self._memory.get(key, default)

    def ping(self) -> bool:
        if self._client is not None:
            self._client.ping()
            return True
        return True

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
            if ttl_seconds:
                self._memory_expirations[key] = time.time() + max(1, int(ttl_seconds))
            else:
                self._memory_expirations.pop(key, None)

    def delete(self, key: str) -> None:
        if self._client is not None:
            self._client.delete(key)
            return
        with self._lock:
            self._memory.pop(key, None)
            self._memory_expirations.pop(key, None)
            self._counters.pop(key, None)
            self._counter_expirations.pop(key, None)

    def increment_counter(self, key: str, ttl_seconds: int) -> int:
        ttl = max(1, int(ttl_seconds or 1))
        if self._client is not None:
            pipeline = self._client.pipeline()
            pipeline.incr(key)
            pipeline.ttl(key)
            count, current_ttl = pipeline.execute()
            if int(count or 0) == 1 or int(current_ttl or -1) < 0:
                self._client.expire(key, ttl)
            return int(count or 0)

        now = time.time()
        with self._lock:
            expiry = float(self._counter_expirations.get(key, 0.0) or 0.0)
            if expiry <= now:
                self._counters[key] = 0
            self._counter_expirations[key] = now + ttl
            self._counters[key] = int(self._counters.get(key, 0)) + 1
            return self._counters[key]

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

    def read_events_after(
        self,
        stream: str,
        last_event_id: str = "0-0",
        limit: int = 100,
        block_ms: int = 1000,
    ) -> list[dict[str, Any]]:
        if self._client is not None:
            result = self._client.xread({stream: last_event_id}, block=block_ms, count=limit)
            out: list[dict[str, Any]] = []
            for _, items in result:
                for event_id, fields in items:
                    out.append({"id": event_id, "payload": json.loads(fields.get("payload", "{}"))})
            return out
        with self._lock:
            events = list(self._streams.get(stream, []))
            if last_event_id == "0-0":
                return events[-limit:]
            seen = False
            new_events: list[dict[str, Any]] = []
            for event in events:
                if seen:
                    new_events.append(event)
                elif event.get("id") == last_event_id:
                    seen = True
            return new_events[-limit:]

    def queue_depth(self, queue_name: str) -> int:
        if self._client is not None:
            key = f"rq:queue:{queue_name}"
            return int(self._client.llen(key) or 0)
        return 0
