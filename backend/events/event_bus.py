from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from core.observability import record_event
from storage.redis_cache import RedisCache


class EventName(str, Enum):
    ALERT_INGESTED = "alert_ingested"
    FEATURES_GENERATED = "features_generated"
    ALERT_SCORED = "alert_scored"
    ALERT_GOVERNED = "alert_governed"
    CASE_CREATED = "case_created"
    CASE_CLOSED = "case_closed"


@dataclass(slots=True)
class EventEnvelope:
    event_name: str
    tenant_id: str
    payload: dict[str, Any]
    correlation_id: str
    version: str
    emitted_at: str
    retry_count: int = 0


class EventBus:
    """Event bus abstraction with typed envelopes and dead-letter routing."""

    def __init__(self, cache: RedisCache, stream_name: str = "althea.events", dead_letter_stream: str = "althea.events.dlq") -> None:
        self._cache = cache
        self._stream_name = stream_name
        self._dead_letter_stream = dead_letter_stream

    def _build_envelope(
        self,
        event_name: str,
        tenant_id: str,
        payload: dict[str, Any],
        correlation_id: str | None,
        version: str,
        retry_count: int,
    ) -> EventEnvelope:
        return EventEnvelope(
            event_name=str(event_name),
            tenant_id=str(tenant_id),
            payload=dict(payload or {}),
            correlation_id=correlation_id or uuid.uuid4().hex,
            version=version,
            emitted_at=datetime.now(timezone.utc).isoformat(),
            retry_count=int(retry_count),
        )

    def publish(
        self,
        event_name: str | EventName,
        tenant_id: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
        version: str = "1.0",
        max_retries: int = 3,
    ) -> str:
        event_label = event_name.value if isinstance(event_name, EventName) else str(event_name)
        record_event(event_label)
        last_error: Exception | None = None
        for attempt in range(max(1, int(max_retries))):
            envelope = self._build_envelope(
                event_name=event_label,
                tenant_id=tenant_id,
                payload=payload,
                correlation_id=correlation_id,
                version=version,
                retry_count=attempt,
            )
            try:
                return self._cache.publish_event(self._stream_name, asdict(envelope))
            except Exception as exc:  # pragma: no cover - network failure path
                last_error = exc
                time.sleep(min(0.2 * (attempt + 1), 1.0))

        dlq_envelope = self._build_envelope(
            event_name=event_label,
            tenant_id=tenant_id,
            payload={**(payload or {}), "publish_error": str(last_error) if last_error else "unknown"},
            correlation_id=correlation_id,
            version=version,
            retry_count=max(0, int(max_retries)),
        )
        return self._cache.publish_event(self._dead_letter_stream, asdict(dlq_envelope))

    def recent(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._cache.read_events(self._stream_name, limit=limit)

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._cache.read_events(self._dead_letter_stream, limit=limit)

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
                handler(event.get("payload", {}))

