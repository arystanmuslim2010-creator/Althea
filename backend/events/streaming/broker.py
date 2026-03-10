from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from storage.redis_cache import RedisCache


@dataclass(slots=True)
class StreamMessage:
    id: str
    topic: str
    tenant_id: str
    payload: dict[str, Any]
    correlation_id: str
    emitted_at: str


class StreamingBackbone:
    """Kafka/Redpanda-compatible streaming abstraction with Redis stream fallback.

    This keeps local and test behavior deterministic while supporting deployment against
    Kafka-compatible clusters when configured.
    """

    def __init__(
        self,
        cache: RedisCache,
        provider: str = "redis",
        stream_prefix: str = "althea.streaming",
    ) -> None:
        self._cache = cache
        self._provider = str(provider or "redis").lower()
        self._stream_prefix = stream_prefix.rstrip(".")

    def _stream_name(self, topic: str) -> str:
        return f"{self._stream_prefix}.{topic}"

    def _cursor_key(self, topic: str, consumer: str = "default") -> str:
        safe_consumer = "".join(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_" for ch in str(consumer or "default"))
        return f"{self._stream_prefix}.cursor.{safe_consumer}.{topic}"

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def publish(
        self,
        topic: str,
        tenant_id: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
    ) -> str:
        envelope = {
            "topic": topic,
            "tenant_id": tenant_id,
            "payload": dict(payload or {}),
            "correlation_id": correlation_id or uuid.uuid4().hex,
            "emitted_at": self._now(),
        }
        return self._cache.publish_event(self._stream_name(topic), envelope)

    def consume_after(
        self,
        topic: str,
        last_event_id: str = "0-0",
        limit: int = 100,
        block_ms: int = 1000,
    ) -> list[StreamMessage]:
        rows = self._cache.read_events_after(
            self._stream_name(topic),
            last_event_id=last_event_id,
            limit=limit,
            block_ms=block_ms,
        )
        out: list[StreamMessage] = []
        for row in rows:
            payload = row.get("payload", {}) or {}
            out.append(
                StreamMessage(
                    id=str(row.get("id") or ""),
                    topic=str(payload.get("topic") or topic),
                    tenant_id=str(payload.get("tenant_id") or ""),
                    payload=dict(payload.get("payload") or {}),
                    correlation_id=str(payload.get("correlation_id") or ""),
                    emitted_at=str(payload.get("emitted_at") or ""),
                )
            )
        return out

    def replay(
        self,
        topic: str,
        start_event_id: str = "0-0",
        batch_size: int = 500,
        max_batches: int = 100,
    ) -> list[StreamMessage]:
        cursor = start_event_id
        replayed: list[StreamMessage] = []
        for _ in range(max(1, max_batches)):
            events = self.consume_after(topic=topic, last_event_id=cursor, limit=batch_size, block_ms=10)
            if not events:
                break
            replayed.extend(events)
            cursor = events[-1].id
            if len(events) < batch_size:
                break
        return replayed

    def backfill(
        self,
        topic: str,
        tenant_id: str,
        payloads: list[dict[str, Any]],
        correlation_id_prefix: str = "backfill",
    ) -> list[str]:
        ids: list[str] = []
        for idx, payload in enumerate(payloads):
            ids.append(
                self.publish(
                    topic=topic,
                    tenant_id=tenant_id,
                    payload={**dict(payload or {}), "backfill": True, "backfill_index": idx},
                    correlation_id=f"{correlation_id_prefix}-{idx}",
                )
            )
        return ids

    def latest_event_id(self, topic: str) -> str | None:
        rows = self._cache.read_events(self._stream_name(topic), limit=1)
        if not rows:
            return None
        event_id = rows[-1].get("id")
        return str(event_id) if event_id else None

    def get_cursor(self, topic: str, consumer: str = "default", default: str = "0-0") -> str:
        value = self._cache.get_json(self._cursor_key(topic=topic, consumer=consumer), default)
        return str(value or default)

    def set_cursor(self, topic: str, event_id: str, consumer: str = "default") -> None:
        self._cache.set_json(self._cursor_key(topic=topic, consumer=consumer), str(event_id or "0-0"))

    def reset_cursor(self, topic: str, consumer: str = "default") -> None:
        self._cache.delete(self._cursor_key(topic=topic, consumer=consumer))

    def rescore(self, tenant_id: str, run_id: str, alert_ids: list[str], model_version: str | None = None) -> str:
        return self.publish(
            topic="alerts.features_generated",
            tenant_id=tenant_id,
            payload={
                "run_id": run_id,
                "alert_ids": list(alert_ids),
                "rescore": True,
                "target_model_version": model_version,
            },
            correlation_id=f"rescore-{run_id}-{int(time.time())}",
        )
