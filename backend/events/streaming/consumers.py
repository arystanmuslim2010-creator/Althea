from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from events.streaming.topics import (
    ALERTS_FEATURES_GENERATED,
    ALERTS_INGESTED,
    ALERTS_PRIORITIZED,
    ALERTS_SCORED,
    CASES_CREATED,
)


@dataclass(slots=True)
class ConsumerContext:
    tenant_id: str
    correlation_id: str
    payload: dict[str, Any]


class _BaseConsumer:
    topic_in: str
    topic_out: str | None = None

    def __init__(self, stream, repository) -> None:
        self._stream = stream
        self._repository = repository

    def handle_batch(self, messages: list[Any]) -> list[str]:
        emitted_ids: list[str] = []
        for message in messages:
            context = ConsumerContext(
                tenant_id=str(message.tenant_id),
                correlation_id=str(message.correlation_id),
                payload=dict(message.payload or {}),
            )
            next_payload = self.process(context)
            if self.topic_out and next_payload is not None:
                emitted_ids.append(
                    self._stream.publish(
                        topic=self.topic_out,
                        tenant_id=context.tenant_id,
                        payload=next_payload,
                        correlation_id=context.correlation_id,
                    )
                )
        return emitted_ids

    def process(self, context: ConsumerContext) -> dict[str, Any] | None:
        raise NotImplementedError


class FeatureServiceConsumer(_BaseConsumer):
    topic_in = ALERTS_INGESTED
    topic_out = ALERTS_FEATURES_GENERATED

    def __init__(self, stream, repository, feature_service, online_feature_store=None) -> None:
        super().__init__(stream, repository)
        self._feature_service = feature_service
        self._online_feature_store = online_feature_store

    def process(self, context: ConsumerContext) -> dict[str, Any] | None:
        run_id = str(context.payload.get("run_id") or "")
        alert_ids = [str(item) for item in (context.payload.get("alert_ids") or []) if str(item)]
        if not run_id or not alert_ids:
            return None

        rows = self._repository.list_alert_payloads_by_run(context.tenant_id, run_id, limit=500000)
        selected = [row for row in rows if str(row.get("alert_id")) in set(alert_ids)]
        if not selected:
            return None

        bundle = self._feature_service.generate_inference_features(pd.DataFrame(selected))
        alerts_df = bundle.get("alerts_df", pd.DataFrame())
        feature_matrix = bundle.get("feature_matrix", pd.DataFrame())
        if alerts_df.empty or feature_matrix.empty:
            return None

        schema_hash = str((bundle.get("feature_schema") or {}).get("schema_hash") or "")
        feature_version = str(context.payload.get("feature_version") or "v1")

        limit = min(len(alerts_df), len(feature_matrix))
        feature_rows: list[dict[str, Any]] = []
        for idx in range(limit):
            alert_id = str(alerts_df.iloc[idx].get("alert_id") or "")
            if not alert_id:
                continue
            payload = {key: value for key, value in feature_matrix.iloc[idx].to_dict().items()}
            payload["alert_id"] = alert_id
            feature_rows.append(payload)
            if self._online_feature_store is not None:
                self._online_feature_store.put_features(
                    tenant_id=context.tenant_id,
                    alert_id=alert_id,
                    version=feature_version,
                    features=payload,
                )

        if feature_rows:
            self._repository.store_feature_rows(
                tenant_id=context.tenant_id,
                run_id=run_id,
                feature_schema_hash=schema_hash,
                feature_rows=feature_rows,
            )

        return {
            "run_id": run_id,
            "alert_ids": alert_ids,
            "feature_version": feature_version,
            "feature_count": len(feature_rows),
            "rescore": bool(context.payload.get("rescore", False)),
            "target_model_version": context.payload.get("target_model_version"),
        }


class ModelScoringConsumer(_BaseConsumer):
    topic_in = ALERTS_FEATURES_GENERATED
    topic_out = ALERTS_SCORED

    def __init__(self, stream, repository, inference_service) -> None:
        super().__init__(stream, repository)
        self._inference_service = inference_service

    def process(self, context: ConsumerContext) -> dict[str, Any] | None:
        run_id = str(context.payload.get("run_id") or "")
        alert_ids = [str(item) for item in (context.payload.get("alert_ids") or []) if str(item)]
        if not run_id or not alert_ids:
            return None

        stored_rows = self._repository.list_feature_rows(context.tenant_id, run_id, limit=500000)
        selected_rows = [row for row in stored_rows if str(row.get("alert_id")) in set(alert_ids)]
        if not selected_rows:
            return None

        feature_frame = pd.DataFrame(selected_rows)
        if "alert_id" in feature_frame.columns:
            feature_frame = feature_frame.drop(columns=["alert_id"])

        inference = self._inference_service.predict(
            tenant_id=context.tenant_id,
            feature_frame=feature_frame,
            strategy="active_approved",
        )
        scores = list(inference.get("scores") or [])

        payloads = self._repository.list_alert_payloads_by_run(context.tenant_id, run_id, limit=500000)
        by_id = {str(row.get("alert_id")): dict(row) for row in payloads}
        updates: list[dict[str, Any]] = []
        for idx, alert_id in enumerate(alert_ids):
            row = by_id.get(alert_id)
            if not row:
                continue
            row["risk_score"] = float(scores[idx]) if idx < len(scores) else float(row.get("risk_score", 0.0) or 0.0)
            row["risk_prob"] = max(0.0, min(1.0, float(row["risk_score"]) / 100.0))
            row["model_version"] = str(inference.get("model_version") or row.get("model_version") or "unknown")
            updates.append(row)

        if updates:
            self._repository.save_alert_payloads(tenant_id=context.tenant_id, run_id=run_id, records=updates)

        return {
            "run_id": run_id,
            "alert_ids": alert_ids,
            "model_version": str(inference.get("model_version") or "unknown"),
            "rescored": bool(context.payload.get("rescore", False)),
        }


class GovernanceConsumer(_BaseConsumer):
    topic_in = ALERTS_SCORED
    topic_out = ALERTS_PRIORITIZED

    def __init__(self, stream, repository, governance_service) -> None:
        super().__init__(stream, repository)
        self._governance_service = governance_service

    def process(self, context: ConsumerContext) -> dict[str, Any] | None:
        run_id = str(context.payload.get("run_id") or "")
        alert_ids = [str(item) for item in (context.payload.get("alert_ids") or []) if str(item)]
        if not run_id or not alert_ids:
            return None
        updates = self._governance_service.prioritize_alerts(
            tenant_id=context.tenant_id,
            run_id=run_id,
            alert_ids=alert_ids,
            model_version=str(context.payload.get("model_version") or "unknown"),
        )
        return {
            "run_id": run_id,
            "alert_ids": alert_ids,
            "model_version": str(context.payload.get("model_version") or "unknown"),
            "prioritized": len(updates),
        }


class CaseCreationConsumer(_BaseConsumer):
    topic_in = ALERTS_PRIORITIZED
    topic_out = CASES_CREATED

    def __init__(self, stream, repository, workflow_service) -> None:
        super().__init__(stream, repository)
        self._workflow_service = workflow_service

    def process(self, context: ConsumerContext) -> dict[str, Any] | None:
        run_id = str(context.payload.get("run_id") or "")
        alert_ids = [str(item) for item in (context.payload.get("alert_ids") or []) if str(item)]
        if not run_id or not alert_ids:
            return None

        created_case_ids: list[str] = []
        for alert_id in alert_ids:
            case_id = self._workflow_service.create_case_from_alert(
                tenant_id=context.tenant_id,
                alert_id=alert_id,
                run_id=run_id,
                actor="streaming-system",
            )
            if case_id:
                created_case_ids.append(case_id)

        return {
            "run_id": run_id,
            "alert_ids": alert_ids,
            "case_ids": created_case_ids,
            "created_count": len(created_case_ids),
        }


class StreamingPipelineOrchestrator:
    """Executes chained topic consumers for replay/backfill processing."""

    def __init__(
        self,
        stream,
        feature_consumer: FeatureServiceConsumer,
        scoring_consumer: ModelScoringConsumer,
        governance_consumer: GovernanceConsumer,
        case_consumer: CaseCreationConsumer,
        cursor_consumer: str = "streaming-worker",
    ) -> None:
        self._stream = stream
        self._feature_consumer = feature_consumer
        self._scoring_consumer = scoring_consumer
        self._governance_consumer = governance_consumer
        self._case_consumer = case_consumer
        self._cursor_consumer = str(cursor_consumer or "streaming-worker")
        self._cursor_by_topic: dict[str, str] = {
            ALERTS_INGESTED: self._load_cursor(ALERTS_INGESTED),
            ALERTS_FEATURES_GENERATED: self._load_cursor(ALERTS_FEATURES_GENERATED),
            ALERTS_SCORED: self._load_cursor(ALERTS_SCORED),
            ALERTS_PRIORITIZED: self._load_cursor(ALERTS_PRIORITIZED),
        }

    def _load_cursor(self, topic: str) -> str:
        existing = self._stream.get_cursor(topic, consumer=self._cursor_consumer, default="")
        if existing:
            return str(existing)
        latest = self._stream.latest_event_id(topic)
        if latest:
            self._stream.set_cursor(topic=topic, event_id=latest, consumer=self._cursor_consumer)
            return latest
        return "0-0"

    def trigger_ingestion(
        self,
        tenant_id: str,
        run_id: str,
        alert_ids: list[str],
        feature_version: str = "v1",
        correlation_id: str | None = None,
    ) -> str:
        return self._stream.publish(
            topic=ALERTS_INGESTED,
            tenant_id=tenant_id,
            payload={"run_id": run_id, "alert_ids": alert_ids, "feature_version": feature_version},
            correlation_id=correlation_id,
        )

    def process_once(self, batch_size: int = 500) -> dict[str, int]:
        counts = {
            ALERTS_INGESTED: 0,
            ALERTS_FEATURES_GENERATED: 0,
            ALERTS_SCORED: 0,
            ALERTS_PRIORITIZED: 0,
        }
        chain = [
            (ALERTS_INGESTED, self._feature_consumer),
            (ALERTS_FEATURES_GENERATED, self._scoring_consumer),
            (ALERTS_SCORED, self._governance_consumer),
            (ALERTS_PRIORITIZED, self._case_consumer),
        ]
        for topic, consumer in chain:
            cursor = self._cursor_by_topic.get(topic, "0-0")
            messages = self._stream.consume_after(topic=topic, last_event_id=cursor, limit=batch_size, block_ms=10)
            if messages:
                counts[topic] = len(messages)
                consumer.handle_batch(messages)
                self._cursor_by_topic[topic] = messages[-1].id
                self._stream.set_cursor(topic=topic, event_id=messages[-1].id, consumer=self._cursor_consumer)
        return counts

    def replay_topic(self, topic: str, start_event_id: str = "0-0", batch_size: int = 500) -> int:
        messages = self._stream.replay(topic=topic, start_event_id=start_event_id, batch_size=batch_size)
        if not messages:
            return 0
        if topic == ALERTS_INGESTED:
            self._feature_consumer.handle_batch(messages)
        elif topic == ALERTS_FEATURES_GENERATED:
            self._scoring_consumer.handle_batch(messages)
        elif topic == ALERTS_SCORED:
            self._governance_consumer.handle_batch(messages)
        elif topic == ALERTS_PRIORITIZED:
            self._case_consumer.handle_batch(messages)
        self._cursor_by_topic[topic] = messages[-1].id
        self._stream.set_cursor(topic=topic, event_id=messages[-1].id, consumer=self._cursor_consumer)
        return len(messages)
