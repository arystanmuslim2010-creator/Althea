from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import numpy as np
import pandas as pd

from core.config import Settings
from core.observability import record_pipeline_run
from events.event_bus import EventBus
from models.inference_service import InferenceService
from services.feature_service import EnterpriseFeatureService
from services.governance_service import GovernanceService
from services.ingestion_service import EnterpriseIngestionService
from services.job_queue_service import JobQueueService
from services.model_monitoring_service import ModelMonitoringService
from storage.postgres_repository import EnterpriseRepository

logger = logging.getLogger("althea.pipeline")


class PipelineService:
    def __init__(
        self,
        settings: Settings,
        repository: EnterpriseRepository,
        event_bus: EventBus,
        job_queue: JobQueueService,
        ingestion_service: EnterpriseIngestionService,
        feature_service: EnterpriseFeatureService,
        inference_service: InferenceService,
        governance_service: GovernanceService,
        model_monitoring_service: ModelMonitoringService,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._event_bus = event_bus
        self._job_queue = job_queue
        self._ingestion_service = ingestion_service
        self._feature_service = feature_service
        self._inference_service = inference_service
        self._governance_service = governance_service
        self._model_monitoring_service = model_monitoring_service

    def get_runtime_context(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        return self._repository.get_runtime_context(tenant_id, user_scope)

    def clear_active_run(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        self._repository.clear_runtime_context(tenant_id, user_scope)
        return {"status": "cleared"}

    def get_run_info(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        context = self.get_runtime_context(tenant_id, user_scope)
        run_id = context.get("active_run_id")
        if not run_id:
            return {}
        run_meta = next((run for run in self._repository.list_pipeline_runs(tenant_id, limit=200) if run.get("run_id") == run_id), {})
        return {
            "run_id": run_id,
            "source": context.get("run_source"),
            "dataset_hash": context.get("dataset_hash"),
            "row_count": run_meta.get("row_count") or context.get("row_count"),
            "job_id": context.get("active_job_id"),
        }

    def list_runs(self, tenant_id: str) -> list[dict[str, Any]]:
        return self._repository.list_pipeline_runs(tenant_id, limit=50)

    def get_job_status(self, tenant_id: str, job_id: str) -> dict[str, Any]:
        cached = self._job_queue.get_status(job_id)
        if cached:
            return cached
        job = self._repository.get_pipeline_job(tenant_id, job_id)
        return job or {"job_id": job_id, "status": "unknown"}

    def enqueue_pipeline_run(self, tenant_id: str, user_scope: str, initiated_by: str | None) -> dict[str, Any]:
        context = self.get_runtime_context(tenant_id, user_scope)
        if not context.get("dataset_artifact_uri"):
            raise ValueError("No data loaded. Generate or upload data first.")

        job_id = f"job_{uuid.uuid4().hex[:16]}"
        self._repository.create_pipeline_job(
            {
                "id": job_id,
                "tenant_id": tenant_id,
                "initiated_by": initiated_by,
                "source": context.get("run_source") or "Unknown",
                "dataset_hash": context.get("dataset_hash") or "",
                "row_count": int(context.get("row_count") or 0),
                "status": "queued",
                "artifact_uri": context.get("dataset_artifact_uri"),
                "raw_artifact_uri": context.get("raw_artifact_uri"),
                "notes": "Queued from API",
                "created_at": pd.Timestamp.utcnow().to_pydatetime(),
            }
        )
        self._repository.upsert_runtime_context(tenant_id, user_scope, active_job_id=job_id)

        payload = {"job_id": job_id, "status": "queued", "run_id": None, "alerts": 0}
        self._job_queue.set_status(job_id, payload)
        self._job_queue.enqueue(
            import_path="workers.pipeline_worker.run_pipeline_job",
            kwargs={"job_id": job_id, "tenant_id": tenant_id, "user_scope": user_scope},
            queue_mode=self._settings.queue_mode,
            redis_url=self._settings.redis_url,
            queue_name=self._settings.rq_queue_name,
        )
        return payload

    def execute_pipeline_job(self, job_id: str, tenant_id: str, user_scope: str) -> dict[str, Any]:
        started = time.perf_counter()
        self._repository.update_pipeline_job(
            job_id,
            tenant_id=tenant_id,
            status="running",
            started_at=pd.Timestamp.utcnow().to_pydatetime(),
        )
        self._job_queue.set_status(job_id, {"job_id": job_id, "status": "running"})
        try:
            context = self.get_runtime_context(tenant_id, user_scope)
            runtime_df = self._ingestion_service.load_runtime_dataframe(context)
            run_id, persisted_count, model_version, score_values = self._run_pipeline(runtime_df, tenant_id=tenant_id)

            monitoring = self._model_monitoring_service.record_run_monitoring(
                tenant_id=tenant_id,
                run_id=run_id,
                model_version=model_version,
                scores=score_values,
            )
            self._repository.update_pipeline_job(
                job_id,
                tenant_id=tenant_id,
                status="completed",
                run_id=run_id,
                row_count=persisted_count,
                completed_at=pd.Timestamp.utcnow().to_pydatetime(),
            )
            self._repository.upsert_runtime_context(
                tenant_id=tenant_id,
                user_scope=user_scope,
                active_run_id=run_id,
                active_job_id=job_id,
            )
            self._publish_pipeline_events(
                tenant_id=tenant_id,
                job_id=job_id,
                run_id=run_id,
                alert_count=persisted_count,
                model_version=model_version,
                monitoring_metrics=monitoring.get("metrics", {}),
            )
            record_pipeline_run(
                status="completed",
                duration_seconds=time.perf_counter() - started,
                alerts_processed=persisted_count,
            )
            payload = {"job_id": job_id, "status": "completed", "run_id": run_id, "alerts": persisted_count}
            self._job_queue.set_status(job_id, payload)
            return payload
        except Exception as exc:
            record_pipeline_run(status="failed", duration_seconds=time.perf_counter() - started, alerts_processed=0)
            self._repository.update_pipeline_job(
                job_id,
                tenant_id=tenant_id,
                status="failed",
                error_message=str(exc),
                completed_at=pd.Timestamp.utcnow().to_pydatetime(),
            )
            payload = {"job_id": job_id, "status": "failed", "detail": str(exc)}
            self._job_queue.set_status(job_id, payload)
            raise

    def _run_pipeline(self, source_df: pd.DataFrame, tenant_id: str) -> tuple[str, int, str, list[float]]:
        feature_bundle = self._feature_service.generate_inference_features(source_df)
        alerts_df = feature_bundle.get("alerts_df", pd.DataFrame()).copy()
        feature_matrix = feature_bundle.get("feature_matrix", pd.DataFrame()).copy()
        if alerts_df.empty or feature_matrix.empty:
            raise ValueError("Feature generation produced no records.")

        inference = self._inference_service.predict(
            tenant_id=tenant_id,
            feature_frame=feature_matrix,
            strategy=self._settings.model_selection_strategy,
        )
        model_version = str(inference.get("model_version") or "unknown")
        scores = list(inference.get("scores") or [])
        explanations = list(inference.get("explanations") or [])

        if len(scores) < len(alerts_df):
            scores = scores + [0.0] * (len(alerts_df) - len(scores))
        scores = scores[: len(alerts_df)]
        alerts_df["risk_score"] = pd.to_numeric(pd.Series(scores), errors="coerce").fillna(0.0)
        alerts_df["risk_prob"] = np.clip(alerts_df["risk_score"] / 100.0, 0.0, 1.0)
        alerts_df["model_version"] = model_version

        top_features_json: list[str] = []
        top_contrib_json: list[str] = []
        explain_json: list[str] = []
        for idx in range(len(alerts_df)):
            row_explain = explanations[idx] if idx < len(explanations) else {}
            top = row_explain.get("top_features", []) if isinstance(row_explain, dict) else []
            top_contrib_json.append(json.dumps(top, ensure_ascii=True))
            top_features_json.append(
                json.dumps([item.get("feature") for item in top if isinstance(item, dict) and item.get("feature")], ensure_ascii=True)
            )
            explain_json.append(
                json.dumps(
                    {
                        "base_prob": float(alerts_df["risk_prob"].iloc[idx]),
                        "model_version": model_version,
                        "contributions": top,
                    },
                    ensure_ascii=True,
                )
            )

        alerts_df["top_feature_contributions_json"] = top_contrib_json
        alerts_df["top_features_json"] = top_features_json
        alerts_df["risk_explain_json"] = explain_json
        alerts_df["rules_json"] = alerts_df.get("rules_json", "[]")
        alerts_df["rule_evidence_json"] = alerts_df.get("rule_evidence_json", "{}")

        governed = self._governance_service.apply_governance(alerts_df)
        run_id = f"run_{uuid.uuid4().hex[:16]}"
        persisted = self._persist_outputs(
            tenant_id=tenant_id,
            run_id=run_id,
            alerts_df=governed,
            feature_matrix=feature_matrix,
        )
        score_values = [float(v) for v in governed["risk_score"].fillna(0.0).tolist()]
        return run_id, persisted, model_version, score_values

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating, float)):
            if np.isnan(value) or np.isinf(value):
                return None
            return float(value)
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return value

    def _persist_outputs(
        self,
        tenant_id: str,
        run_id: str,
        alerts_df: pd.DataFrame,
        feature_matrix: pd.DataFrame,
    ) -> int:
        if alerts_df.empty:
            return 0

        batch_size = max(1000, int(self._settings.pipeline_batch_size))
        total = 0
        schema = self._feature_service.validate_feature_schema(expected_schema={"columns": []}, df=feature_matrix)
        schema_hash = schema.get("current_schema", {}).get("schema_hash", "")

        for start in range(0, len(alerts_df), batch_size):
            alerts_chunk = alerts_df.iloc[start : start + batch_size]
            feature_chunk = feature_matrix.iloc[start : start + batch_size]
            records: list[dict[str, Any]] = []
            feature_rows: list[dict[str, Any]] = []

            for idx, (_, alert_row) in enumerate(alerts_chunk.iterrows()):
                row = {key: self._sanitize_value(value) for key, value in alert_row.to_dict().items()}
                alert_id = str(row.get("alert_id") or f"ALT{start+idx+1:06d}")
                row["alert_id"] = alert_id
                row["user_id"] = str(row.get("user_id") or "")

                feature_payload = {
                    key: self._sanitize_value(value)
                    for key, value in feature_chunk.iloc[idx].to_dict().items()
                }
                row["features_json"] = feature_payload
                records.append(row)
                feature_rows.append({"alert_id": alert_id, **feature_payload})

            total += self._repository.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=records)
            if feature_rows:
                self._repository.store_feature_rows(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    feature_schema_hash=schema_hash,
                    feature_rows=feature_rows,
                )
        return total

    def _publish_pipeline_events(
        self,
        tenant_id: str,
        job_id: str,
        run_id: str,
        alert_count: int,
        model_version: str,
        monitoring_metrics: dict[str, Any],
    ) -> None:
        base = {
            "job_id": job_id,
            "run_id": run_id,
            "alert_count": alert_count,
            "model_version": model_version,
            "psi_score": float(monitoring_metrics.get("psi", 0.0) or 0.0),
            "drift_score": float(monitoring_metrics.get("drift_score", 0.0) or 0.0),
            "degradation_flag": bool(monitoring_metrics.get("degradation_flag", False)),
        }
        self._event_bus.publish("alert_ingested", tenant_id, base, correlation_id=job_id, version="2.0")
        self._event_bus.publish("features_generated", tenant_id, base, correlation_id=job_id, version="2.0")
        self._event_bus.publish("alert_scored", tenant_id, base, correlation_id=job_id, version="2.0")
        self._event_bus.publish("alert_governed", tenant_id, base, correlation_id=job_id, version="2.0")

    def compute_health(self, run_id: str, tenant_id: str) -> dict[str, Any]:
        if not run_id:
            return {"status": "N/A"}
        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        if not payloads:
            return {"status": "N/A"}
        scores = np.asarray([float(item.get("risk_score", 0.0) or 0.0) for item in payloads], dtype=float)
        if scores.size == 0:
            return {"status": "N/A"}
        q95 = float(np.quantile(scores, 0.95))
        mean = float(np.mean(scores))
        if q95 >= 98 or mean >= 85:
            status = "warning"
        elif q95 >= 90 or mean >= 70:
            status = "stable"
        else:
            status = "healthy"
        return {"status": status, "mean_risk_score": mean, "p95_risk_score": q95}
