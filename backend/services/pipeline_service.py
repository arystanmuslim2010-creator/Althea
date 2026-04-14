from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

from core.config import Settings
from core.observability import (
    get_legacy_path_access_snapshot,
    get_recent_ingestion_summaries,
    get_rollout_metrics_snapshot,
    record_ingestion_attempt,
    record_ingestion_path_used,
    record_ingestion_summary,
    record_primary_ingestion_mode,
    record_queue_depth,
)
from events.event_bus import EventBus
from models.inference_service import InferenceService
from services.alert_ingestion_service import AlertIngestionService, AlertIngestionValidationError
from services.feature_adapter import AlertFeatureAdapter
from services.feature_service import EnterpriseFeatureService
from services.governance_service import GovernanceService
from services.ingestion_service import EnterpriseIngestionService
from services.job_queue_service import JobQueueService
from services.model_monitoring_service import ModelMonitoringService
from services.rollout_evaluator import RolloutEvaluator
from storage.postgres_repository import EnterpriseRepository

logger = logging.getLogger("althea.pipeline")


class PipelineService:
    _SCALE_WARNING_LATENCY_MS = 30000
    _SCALE_WARNING_PROCESSING_TIME_PER_ALERT_MS = 250.0

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
        streaming_orchestrator=None,
        alert_ingestion_service: AlertIngestionService | None = None,
        feature_adapter: AlertFeatureAdapter | None = None,
        rollout_evaluator: RolloutEvaluator | None = None,
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
        self._streaming_orchestrator = streaming_orchestrator
        self._alert_ingestion_service = alert_ingestion_service or AlertIngestionService()
        self._feature_adapter = feature_adapter or AlertFeatureAdapter()
        self._rollout_evaluator = rollout_evaluator or RolloutEvaluator()
        self._last_alert_ingestion_result: dict[str, Any] = {}
        self._runtime_primary_ingestion_mode_override: str | None = None
        self._last_primary_ingestion_mode: str | None = None

    @staticmethod
    def _normalize_primary_ingestion_mode(mode: str | None) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized not in {"legacy", "alert_jsonl"}:
            return "legacy"
        return normalized

    def _is_alert_jsonl_ingestion_enabled(self) -> bool:
        return bool(getattr(self._settings, "enable_alert_jsonl_ingestion", False))

    def get_primary_ingestion_mode(self) -> str:
        configured_mode = self._normalize_primary_ingestion_mode(
            self._runtime_primary_ingestion_mode_override or getattr(self._settings, "primary_ingestion_mode", "alert_jsonl")
        )
        if configured_mode != self._last_primary_ingestion_mode:
            logger.info(
                "Primary ingestion mode active",
                extra={
                    "primary_ingestion_mode": configured_mode,
                    "previous_primary_ingestion_mode": self._last_primary_ingestion_mode,
                },
            )
            self._last_primary_ingestion_mode = configured_mode
        record_primary_ingestion_mode(configured_mode)
        return configured_mode

    def set_runtime_primary_ingestion_mode(self, mode: str) -> dict[str, Any]:
        resolved = self._normalize_primary_ingestion_mode(mode)
        previous = self.get_primary_ingestion_mode()
        self._runtime_primary_ingestion_mode_override = resolved
        active = self.get_primary_ingestion_mode()
        logger.warning(
            "Primary ingestion mode changed at runtime",
            extra={"previous_primary_ingestion_mode": previous, "primary_ingestion_mode": active},
        )
        return {
            "status": "ok",
            "primary_ingestion_mode": active,
            "previous_primary_ingestion_mode": previous,
            "source": "runtime_override",
        }

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
        record_queue_depth(self._job_queue.queue_depth(self._settings.rq_queue_name))
        cached = self._job_queue.get_status(job_id)
        job = self._repository.get_pipeline_job(tenant_id, job_id)
        if job:
            db_status = str(job.get("status") or "").lower().strip()
            if not cached:
                return job

            cached_status = str(cached.get("status") or "").lower().strip()
            # A stale worker can emit "discarded" when API and worker are briefly out of sync.
            # If DB still tracks this job as active/terminal, treat DB as source of truth.
            if cached_status == "discarded" and db_status not in {"unknown", ""}:
                return job

            # Prefer more advanced cached progression when DB still lags behind queue state.
            if cached_status in {"running", "completed", "failed"} and db_status in {"queued", "running"}:
                merged = dict(job)
                merged.update(cached)
                return merged
            return job
        return cached or {"job_id": job_id, "status": "unknown"}

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
        try:
            self._job_queue.enqueue(
                import_path="workers.pipeline_worker.run_pipeline_job",
                kwargs={"job_id": job_id, "tenant_id": tenant_id, "user_scope": user_scope},
                queue_mode=self._settings.queue_mode,
                redis_url=self._settings.redis_url,
                queue_name=self._settings.rq_queue_name,
                job_timeout_seconds=self._settings.rq_job_timeout_seconds,
            )
        except Exception as exc:
            # Provide a deterministic, user-actionable error instead of generic 500 when queue infra is unavailable.
            self._repository.update_pipeline_job(
                job_id,
                tenant_id=tenant_id,
                status="failed",
                error_message=str(exc),
                completed_at=pd.Timestamp.utcnow().to_pydatetime(),
            )
            self._repository.upsert_runtime_context(tenant_id, user_scope, active_job_id=None)
            self._job_queue.set_status(job_id, {"job_id": job_id, "status": "failed", "detail": str(exc)})
            raise ValueError(
                "Pipeline queue is unavailable. Start Redis and pipeline worker, then retry."
            ) from exc
        record_queue_depth(self._job_queue.queue_depth(self._settings.rq_queue_name))
        return payload

    def execute_pipeline_job(self, job_id: str, tenant_id: str, user_scope: str) -> dict[str, Any]:
        from workers.pipeline_worker import execute_pipeline_job

        return execute_pipeline_job(service=self, job_id=job_id, tenant_id=tenant_id, user_scope=user_scope)

    def run_pipeline_stream(
        self,
        tenant_id: str,
        source_chunks: Iterable[pd.DataFrame],
    ) -> tuple[str, int, str, list[float]]:
        run_id = f"run_{uuid.uuid4().hex[:16]}"
        total_persisted = 0
        model_version = "unknown"
        score_sample: list[float] = []
        max_monitoring_scores = 50000

        for chunk in source_chunks:
            if chunk is None or chunk.empty:
                continue
            chunk_run_id, persisted, version, scores = self._run_pipeline(chunk, tenant_id=tenant_id, run_id=run_id)
            run_id = chunk_run_id
            total_persisted += int(persisted)
            model_version = version or model_version
            if len(score_sample) < max_monitoring_scores:
                remaining = max_monitoring_scores - len(score_sample)
                score_sample.extend(scores[:remaining])

        if total_persisted <= 0:
            raise ValueError("Pipeline produced no alerts from streamed dataset.")
        return run_id, total_persisted, model_version, score_sample

    def run_alert_ingestion_pipeline(
        self,
        file_path: str,
        run_id: str,
        tenant_id: str | None = None,
        user_scope: str = "public",
        canary_override: bool = False,
        upload_row_count: int | None = None,
    ) -> dict[str, Any]:
        if not self._is_alert_jsonl_ingestion_enabled():
            raise RuntimeError("alert_jsonl_ingestion_disabled")

        resolved_tenant_id = str(tenant_id or self._settings.default_tenant_id)
        resolved_user_scope = str(user_scope or "public")
        self._repository.set_tenant_context(resolved_tenant_id)
        configured_max_upload_rows = int(getattr(self._settings, "alert_jsonl_max_upload_rows", 1000))
        observed_upload_rows = max(0, int(upload_row_count or 0))
        effective_max_upload_rows = max(1, configured_max_upload_rows)

        logger.info(
            json.dumps(
                {
                    "event": "alert_jsonl_pipeline_started",
                    "tenant_id": resolved_tenant_id,
                    "run_id": run_id,
                    "strict_validation": bool(self._settings.strict_ingestion_validation),
                    "max_upload_rows": int(effective_max_upload_rows),
                    "legacy_emergency_override_enabled": bool(getattr(self._settings, "enable_legacy_ingestion", False)),
                    "upload_row_count": observed_upload_rows,
                    "primary_ingestion_mode": self.get_primary_ingestion_mode(),
                },
                ensure_ascii=True,
            )
        )
        record_ingestion_attempt(source_system="alert_jsonl", strict_mode=bool(self._settings.strict_ingestion_validation))
        known_recent_alert_ids = set(
            self._repository.list_recent_alert_ids(tenant_id=resolved_tenant_id, limit=5000)
        )

        try:
            summary = self._alert_ingestion_service.ingest_jsonl(
                file_path=file_path,
                run_id=run_id,
                strict_validation=self._settings.strict_ingestion_validation,
                max_upload_rows=int(effective_max_upload_rows),
                allow_ibm_amlsim_import=bool(getattr(self._settings, "enable_ibm_amlsim_import", False)),
                known_recent_alert_ids=known_recent_alert_ids,
            )
        except AlertIngestionValidationError as exc:
            summary = dict(exc.summary or {})
            summary.setdefault("run_id", run_id)
            record_ingestion_summary(summary)
            self._last_alert_ingestion_result = {k: v for k, v in summary.items() if k != "alerts"}
            logger.warning(
                json.dumps(
                    {
                        "event": "alert_jsonl_pipeline_validation_failed",
                        "tenant_id": resolved_tenant_id,
                        "run_id": run_id,
                        "status": str(summary.get("status") or "failed_validation"),
                        "source_system": str(summary.get("source_system") or "unknown"),
                        "strict_mode": bool(summary.get("strict_mode_used", self._settings.strict_ingestion_validation)),
                        "total_rows": int(summary.get("total_rows") or 0),
                        "success_count": int(summary.get("success_count") or 0),
                        "failed_count": int(summary.get("failed_count") or 0),
                        "warning_count": int(summary.get("warning_count") or 0),
                        "failure_reason_category": str(summary.get("failure_reason_category") or "validation_failure"),
                    },
                    ensure_ascii=True,
                )
            )
            raise
        alerts = list(summary.get("alerts") or [])
        success_count = int(summary.get("success_count") or 0)
        failed_count = int(summary.get("failed_count") or 0)
        total_rows = int(summary.get("total_rows") or 0)
        warning_count = int(summary.get("warning_count") or 0)
        source_system = str(summary.get("source_system") or "unknown")
        status = str(summary.get("status") or ("accepted" if success_count > 0 else "rejected"))
        elapsed_ms = int(summary.get("elapsed_ms") or 0)
        ingested_transaction_count = int(summary.get("ingested_transaction_count") or 0)
        failure_reason_category = str(summary.get("failure_reason_category") or "none")
        critical_issue_count = int(summary.get("critical_issue_count") or 0)
        processing_time_per_alert_ms = float(summary.get("processing_time_per_alert_ms") or 0.0)

        persisted_raw = 0
        persisted_scored = 0
        model_version = "unknown"
        monitoring_metrics = {"psi": 0.0, "drift_score": 0.0, "degradation_flag": False}

        if alerts:
            persisted_raw = self._repository.save_alert_payloads(
                tenant_id=resolved_tenant_id,
                run_id=run_id,
                records=alerts,
            )
            feature_input = self._feature_adapter.alerts_to_dataframe(alerts)
            if not feature_input.empty:
                try:
                    _, persisted_scored, model_version, score_values = self._run_pipeline(
                        source_df=feature_input,
                        tenant_id=resolved_tenant_id,
                        run_id=run_id,
                        run_source="AlertJSONL",
                    )
                    monitoring = self._model_monitoring_service.record_run_monitoring(
                        tenant_id=resolved_tenant_id,
                        run_id=run_id,
                        model_version=model_version,
                        scores=score_values,
                    )
                    monitoring_metrics = dict(monitoring.get("metrics", {}))
                    self._publish_pipeline_events(
                        tenant_id=resolved_tenant_id,
                        job_id=f"alert_jsonl_{run_id}",
                        run_id=run_id,
                        alert_count=persisted_scored,
                        model_version=model_version,
                        monitoring_metrics=monitoring_metrics,
                    )
                except Exception as exc:
                    logger.exception(
                        "Alert JSONL scoring pipeline failed after successful ingestion",
                        extra={
                            "tenant_id": resolved_tenant_id,
                            "run_id": run_id,
                            "error": str(exc),
                        },
                    )

        if success_count > 0:
            self._repository.upsert_runtime_context(
                tenant_id=resolved_tenant_id,
                user_scope=resolved_user_scope,
                active_run_id=run_id,
                run_source="AlertJSONL",
                row_count=max(persisted_scored, persisted_raw, success_count),
                active_job_id=None,
            )

        summary_payload = {
            **summary,
            "source_system": source_system,
            "status": status,
            "failure_reason_category": failure_reason_category,
            "warning_count": warning_count,
            "elapsed_ms": elapsed_ms,
            "ingested_transaction_count": ingested_transaction_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "rollout_mode": "full",
            "canary_override_used": False,
            "critical_issue_count": critical_issue_count,
            "processing_time_per_alert_ms": processing_time_per_alert_ms,
        }
        record_ingestion_summary(summary_payload)
        record_ingestion_path_used(
            ingestion_path="alert_jsonl",
            primary_mode=self.get_primary_ingestion_mode(),
            status=status,
            alerts_ingested=int(summary.get("ingested_alert_count") or success_count),
        )
        self._last_alert_ingestion_result = {k: v for k, v in summary_payload.items() if k != "alerts"}
        if elapsed_ms >= self._SCALE_WARNING_LATENCY_MS:
            logger.warning(
                "Alert ingestion latency exceeded scale warning threshold",
                extra={"run_id": run_id, "elapsed_ms": elapsed_ms, "threshold_ms": self._SCALE_WARNING_LATENCY_MS},
            )
        if processing_time_per_alert_ms >= self._SCALE_WARNING_PROCESSING_TIME_PER_ALERT_MS:
            logger.warning(
                "Alert ingestion processing time per alert exceeded threshold",
                extra={
                    "run_id": run_id,
                    "processing_time_per_alert_ms": processing_time_per_alert_ms,
                    "threshold_ms": self._SCALE_WARNING_PROCESSING_TIME_PER_ALERT_MS,
                },
            )
        if success_count > 0 and persisted_raw <= 0:
            logger.warning(
                "Alert ingestion persistence anomaly detected",
                extra={"run_id": run_id, "success_count": success_count, "persisted_raw": persisted_raw},
            )
        rollout_snapshot = get_rollout_metrics_snapshot(window_runs=20, source_system="alert_jsonl")
        rollout_decision = self._rollout_evaluator.evaluate(
            metrics_snapshot=rollout_snapshot,
            recent_runs=get_recent_ingestion_summaries(limit=20, source_system="alert_jsonl"),
        )

        logger.info(
            json.dumps(
                {
                    "event": "alert_jsonl_pipeline_completed",
                    "tenant_id": resolved_tenant_id,
                    "run_id": run_id,
                    "status": status,
                    "source_system": source_system,
                    "strict_mode": bool(summary.get("strict_mode_used", self._settings.strict_ingestion_validation)),
                    "total_rows": total_rows,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "warning_count": warning_count,
                    "elapsed_ms": elapsed_ms,
                    "ingested_transaction_count": ingested_transaction_count,
                    "failure_reason_category": failure_reason_category,
                    "critical_issue_count": critical_issue_count,
                    "rollout_mode": "full",
                    "canary_override": False,
                    "persisted_raw": persisted_raw,
                    "persisted_scored": persisted_scored,
                    "model_version": model_version,
                    "rollout_decision": str(rollout_decision.get("decision") or "HOLD"),
                },
                ensure_ascii=True,
            )
        )

        return {
            "run_id": run_id,
            "total_rows": total_rows,
            "success_count": success_count,
            "failed_count": failed_count,
            "warning_count": warning_count,
            "strict_mode_used": bool(summary.get("strict_mode_used", self._settings.strict_ingestion_validation)),
            "source_system": source_system,
            "elapsed_ms": elapsed_ms,
            "status": status,
            "failure_reason_category": failure_reason_category,
            "ingested_alert_count": int(summary.get("ingested_alert_count") or success_count),
            "ingested_transaction_count": ingested_transaction_count,
            "data_quality_inconsistency_count": int(summary.get("data_quality_inconsistency_count") or 0),
            "data_quality_counts": dict(summary.get("data_quality_counts") or {}),
            "persisted_raw": int(persisted_raw),
            "persisted_scored": int(persisted_scored),
            "rollout_mode": "full",
            "canary_override_used": False,
            "critical_issue_count": critical_issue_count,
            "critical_data_quality_issues": list(summary.get("critical_data_quality_issues") or []),
            "processing_time_per_alert_ms": processing_time_per_alert_ms,
            "rollout_decision": str(rollout_decision.get("decision") or "HOLD"),
        }

    def get_rollout_status(self, tenant_id: str, window_runs: int = 20) -> dict[str, Any]:
        if not str(tenant_id or "").strip():
            raise ValueError("tenant_id is required")
        primary_mode = self.get_primary_ingestion_mode()
        ingestion_enabled = self._is_alert_jsonl_ingestion_enabled()
        metrics_snapshot = get_rollout_metrics_snapshot(window_runs=window_runs, source_system="alert_jsonl")
        recent_runs = get_recent_ingestion_summaries(limit=window_runs, source_system="alert_jsonl")
        legacy_snapshot = get_legacy_path_access_snapshot(limit=200)
        decision_payload = self._rollout_evaluator.evaluate(metrics_snapshot=metrics_snapshot, recent_runs=recent_runs)
        last_result = dict(self._last_alert_ingestion_result or {})
        last_result.pop("alerts", None)
        recent_failures = sum(
            1
            for row in recent_runs
            if str(row.get("status") or "").strip().lower() in {"rejected", "failed_validation"}
        )
        warning_total = sum(max(0, int(row.get("warning_count") or 0)) for row in recent_runs)
        legacy_enabled = bool(getattr(self._settings, "enable_legacy_ingestion", False))
        decision = str(decision_payload.get("decision") or "HOLD")
        new_ingestion_healthy = bool(ingestion_enabled) and decision != "ROLLBACK"
        return {
            "primary_ingestion_mode": primary_mode,
            "rollout_mode": "full",
            "ingestion_enabled": ingestion_enabled,
            "strict_validation_enabled": bool(getattr(self._settings, "strict_ingestion_validation", False)),
            "max_upload_rows": int(getattr(self._settings, "alert_jsonl_max_upload_rows", 1000)),
            "last_ingestion_result": last_result,
            "decision": decision,
            "reasons": list(decision_payload.get("reasons") or []),
            "metrics_snapshot": dict(decision_payload.get("metrics_snapshot") or {}),
            "thresholds": dict(decision_payload.get("thresholds") or {}),
            "legacy_ingestion_enabled": legacy_enabled,
            "legacy_disabled": not legacy_enabled,
            "blocked_legacy_attempts_recent": int(legacy_snapshot.get("blocked_count") or 0),
            "legacy_access_attempts_recent": int(legacy_snapshot.get("attempt_count") or 0),
            "legacy_access_by_endpoint": dict(legacy_snapshot.get("by_endpoint") or {}),
            "recent_ingestion_failure_runs": int(recent_failures),
            "recent_ingestion_warning_count": int(warning_total),
            "new_ingestion_healthy": new_ingestion_healthy,
        }

    def get_finalization_status(self, tenant_id: str, window_runs: int = 20) -> dict[str, Any]:
        return self.get_rollout_status(tenant_id=tenant_id, window_runs=window_runs)

    @staticmethod
    def _is_demo_source(source: str | None) -> bool:
        normalized = str(source or "").strip().lower()
        return normalized in {"synthetic", "bankcsv", "demo", "generated_demo"}

    def _run_pipeline(
        self,
        source_df: pd.DataFrame,
        tenant_id: str,
        run_id: str | None = None,
        run_source: str | None = None,
    ) -> tuple[str, int, str, list[float]]:
        # Step 1: Feature generation
        feature_bundle = self._feature_service.generate_features_batch(source_df)
        alerts_df = feature_bundle.get("alerts_df", pd.DataFrame()).copy()
        feature_matrix = feature_bundle.get("feature_matrix", pd.DataFrame()).copy()
        if alerts_df.empty or feature_matrix.empty:
            raise ValueError("Feature generation produced no records.")

        # Step 2: Escalation scoring
        inference = self._inference_service.predict(
            tenant_id=tenant_id,
            feature_frame=feature_matrix,
            strategy="active_approved",
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

        # Step 3: Explanation assembly
        top_features_json: list[str] = []
        top_contrib_json: list[str] = []
        explain_json: list[str] = []
        ml_signals_json: list[str] = []
        for idx in range(len(alerts_df)):
            row_explain = explanations[idx] if idx < len(explanations) else {}
            feature_attribution = row_explain.get("feature_attribution", []) if isinstance(row_explain, dict) else []
            top_contrib_json.append(json.dumps(feature_attribution, ensure_ascii=True))
            top_features_json.append(
                json.dumps(
                    [item.get("feature") for item in feature_attribution if isinstance(item, dict) and item.get("feature")],
                    ensure_ascii=True,
                )
            )
            explain_json.append(
                json.dumps(
                    {
                        "base_prob": float(alerts_df["risk_prob"].iloc[idx]),
                        "model_version": model_version,
                        "contributions": feature_attribution,
                        "feature_attribution": feature_attribution,
                        "risk_reason_codes": row_explain.get("risk_reason_codes", []),
                        "explanation_method": row_explain.get("explanation_method", "unknown"),
                        "explanation_status": row_explain.get("explanation_status", "unknown"),
                        "explanation_warning": row_explain.get("explanation_warning"),
                        "explanation_warning_code": row_explain.get("explanation_warning_code"),
                    },
                    ensure_ascii=True,
                )
            )
            ml_signals_json.append(
                json.dumps(
                    {
                        "model_version": model_version,
                        "top_feature_contributions": feature_attribution,
                        "explanation_method": row_explain.get("explanation_method", "unknown"),
                        "explanation_status": row_explain.get("explanation_status", "unknown"),
                        "explanation_warning": row_explain.get("explanation_warning"),
                        "explanation_warning_code": row_explain.get("explanation_warning_code"),
                    },
                    ensure_ascii=True,
                )
            )
        alerts_df["top_feature_contributions_json"] = top_contrib_json
        alerts_df["top_features_json"] = top_features_json
        alerts_df["risk_explain_json"] = explain_json
        alerts_df["ml_signals_json"] = ml_signals_json
        alerts_df["rules_json"] = alerts_df.get("rules_json", "[]")
        alerts_df["rule_evidence_json"] = alerts_df.get("rule_evidence_json", "{}")

        # Step 4: Governance enforcement (ranking + policy)
        governed = self._governance_service.apply_governance(
            alerts_df,
            stabilize_for_demo=self._is_demo_source(run_source),
        )

        # Step 5: Persist
        resolved_run_id = run_id or f"run_{uuid.uuid4().hex[:16]}"
        decision_audit_records = self._build_decision_audit_records(
            tenant_id=tenant_id,
            run_id=resolved_run_id,
            alerts_df=governed,
            model_version=model_version,
        )
        if decision_audit_records:
            self._repository.save_decision_audit_records(
                tenant_id=tenant_id,
                records=decision_audit_records,
            )
        persisted = self._persist_outputs(
            tenant_id=tenant_id,
            run_id=resolved_run_id,
            alerts_df=governed,
            feature_matrix=feature_matrix,
        )
        score_values = [float(v) for v in governed.get("priority_score", governed["risk_score"]).fillna(0.0).tolist()]
        logger.info(
            json.dumps(
                {
                    "event": "pipeline_chunk_scored",
                    "tenant_id": tenant_id,
                    "pipeline_run_id": resolved_run_id,
                    "model_version": model_version,
                    "alert_id": str(governed["alert_id"].iloc[0]) if "alert_id" in governed.columns and len(governed) else None,
                    "alerts_in_chunk": int(len(governed)),
                },
                ensure_ascii=True,
            )
        )
        return resolved_run_id, persisted, model_version, score_values

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

    @staticmethod
    def _parse_json_object(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _build_decision_audit_records(
        self,
        tenant_id: str,
        run_id: str,
        alerts_df: pd.DataFrame,
        model_version: str,
    ) -> list[dict[str, Any]]:
        if alerts_df is None or alerts_df.empty:
            return []
        decided_at = datetime.now(timezone.utc)
        records: list[dict[str, Any]] = []
        for row in alerts_df.to_dict("records"):
            alert_id = str(row.get("alert_id") or "").strip()
            if not alert_id:
                continue
            compliance_flags = self._parse_json_object(row.get("compliance_flags_json"))
            signals = self._parse_json_object(row.get("ml_signals_json"))
            if not signals:
                signals = {
                    "model_version": str(row.get("model_version") or model_version or "unknown"),
                    "risk_reason_codes": row.get("risk_reason_codes") or [],
                    "top_feature_contributions": self._parse_json_object(row.get("risk_explain_json")).get("feature_attribution", []),
                }
            records.append(
                {
                    "tenant_id": tenant_id,
                    "alert_id": alert_id,
                    "run_id": run_id,
                    "model_version": str(row.get("model_version") or model_version or "unknown"),
                    "priority_score": row.get("priority_score", row.get("risk_score")),
                    "escalation_prob": row.get("risk_prob"),
                    "graph_risk_score": row.get("graph_risk_score"),
                    "similar_suspicious_strength": row.get("similar_suspicious_strength"),
                    "p50_hours": row.get("p50_hours"),
                    "p90_hours": row.get("p90_hours"),
                    "governance_status": row.get("governance_status"),
                    "queue_action": row.get("queue_action"),
                    "priority_bucket": row.get("alert_priority") or row.get("priority") or row.get("risk_band"),
                    "compliance_flags_json": compliance_flags,
                    "signals_json": signals,
                    "decided_at": decided_at,
                }
            )
        if records:
            logger.info(
                "Persisting decision audit batch",
                extra={"tenant_id": tenant_id, "run_id": run_id, "records": len(records)},
            )
        return records

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
                row["features_json"] = dict(feature_payload)
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
        if self._streaming_orchestrator is not None and self._settings.streaming_inline_processing:
            logger.info(
                "Skipping automatic streaming reprocessing for completed pipeline run",
                extra={"tenant_id": tenant_id, "run_id": run_id, "job_id": job_id},
            )

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
