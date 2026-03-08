from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd

from core.config import Settings
from core.observability import record_pipeline_run
from events.event_bus import EventBus
from services.feature_service import EnterpriseFeatureService
from services.job_queue_service import JobQueueService
from services.model_monitoring_service import ModelMonitoringService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from src import config as legacy_config
from src import health_monitor
from src.domain.schemas import OverlayInputError
from src.storage import Storage as LegacyStorage

logger = logging.getLogger("althea.pipeline")


class PipelineService:
    def __init__(
        self,
        settings: Settings,
        repository: EnterpriseRepository,
        object_storage: ObjectStorage,
        event_bus: EventBus,
        job_queue: JobQueueService,
        feature_service: EnterpriseFeatureService,
        model_monitoring_service: ModelMonitoringService,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._object_storage = object_storage
        self._event_bus = event_bus
        self._job_queue = job_queue
        self._feature_service = feature_service
        self._model_monitoring_service = model_monitoring_service
        self._legacy_storage = LegacyStorage(db_path=str(settings.legacy_sqlite_path))

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
        run_meta = {}
        try:
            for run in self._repository.list_pipeline_runs(tenant_id, limit=200):
                if run.get("run_id") == run_id:
                    run_meta = run
                    break
        except Exception:
            run_meta = {}
        if not run_meta:
            run_meta = self._legacy_storage.get_run(run_id) or {}
        return {
            "run_id": run_id,
            "source": context.get("run_source"),
            "dataset_hash": context.get("dataset_hash"),
            "row_count": run_meta.get("row_count") or context.get("row_count"),
            "job_id": context.get("active_job_id"),
        }

    def list_runs(self, tenant_id: str) -> list[dict[str, Any]]:
        try:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=50)
            if runs:
                return runs
        except Exception:
            pass
        return self._legacy_storage.list_runs(limit=50)

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
        job = self._repository.create_pipeline_job(
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
        context = self.get_runtime_context(tenant_id, user_scope)
        self._repository.update_pipeline_job(job_id, status="running", started_at=pd.Timestamp.utcnow().to_pydatetime())
        self._job_queue.set_status(job_id, {"job_id": job_id, "status": "running"})
        try:
            run_id = self._run_pipeline_for_context(context)
            alerts_df = self._legacy_storage.load_alerts_by_run(run_id)
            count = int(len(alerts_df))
            persisted = self._persist_enterprise_outputs(tenant_id=tenant_id, run_id=run_id, alerts_df=alerts_df)
            model_version = str(alerts_df.get("model_version", pd.Series(["unknown"])).iloc[0] if not alerts_df.empty else "unknown")
            monitoring = self._model_monitoring_service.record_run_monitoring(
                tenant_id=tenant_id,
                run_id=run_id,
                model_version=model_version,
                scores=[float(x) for x in alerts_df.get("risk_score", pd.Series([])).fillna(0.0).tolist()],
            )
            self._repository.update_pipeline_job(
                job_id,
                status="completed",
                run_id=run_id,
                row_count=count,
                completed_at=pd.Timestamp.utcnow().to_pydatetime(),
            )
            self._repository.upsert_runtime_context(
                tenant_id,
                user_scope,
                active_run_id=run_id,
                active_job_id=job_id,
            )
            self._publish_pipeline_events(
                tenant_id=tenant_id,
                job_id=job_id,
                run_id=run_id,
                alert_count=count,
                model_version=model_version,
                monitoring_metrics=monitoring.get("metrics", {}),
            )
            record_pipeline_run(status="completed", duration_seconds=time.perf_counter() - started, alerts_processed=persisted)
            payload = {"job_id": job_id, "status": "completed", "run_id": run_id, "alerts": count}
            self._job_queue.set_status(job_id, payload)
            return payload
        except Exception as exc:
            record_pipeline_run(status="failed", duration_seconds=time.perf_counter() - started, alerts_processed=0)
            self._repository.update_pipeline_job(
                job_id,
                status="failed",
                error_message=str(exc),
                completed_at=pd.Timestamp.utcnow().to_pydatetime(),
            )
            payload = {"job_id": job_id, "status": "failed", "detail": str(exc)}
            self._job_queue.set_status(job_id, payload)
            raise

    def _load_dataframe(self, context: dict[str, Any]) -> pd.DataFrame:
        artifact_uri = context.get("dataset_artifact_uri")
        if not artifact_uri:
            raise ValueError("No staged dataset found.")
        return pd.read_csv(BytesIO(self._object_storage.get_bytes(artifact_uri)))

    def _dataset_hash_from_df(self, df: pd.DataFrame) -> str:
        raw = df.head(5000).to_csv(index=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]

    def _ensure_risk_band(self, df_in: pd.DataFrame) -> pd.DataFrame:
        if "risk_score" not in df_in.columns:
            return df_in
        t1 = int(getattr(legacy_config, "RISK_BAND_T1", 40))
        t2 = int(getattr(legacy_config, "RISK_BAND_T2", 70))
        t3 = int(getattr(legacy_config, "RISK_BAND_T3", 90))
        df_out = df_in.copy()
        rs = pd.to_numeric(df_out["risk_score"], errors="coerce").fillna(0.0)
        df_out["risk_band"] = np.select(
            [rs < t1, rs < t2, rs < t3],
            ["LOW", "MEDIUM", "HIGH"],
            default="CRITICAL",
        )
        return df_out

    def _run_pipeline_for_context(self, context: dict[str, Any]) -> str:
        df = self._load_dataframe(context)
        run_source = context.get("run_source", "Unknown")

        if getattr(legacy_config, "OVERLAY_MODE", False) and run_source == "CSV":
            raise OverlayInputError("Overlay requires alert-level input from AML monitoring systems.")

        from src.pipeline import run_pipeline as run_overlay_pipeline

        source = "csv" if run_source in ("BankCSV", "CSV") and context.get("raw_artifact_uri") else "dataframe"
        input_bytes = self._object_storage.get_bytes(context["raw_artifact_uri"]) if source == "csv" else None
        run_id = run_overlay_pipeline(
            source=source,
            config_overrides={"policy_version": getattr(legacy_config, "CURRENT_POLICY_VERSION", "1.0")},
            input_df=df,
            input_bytes=input_bytes,
            storage=self._legacy_storage,
            data_dir=self._settings.data_dir,
            reports_dir=self._settings.reports_dir,
            dead_letter_dir=self._settings.dead_letter_dir,
        )
        return run_id

    def _persist_enterprise_outputs(self, tenant_id: str, run_id: str, alerts_df: pd.DataFrame) -> int:
        if alerts_df is None or alerts_df.empty:
            return 0

        total_persisted = 0
        batch_size = max(1000, int(self._settings.pipeline_batch_size))

        for start in range(0, len(alerts_df), batch_size):
            chunk = alerts_df.iloc[start : start + batch_size]
            records: list[dict[str, Any]] = []
            feature_rows: list[dict[str, Any]] = []
            for record in chunk.to_dict("records"):
                row = dict(record)
                row["alert_id"] = str(row.get("alert_id", ""))
                row["user_id"] = str(row.get("user_id", ""))
                for json_col in ["risk_explain_json", "rules_json", "rule_evidence_json", "features_json"]:
                    if json_col in row and not isinstance(row[json_col], str):
                        row[json_col] = json.dumps(row[json_col])
                records.append(row)

                payload = row.get("features_json")
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                feature_rows.append({"alert_id": row.get("alert_id"), **(payload or {})})

            total_persisted += self._repository.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=records)

            if feature_rows:
                feature_frame = pd.DataFrame(feature_rows).fillna(0)
                schema = self._feature_service.validate_feature_schema(expected_schema={"columns": []}, df=feature_frame)
                schema_hash = schema.get("current_schema", {}).get("schema_hash", "")
                self._repository.store_feature_rows(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    feature_schema_hash=schema_hash,
                    feature_rows=feature_rows,
                )
        return total_persisted

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
        self._event_bus.publish("alert_ingested", tenant_id, base)
        self._event_bus.publish("features_generated", tenant_id, base)
        self._event_bus.publish("alert_scored", tenant_id, base)
        self._event_bus.publish("alert_governed", tenant_id, base)

    def compute_health(self, run_id: str) -> dict[str, Any]:
        alerts_df = self._legacy_storage.load_alerts_by_run(run_id or "")
        if alerts_df.empty or "risk_score" not in alerts_df.columns:
            return {"status": "N/A"}
        report = health_monitor.compute_health_report(alerts_df=alerts_df, daily_stats=[], baseline_df=None)
        return {"status": report.get("status", "N/A")}
