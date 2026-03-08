from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from io import BytesIO
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from core.config import Settings
from events.event_bus import EventBus
from services.feature_service import EnterpriseFeatureService
from services.job_queue_service import JobQueueService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from src import alert_governance, config as legacy_config, features, risk_governance, scoring
from src import health_monitor
from src.domain.schemas import OverlayInputError
from src.external_data import load_all_configured_sources
from src.governance.decision_logger import DecisionLogger
from src.queue_governance import apply_alert_governance
from src.rule_engine import aggregate_rule_score, run_all_rules
from src.services.scoring_service import ScoringService as LegacyScoringService
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
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._object_storage = object_storage
        self._event_bus = event_bus
        self._job_queue = job_queue
        self._feature_service = feature_service
        self._legacy_storage = LegacyStorage(db_path=str(settings.legacy_sqlite_path))
        self._legacy_scoring_service = LegacyScoringService()

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
            return self._legacy_storage.list_runs(limit=50)
        except Exception:
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
        if self._settings.queue_mode == "rq":
            self._job_queue.enqueue(
                import_path="workers.pipeline_worker.run_pipeline_job",
                kwargs={"job_id": job_id, "tenant_id": tenant_id, "user_scope": user_scope},
                queue_mode=self._settings.queue_mode,
                redis_url=self._settings.redis_url,
                queue_name=self._settings.rq_queue_name,
            )
            return payload
        result = self.execute_pipeline_job(job_id=job_id, tenant_id=tenant_id, user_scope=user_scope)
        return {"job_id": job_id, "status": result["status"], "run_id": result.get("run_id"), "alerts": result.get("alerts", 0)}

    def execute_pipeline_job(self, job_id: str, tenant_id: str, user_scope: str) -> dict[str, Any]:
        context = self.get_runtime_context(tenant_id, user_scope)
        self._repository.update_pipeline_job(job_id, status="running", started_at=pd.Timestamp.utcnow().to_pydatetime())
        self._job_queue.set_status(job_id, {"job_id": job_id, "status": "running"})
        try:
            run_id = self._run_pipeline_for_context(context)
            alerts_df = self._legacy_storage.load_alerts_by_run(run_id)
            count = int(len(alerts_df))
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
            self._publish_pipeline_events(tenant_id, job_id, run_id, count)
            payload = {"job_id": job_id, "status": "completed", "run_id": run_id, "alerts": count}
            self._job_queue.set_status(job_id, payload)
            return payload
        except Exception as exc:
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

        try:
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
        except ImportError:
            pass

        cfg = SimpleNamespace(**{name: getattr(legacy_config, name) for name in dir(legacy_config) if name.isupper()})
        try:
            loaded_external_sources = load_all_configured_sources()
        except Exception:
            loaded_external_sources = {}
        df, feature_groups = features.compute_behavioral_features(df, cfg)
        df = run_all_rules(df, cfg)
        df = aggregate_rule_score(df, cfg)
        all_feature_cols = feature_groups["all_feature_cols"]
        X = features.build_feature_matrix(df, all_feature_cols)
        df = self._legacy_scoring_service.run_anomaly_detection(df, X)
        models, calibrator = scoring.train_risk_engine(df, feature_groups)
        df = scoring.score_with_risk_engine(df, models, calibrator, external_sources=loaded_external_sources)
        df["risk_score_original"] = df["risk_score"].copy()
        df = risk_governance.apply_risk_governance(df)
        df["risk_score"] = df["risk_score_governed"].copy()
        df = risk_governance.stabilize_risk_scores(df, cfg)
        df["risk_score"] = df["risk_score_final"].copy()
        df = apply_alert_governance(df)
        df = alert_governance.apply_alert_suppression(df, cfg=cfg)
        if "governance_status" not in df.columns and "alert_id" in df.columns:
            df = apply_alert_governance(df)
        if "in_queue" not in df.columns and "governance_status" in df.columns:
            gov = df["governance_status"].astype(str).str.lower()
            df["in_queue"] = gov.isin(["eligible", "mandatory_review"])
        df = self._ensure_risk_band(df)
        if "priority" not in df.columns:
            df["priority"] = df["risk_band"].astype(str).str.lower() if "risk_band" in df.columns else "low"
        if "model_version" not in df.columns:
            df["model_version"] = str(getattr(legacy_config, "MODEL_VERSION", "v1.0"))
        DecisionLogger().log_decisions(df, model_version=str(getattr(legacy_config, "MODEL_VERSION", "v1.0")))
        df = self._legacy_scoring_service.generate_explainability_drivers(df)
        run_id = f"run_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        dataset_hash = context.get("dataset_hash") or self._dataset_hash_from_df(df)
        self._legacy_storage.save_run(run_id, run_source, dataset_hash, len(df))
        records = df.to_dict("records")
        clean_records = []
        for record in records:
            record["alert_id"] = str(record.get("alert_id", ""))
            record["user_id"] = str(record.get("user_id", ""))
            for json_col in ["risk_explain_json", "rules_json", "rule_evidence_json"]:
                if json_col in record and not isinstance(record[json_col], str):
                    record[json_col] = json.dumps(record[json_col])
            clean_records.append(record)
        self._legacy_storage.upsert_alerts(clean_records, run_id=run_id)
        return run_id

    def _publish_pipeline_events(self, tenant_id: str, job_id: str, run_id: str, alert_count: int) -> None:
        base = {"job_id": job_id, "run_id": run_id, "alert_count": alert_count}
        self._event_bus.publish("alert_ingested", tenant_id, base)
        self._event_bus.publish("alert_scored", tenant_id, base)
        self._event_bus.publish("alert_governed", tenant_id, base)

    def compute_health(self, run_id: str) -> dict[str, Any]:
        alerts_df = self._legacy_storage.load_alerts_by_run(run_id or "")
        if alerts_df.empty or "risk_score" not in alerts_df.columns:
            return {"status": "N/A"}
        report = health_monitor.compute_health_report(alerts_df=alerts_df, daily_stats=[], baseline_df=None)
        return {"status": report.get("status", "N/A")}
