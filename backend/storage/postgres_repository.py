from __future__ import annotations

import json
import math
import uuid
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint, create_engine, desc, event, func, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

try:  # pragma: no cover - optional for non-postgres tests
    from sqlalchemy.dialects.postgresql import insert as postgres_insert
except Exception:  # pragma: no cover
    postgres_insert = None

from storage.enrichment_models import EnrichmentBase

logger = logging.getLogger("althea.repository")
SCHEMA_VERSION = "20260405_0010_phase5_post_cutover_hardening"


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


DEFAULT_RBAC_PERMISSIONS: dict[str, list[str]] = {
    "analyst": [
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "work_cases",
    ],
    "investigator": [
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "reassign_alerts",
        "approve_escalations",
        "view_team_queue",
        "work_cases",
    ],
    "manager": [
        "view_all_alerts",
        "view_dashboards",
        "approve_sar_cases",
        "manager_approval",
        "work_cases",
    ],
    "admin": [
        "manage_users",
        "manage_roles",
        "view_system_logs",
        "view_all_alerts",
        "view_team_queue",
        "manager_approval",
        "work_cases",
    ],
}


class RuntimeContext(Base):
    __tablename__ = "runtime_contexts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    user_scope: Mapped[str] = mapped_column(String(128), index=True)
    actor: Mapped[str] = mapped_column(String(128), default="Analyst_1")
    active_run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    run_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dataset_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dataset_artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    raw_artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class PipelineRunRecord(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    initiated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(64))
    dataset_hash: Mapped[str] = mapped_column(String(128))
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="queued")
    artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    raw_artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AlertRecord(Base):
    __tablename__ = "alerts"
    __table_args__ = (UniqueConstraint("tenant_id", "alert_id", name="uq_alerts_tenant_alert_id"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    risk_score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    risk_band: Mapped[str | None] = mapped_column(String(32), nullable=True)
    priority: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(64), default="new", index=True)
    # Backward-compatibility bridge for legacy databases that still enforce NOT NULL payload_json.
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    source_system: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ingestion_run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    schema_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    evaluation_label_is_sar: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    ingestion_metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    explainability_data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class CaseRecord(Base):
    __tablename__ = "cases"

    case_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(64), default="open")
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    assigned_to: Mapped[str | None] = mapped_column(String(128), nullable=True)
    alert_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    immutable_timeline_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class UserRecord(Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    email: Mapped[str] = mapped_column(String(256), index=True)
    password_hash: Mapped[str] = mapped_column(String(512))
    role: Mapped[str] = mapped_column(String(64), default="analyst")
    team: Mapped[str] = mapped_column(String(128), default="default")
    idp_provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class UserSessionRecord(Base):
    __tablename__ = "user_sessions"

    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    refresh_token_hash: Mapped[str] = mapped_column(String(256))
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class RBACRoleRecord(Base):
    __tablename__ = "rbac_roles"
    __table_args__ = (UniqueConstraint("tenant_id", "role_name", name="uq_rbac_roles_tenant_role"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    role_name: Mapped[str] = mapped_column(String(64), index=True)
    permissions_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    is_system: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class UserRoleRecord(Base):
    __tablename__ = "user_roles"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id", "role_name", name="uq_user_roles_tenant_user_role"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    role_name: Mapped[str] = mapped_column(String(64), index=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class RolePermissionRecord(Base):
    __tablename__ = "role_permissions"
    __table_args__ = (UniqueConstraint("tenant_id", "role_name", "permission_name", name="uq_role_permissions_tenant_role_perm"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    role_name: Mapped[str] = mapped_column(String(64), index=True)
    permission_name: Mapped[str] = mapped_column(String(128), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class InvestigationLogRecord(Base):
    __tablename__ = "investigation_logs"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    case_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    alert_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(128))
    performed_by: Mapped[str] = mapped_column(String(128))
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class AuthAuditLogRecord(Base):
    __tablename__ = "auth_audit_logs"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    action: Mapped[str] = mapped_column(String(128))
    old_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    new_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class IdentityProviderRecord(Base):
    __tablename__ = "identity_provider"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_identity_provider_tenant_name"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    provider_type: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(128))
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class ExternalIdentityRecord(Base):
    __tablename__ = "external_identity"
    __table_args__ = (UniqueConstraint("tenant_id", "provider_id", "external_subject", name="uq_external_identity_subject"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    provider_id: Mapped[str] = mapped_column(String(128), index=True)
    external_subject: Mapped[str] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class PipelineJobCheckpointRecord(Base):
    __tablename__ = "pipeline_job_checkpoints"
    __table_args__ = (UniqueConstraint("tenant_id", "job_id", "chunk_index", name="uq_pipeline_checkpoint_chunk"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    job_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    chunk_index: Mapped[int] = mapped_column(Integer, index=True)
    processed_rows: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="completed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class AlertAssignmentRecord(Base):
    __tablename__ = "alerts_assignments"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    assigned_to: Mapped[str] = mapped_column(String(128))
    assigned_by: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(64), default="open")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


class AlertNoteRecord(Base):
    __tablename__ = "alert_notes"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(128))
    note_text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class ModelVersionRecord(Base):
    __tablename__ = "model_versions"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    model_version: Mapped[str] = mapped_column(String(128), index=True)
    training_dataset_hash: Mapped[str] = mapped_column(String(256))
    feature_schema_uri: Mapped[str] = mapped_column(String(512))
    feature_schema_hash: Mapped[str] = mapped_column(String(256))
    metrics_uri: Mapped[str] = mapped_column(String(512))
    artifact_uri: Mapped[str] = mapped_column(String(512))
    approval_status: Mapped[str] = mapped_column(String(64), default="pending")
    training_metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class FeatureStoreRecord(Base):
    __tablename__ = "feature_store"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    feature_schema_hash: Mapped[str] = mapped_column(String(256), index=True)
    features_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class ModelMonitoringRecord(Base):
    __tablename__ = "model_monitoring"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str] = mapped_column(String(128), index=True)
    model_version: Mapped[str] = mapped_column(String(128), index=True)
    psi_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    drift_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    degradation_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class AISummaryRecord(Base):
    __tablename__ = "ai_summaries"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    entity_type: Mapped[str] = mapped_column(String(32), index=True)
    entity_id: Mapped[str] = mapped_column(String(128), index=True)
    summary: Mapped[str] = mapped_column(Text)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class AlertOutcomeRecord(Base):
    __tablename__ = "alert_outcomes"
    __table_args__ = (UniqueConstraint("tenant_id", "alert_id", name="uq_alert_outcomes_tenant_alert"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    analyst_decision: Mapped[str] = mapped_column(String(64))
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    analyst_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    model_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    risk_score_at_decision: Mapped[float | None] = mapped_column(Float, nullable=True)
    sar_filed_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    qa_override: Mapped[bool] = mapped_column(Boolean, default=False)
    investigation_start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    investigation_end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    touch_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notes_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_label_status: Mapped[str] = mapped_column(String(32), default="final")
    final_label_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class DecisionAuditRecord(Base):
    __tablename__ = "decision_audit"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    model_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    priority_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    escalation_prob: Mapped[float | None] = mapped_column(Float, nullable=True)
    graph_risk_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    similar_suspicious_strength: Mapped[float | None] = mapped_column(Float, nullable=True)
    p50_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    p90_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    governance_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    queue_action: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    priority_bucket: Mapped[str | None] = mapped_column(String(32), nullable=True)
    compliance_flags_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    signals_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class GlobalPatternSignalRecord(Base):
    __tablename__ = "global_pattern_signals"
    __table_args__ = (UniqueConstraint("signal_type", "signal_hash", name="uq_global_pattern_signals_type_hash"),)

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    signal_type: Mapped[str] = mapped_column(String(64), index=True)
    signal_hash: Mapped[str] = mapped_column(String(256), index=True)
    tenant_count: Mapped[int] = mapped_column(Integer, default=1)
    alert_count: Mapped[int] = mapped_column(Integer, default=1)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EnterpriseRepository:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        connect_args = {}
        engine_kwargs: dict[str, Any] = {"future": True, "connect_args": connect_args, "pool_pre_ping": True}
        if database_url.startswith("sqlite"):
            # SQLite allows one writer at a time; timeout + WAL reduce transient "database is locked" errors in local dev.
            connect_args = {"check_same_thread": False, "timeout": 30}
        else:
            # Production-oriented defaults for PostgreSQL connection pooling.
            engine_kwargs.update({"pool_size": 10, "max_overflow": 20})
        engine_kwargs["connect_args"] = connect_args
        self.engine = create_engine(database_url, **engine_kwargs)
        if database_url.startswith("sqlite"):
            self._configure_sqlite_for_concurrency()
        self._session_factory = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False, future=True)
        Base.metadata.create_all(self.engine)
        EnrichmentBase.metadata.create_all(self.engine)
        self._ensure_schema_compatibility()

    def _configure_sqlite_for_concurrency(self) -> None:
        @event.listens_for(self.engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:  # type: ignore[no-redef]
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

    def _ensure_schema_compatibility(self) -> None:
        self._ensure_schema_version_tracking()
        self._log_active_alembic_revision()
        self._ensure_model_versions_columns()
        self._ensure_alerts_columns()
        self._ensure_alert_outcomes_columns()
        self._ensure_user_email_uniqueness()
        self._ensure_rbac_seed()
        self._ensure_role_permissions_seed()
        self._ensure_tier1_enterprise_tables()
        self._ensure_investigation_intelligence_tables()

    def _ensure_alert_outcomes_columns(self) -> None:
        required_columns = {
            "sar_filed_flag": "BOOLEAN",
            "qa_override": "BOOLEAN",
            "investigation_start_time": "TIMESTAMP",
            "investigation_end_time": "TIMESTAMP",
            "resolution_hours": "DOUBLE PRECISION",
            "touch_count": "INTEGER",
            "notes_count": "INTEGER",
            "final_label_status": "VARCHAR(32)",
            "final_label_timestamp": "TIMESTAMP",
        }
        with self.session() as session:
            if self._database_url.startswith("sqlite"):
                rows = session.execute(text("PRAGMA table_info(alert_outcomes)")).all()
                existing = {str(row[1]) for row in rows}
                for column, col_type in required_columns.items():
                    if column in existing:
                        continue
                    sqlite_type = "TEXT"
                    default_sql = ""
                    if column in {"sar_filed_flag", "qa_override"}:
                        sqlite_type = "INTEGER"
                        default_sql = " DEFAULT 0"
                    elif column in {"resolution_hours"}:
                        sqlite_type = "REAL"
                    elif column in {"touch_count", "notes_count"}:
                        sqlite_type = "INTEGER"
                    elif column == "final_label_status":
                        sqlite_type = "TEXT"
                        default_sql = " DEFAULT 'final'"
                    session.execute(text(f"ALTER TABLE alert_outcomes ADD COLUMN {column} {sqlite_type}{default_sql}"))
                return

            rows = session.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'alert_outcomes'
                    """
                )
            ).all()
            existing = {str(row[0]) for row in rows}
            for column, col_type in required_columns.items():
                if column in existing:
                    continue
                if column in {"sar_filed_flag", "qa_override"}:
                    session.execute(text(f"ALTER TABLE alert_outcomes ADD COLUMN {column} {col_type} DEFAULT FALSE"))
                elif column == "final_label_status":
                    session.execute(text("ALTER TABLE alert_outcomes ADD COLUMN final_label_status VARCHAR(32) DEFAULT 'final'"))
                else:
                    session.execute(text(f"ALTER TABLE alert_outcomes ADD COLUMN {column} {col_type}"))

    def _ensure_schema_version_tracking(self) -> None:
        with self.session() as session:
            if self._database_url.startswith("sqlite"):
                session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS schema_versions (
                            version VARCHAR(128) PRIMARY KEY,
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                )
                session.execute(
                    text("INSERT OR IGNORE INTO schema_versions(version) VALUES (:version)"),
                    {"version": SCHEMA_VERSION},
                )
                latest = session.execute(
                    text("SELECT version FROM schema_versions ORDER BY applied_at DESC LIMIT 1")
                ).scalar_one_or_none()
                logger.info("Active schema version", extra={"schema_version": str(latest or SCHEMA_VERSION)})
                return

            session.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS schema_versions (
                        version VARCHAR(128) PRIMARY KEY,
                        applied_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                )
            )
            session.execute(
                text(
                    """
                    INSERT INTO schema_versions(version)
                    VALUES (:version)
                    ON CONFLICT (version) DO NOTHING
                    """
                ),
                {"version": SCHEMA_VERSION},
            )
            latest = session.execute(
                text("SELECT version FROM schema_versions ORDER BY applied_at DESC LIMIT 1")
            ).scalar_one_or_none()
            logger.info("Active schema version", extra={"schema_version": str(latest or SCHEMA_VERSION)})

    def _log_active_alembic_revision(self) -> None:
        try:
            with self.session() as session:
                if self._database_url.startswith("sqlite"):
                    has_table = session.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table' AND name='alembic_version'")
                    ).scalar_one_or_none()
                    if not has_table:
                        return
                    revision = session.execute(text("SELECT version_num FROM alembic_version LIMIT 1")).scalar_one_or_none()
                    if revision:
                        logger.info("Active Alembic revision", extra={"alembic_revision": str(revision)})
                    return

                revision = session.execute(text("SELECT version_num FROM alembic_version LIMIT 1")).scalar_one_or_none()
                if revision:
                    logger.info("Active Alembic revision", extra={"alembic_revision": str(revision)})
        except Exception:
            # Optional visibility only: don't block repository startup if Alembic table is absent.
            return

    @property
    def _is_postgres(self) -> bool:
        return self.engine.dialect.name.startswith("postgres")

    def _ensure_model_versions_columns(self) -> None:
        # Backward-compatible online schema patch for older local databases.
        required_columns = {
            "training_metadata_json": "JSON",
            "approved_by": "VARCHAR(128)",
            "approved_at": "TIMESTAMP",
        }
        with self.session() as session:
            if self._database_url.startswith("sqlite"):
                rows = session.execute(text("PRAGMA table_info(model_versions)")).all()
                existing = {str(row[1]) for row in rows}
                for column, col_type in required_columns.items():
                    if column in existing:
                        continue
                    default_sql = " DEFAULT '{}' " if column == "training_metadata_json" else ""
                    session.execute(text(f"ALTER TABLE model_versions ADD COLUMN {column} {col_type}{default_sql}"))
                return

            rows = session.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'model_versions'
                    """
                )
            ).all()
            existing = {str(row[0]) for row in rows}
            for column, col_type in required_columns.items():
                if column in existing:
                    continue
                if column == "training_metadata_json":
                    session.execute(text("ALTER TABLE model_versions ADD COLUMN training_metadata_json JSONB DEFAULT '{}'::jsonb"))
                else:
                    session.execute(text(f"ALTER TABLE model_versions ADD COLUMN {column} {col_type}"))

    def _ensure_alerts_columns(self) -> None:
        # Compatibility fallback only.
        # Canonical schema path is Alembic migrations; this keeps legacy/local DBs online-safe.
        required_columns = {
            "alert_id": "VARCHAR(128)",
            "risk_score": "DOUBLE PRECISION",
            "risk_band": "VARCHAR(32)",
            "priority": "VARCHAR(32)",
            "status": "VARCHAR(64)",
            "raw_payload": "JSONB",
            "raw_payload_json": "JSONB",
            "source_system": "VARCHAR(128)",
            "ingestion_run_id": "VARCHAR(128)",
            "schema_version": "VARCHAR(64)",
            "evaluation_label_is_sar": "BOOLEAN",
            "ingestion_metadata_json": "JSONB",
            "explainability_data": "JSONB",
        }
        with self.session() as session:
            if self._database_url.startswith("sqlite"):
                rows = session.execute(text("PRAGMA table_info(alerts)")).all()
                existing = {str(row[1]) for row in rows}
                for column, col_type in required_columns.items():
                    if column in existing:
                        continue
                    sqlite_type = "TEXT"
                    default_sql = ""
                    if column in {"risk_score"}:
                        sqlite_type = "REAL"
                        default_sql = " DEFAULT 0.0"
                    elif column in {"status"}:
                        sqlite_type = "TEXT"
                        default_sql = " DEFAULT 'new'"
                    elif column in {"raw_payload", "raw_payload_json", "ingestion_metadata_json", "explainability_data"}:
                        sqlite_type = "TEXT"
                        default_sql = " DEFAULT '{}'"
                    elif column == "evaluation_label_is_sar":
                        sqlite_type = "INTEGER"
                        default_sql = ""
                    session.execute(text(f"ALTER TABLE alerts ADD COLUMN {column} {sqlite_type}{default_sql}"))

                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_risk_score ON alerts (risk_score)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_status ON alerts (status)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_created_at ON alerts (created_at)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_alert_id ON alerts (alert_id)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_ingestion_run_id ON alerts (ingestion_run_id)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_source_system ON alerts (source_system)"))
                try:
                    session.execute(
                        text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_tenant_alert_id "
                            "ON alerts (tenant_id, alert_id)"
                        )
                    )
                except Exception as exc:
                    logger.warning("Unable to enforce tenant alert uniqueness", extra={"error": str(exc)})
                return

            rows = session.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'alerts'
                    """
                )
            ).all()
            existing = {str(row[0]) for row in rows}
            for column, col_type in required_columns.items():
                if column in existing:
                    continue
                if column in {"raw_payload", "raw_payload_json", "ingestion_metadata_json", "explainability_data"}:
                    session.execute(text(f"ALTER TABLE alerts ADD COLUMN {column} {col_type} DEFAULT '{{}}'::jsonb"))
                elif column == "risk_score":
                    session.execute(text(f"ALTER TABLE alerts ADD COLUMN {column} {col_type} DEFAULT 0.0"))
                elif column == "status":
                    session.execute(text(f"ALTER TABLE alerts ADD COLUMN {column} {col_type} DEFAULT 'new'"))
                else:
                    session.execute(text(f"ALTER TABLE alerts ADD COLUMN {column} {col_type}"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_risk_score ON alerts (risk_score)"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_status ON alerts (status)"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_created_at ON alerts (created_at)"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_alert_id ON alerts (alert_id)"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_ingestion_run_id ON alerts (ingestion_run_id)"))
            session.execute(text("CREATE INDEX IF NOT EXISTS ix_alerts_source_system ON alerts (source_system)"))
            try:
                session.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_tenant_alert_id "
                        "ON alerts (tenant_id, alert_id)"
                    )
                )
            except Exception as exc:
                logger.warning("Unable to enforce tenant alert uniqueness", extra={"error": str(exc)})

    def _ensure_user_email_uniqueness(self) -> None:
        with self.session() as session:
            if self._database_url.startswith("sqlite"):
                session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_tenant_email ON users (tenant_id, email)"))
                return
            session.execute(text("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_email_key"))
            session.execute(text("DROP INDEX IF EXISTS ix_users_email"))
            session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_tenant_email ON users (tenant_id, email)"))

    def _ensure_rbac_seed(self) -> None:
        with self.session() as session:
            existing = session.execute(select(RBACRoleRecord).where(RBACRoleRecord.tenant_id == "_system")).scalars().all()
            existing_names = {row.role_name for row in existing}
            for role_name, permissions in DEFAULT_RBAC_PERMISSIONS.items():
                if role_name in existing_names:
                    continue
                session.add(
                    RBACRoleRecord(
                        tenant_id="_system",
                        role_name=role_name,
                        permissions_json=permissions,
                        is_system=True,
                        created_at=_utcnow(),
                    )
                )
            session.flush()

    def _ensure_role_permissions_seed(self) -> None:
        # Keep canonical role->permission mappings in relational form for DB-backed permission checks.
        with self.session() as session:
            for role_name, permissions in DEFAULT_RBAC_PERMISSIONS.items():
                for permission_name in permissions:
                    existing = session.execute(
                        select(RolePermissionRecord).where(
                            RolePermissionRecord.tenant_id == "_system",
                            RolePermissionRecord.role_name == role_name,
                            RolePermissionRecord.permission_name == permission_name,
                        )
                    ).scalar_one_or_none()
                    if existing:
                        continue
                    session.add(
                        RolePermissionRecord(
                            tenant_id="_system",
                            role_name=role_name,
                            permission_name=permission_name,
                            created_at=_utcnow(),
                        )
                    )
            session.flush()

    def _ensure_tier1_enterprise_tables(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS features (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                feature_name VARCHAR(128) NOT NULL,
                feature_type VARCHAR(64) NOT NULL,
                description TEXT NULL,
                owner VARCHAR(128) NULL,
                source_system VARCHAR(128) NULL,
                tags_json JSON NOT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_features_tenant_name ON features (tenant_id, feature_name)",
            """
            CREATE TABLE IF NOT EXISTS feature_versions (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                feature_name VARCHAR(128) NOT NULL,
                version VARCHAR(64) NOT NULL,
                transformation_sql TEXT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                metadata_json JSON NOT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_versions_tenant_name_version ON feature_versions (tenant_id, feature_name, version)",
            """
            CREATE TABLE IF NOT EXISTS feature_dependencies (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                feature_name VARCHAR(128) NOT NULL,
                depends_on VARCHAR(128) NOT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_feature_dependencies_tenant_feature_dep ON feature_dependencies (tenant_id, feature_name, depends_on)",
            """
            CREATE TABLE IF NOT EXISTS offline_feature_store (
                id VARCHAR(256) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                run_id VARCHAR(128) NOT NULL,
                alert_id VARCHAR(128) NOT NULL,
                feature_version VARCHAR(64) NOT NULL,
                features_json JSON NOT NULL,
                parquet_uri VARCHAR(1024) NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_offline_feature_store_tenant_run ON offline_feature_store (tenant_id, run_id)",
            """
            CREATE TABLE IF NOT EXISTS model_governance_lifecycle (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                model_version VARCHAR(128) NOT NULL,
                lifecycle_state VARCHAR(32) NOT NULL,
                actor_role VARCHAR(64) NULL,
                actor_id VARCHAR(128) NULL,
                notes TEXT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_model_governance_lifecycle_tenant_model ON model_governance_lifecycle (tenant_id, model_version)",
            """
            CREATE TABLE IF NOT EXISTS model_governance_approvals (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                model_version VARCHAR(128) NOT NULL,
                stage VARCHAR(64) NOT NULL,
                actor_id VARCHAR(128) NOT NULL,
                decision VARCHAR(32) NOT NULL,
                notes TEXT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_model_governance_approvals_tenant_model ON model_governance_approvals (tenant_id, model_version)",
            """
            CREATE TABLE IF NOT EXISTS model_governance_monitoring (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                model_version VARCHAR(128) NOT NULL,
                drift_metric FLOAT NOT NULL DEFAULT 0,
                score_shift_metric FLOAT NOT NULL DEFAULT 0,
                feedback_outcome_rate FLOAT NOT NULL DEFAULT 0,
                metadata_json JSON NOT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_model_governance_monitoring_tenant_model ON model_governance_monitoring (tenant_id, model_version)",
            """
            CREATE TABLE IF NOT EXISTS workflow_state_transitions (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                case_id VARCHAR(128) NOT NULL,
                from_state VARCHAR(32) NOT NULL,
                to_state VARCHAR(32) NOT NULL,
                actor_id VARCHAR(128) NULL,
                reason TEXT NULL,
                created_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_workflow_state_transitions_tenant_case ON workflow_state_transitions (tenant_id, case_id)",
        ]
        with self.session() as session:
            for statement in statements:
                session.execute(text(statement))

    def _ensure_investigation_intelligence_tables(self) -> None:
        """Auto-provision investigation intelligence tables for SQLite dev and migration-less deploys."""
        statements = [
            """
            CREATE TABLE IF NOT EXISTS alert_outcomes (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                alert_id VARCHAR(128) NOT NULL,
                analyst_decision VARCHAR(64) NOT NULL,
                decision_reason TEXT NULL,
                analyst_id VARCHAR(128) NULL,
                model_version VARCHAR(128) NULL,
                risk_score_at_decision REAL NULL,
                sar_filed_flag BOOLEAN NOT NULL DEFAULT FALSE,
                qa_override BOOLEAN NOT NULL DEFAULT FALSE,
                investigation_start_time TIMESTAMP NULL,
                investigation_end_time TIMESTAMP NULL,
                resolution_hours REAL NULL,
                touch_count INTEGER NULL,
                notes_count INTEGER NULL,
                final_label_status VARCHAR(32) NOT NULL DEFAULT 'final',
                final_label_timestamp TIMESTAMP NULL,
                timestamp TIMESTAMP NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_outcomes_tenant_alert ON alert_outcomes (tenant_id, alert_id)",
            "CREATE INDEX IF NOT EXISTS ix_alert_outcomes_tenant_decision ON alert_outcomes (tenant_id, analyst_decision)",
            """
            CREATE TABLE IF NOT EXISTS decision_audit (
                id VARCHAR(128) PRIMARY KEY,
                tenant_id VARCHAR(128) NOT NULL,
                alert_id VARCHAR(128) NOT NULL,
                run_id VARCHAR(128) NULL,
                model_version VARCHAR(128) NULL,
                priority_score REAL NULL,
                escalation_prob REAL NULL,
                graph_risk_score REAL NULL,
                similar_suspicious_strength REAL NULL,
                p50_hours REAL NULL,
                p90_hours REAL NULL,
                governance_status VARCHAR(64) NULL,
                queue_action VARCHAR(64) NULL,
                priority_bucket VARCHAR(32) NULL,
                compliance_flags_json JSON NULL,
                signals_json JSON NULL,
                decided_at TIMESTAMP NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_decision_audit_tenant_alert ON decision_audit (tenant_id, alert_id)",
            "CREATE INDEX IF NOT EXISTS ix_decision_audit_tenant_run ON decision_audit (tenant_id, run_id)",
            "CREATE INDEX IF NOT EXISTS ix_decision_audit_queue_action ON decision_audit (queue_action)",
            """
            CREATE TABLE IF NOT EXISTS global_pattern_signals (
                id VARCHAR(128) PRIMARY KEY,
                signal_type VARCHAR(64) NOT NULL,
                signal_hash VARCHAR(256) NOT NULL,
                tenant_count INTEGER NOT NULL DEFAULT 1,
                alert_count INTEGER NOT NULL DEFAULT 1,
                first_seen_at TIMESTAMP NULL,
                last_seen_at TIMESTAMP NULL,
                metadata_json JSON NOT NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_global_pattern_signals_type_hash ON global_pattern_signals (signal_type, signal_hash)",
            "CREATE INDEX IF NOT EXISTS ix_global_pattern_signals_type ON global_pattern_signals (signal_type)",
        ]
        with self.session() as session:
            for statement in statements:
                session.execute(text(statement))

    def set_tenant_context(self, tenant_id: str) -> None:
        tenant_id = self._require_tenant(tenant_id)
        if not self._is_postgres:
            # SQLite and other local dev engines do not support PostgreSQL session settings.
            return
        with self.session(tenant_id=tenant_id) as session:
            # Read back setting to force application on the active connection and fail fast if misconfigured.
            session.execute(text("SELECT current_setting('app.tenant_id', true)"))

    @contextmanager
    def session(self, tenant_id: str | None = None) -> Iterator[Session]:
        session = self._session_factory()
        try:
            if tenant_id and self._is_postgres:
                session.execute(
                    text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                    {"tenant_id": str(tenant_id)},
                )
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def ping(self) -> bool:
        with self.session() as session:
            session.execute(text("SELECT 1"))
            return True

    @staticmethod
    def _require_tenant(tenant_id: str) -> str:
        normalized = str(tenant_id or "").strip()
        if not normalized:
            raise ValueError("tenant_id is required")
        return normalized

    def upsert_runtime_context(self, tenant_id: str, user_scope: str, **payload: Any) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            record = self._get_latest_runtime_context_record(session, tenant_id, user_scope)
            if record is None:
                record = RuntimeContext(tenant_id=tenant_id, user_scope=user_scope)
                session.add(record)
            for key, value in payload.items():
                setattr(record, key, value)
            session.flush()
            return self._to_dict(record)

    def get_runtime_context(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            record = self._get_latest_runtime_context_record(session, tenant_id, user_scope)
            if record is None:
                return self.upsert_runtime_context(tenant_id, user_scope)
            return self._to_dict(record)

    def clear_runtime_context(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        return self.upsert_runtime_context(
            tenant_id,
            user_scope,
            active_run_id=None,
            run_source=None,
            dataset_hash=None,
            dataset_artifact_uri=None,
            raw_artifact_uri=None,
            row_count=None,
            active_job_id=None,
        )

    def create_pipeline_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=payload["tenant_id"]) as session:
            record = PipelineRunRecord(**payload)
            session.add(record)
            session.flush()
            return self._to_dict(record)

    def update_pipeline_job(self, job_id: str, tenant_id: str, **payload: Any) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            record = session.execute(
                select(PipelineRunRecord).where(PipelineRunRecord.id == job_id, PipelineRunRecord.tenant_id == tenant_id)
            ).scalar_one_or_none()
            if record is None:
                return None
            for key, value in payload.items():
                setattr(record, key, value)
            session.flush()
            return self._to_dict(record)

    def get_pipeline_job(self, tenant_id: str, job_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            record = session.execute(
                select(PipelineRunRecord).where(
                    PipelineRunRecord.tenant_id == tenant_id,
                    PipelineRunRecord.id == job_id,
                )
            ).scalar_one_or_none()
            return self._to_dict(record) if record else None

    def list_pipeline_runs(self, tenant_id: str, limit: int = 50) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(PipelineRunRecord)
                .where(PipelineRunRecord.tenant_id == tenant_id)
                .order_by(PipelineRunRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def get_completed_pipeline_chunk_indexes(self, tenant_id: str, job_id: str) -> set[int]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(PipelineJobCheckpointRecord.chunk_index).where(
                    PipelineJobCheckpointRecord.tenant_id == tenant_id,
                    PipelineJobCheckpointRecord.job_id == job_id,
                    PipelineJobCheckpointRecord.status == "completed",
                )
            ).all()
            return {int(row[0]) for row in rows if row and row[0] is not None}

    def upsert_pipeline_checkpoint(
        self,
        tenant_id: str,
        job_id: str,
        chunk_index: int,
        processed_rows: int,
        run_id: str | None,
        status: str = "completed",
    ) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            record = session.execute(
                select(PipelineJobCheckpointRecord).where(
                    PipelineJobCheckpointRecord.tenant_id == tenant_id,
                    PipelineJobCheckpointRecord.job_id == job_id,
                    PipelineJobCheckpointRecord.chunk_index == int(chunk_index),
                )
            ).scalar_one_or_none()
            if record is None:
                record = PipelineJobCheckpointRecord(
                    tenant_id=tenant_id,
                    job_id=job_id,
                    chunk_index=int(chunk_index),
                    processed_rows=int(processed_rows),
                    run_id=run_id,
                    status=status,
                    created_at=_utcnow(),
                    updated_at=_utcnow(),
                )
                session.add(record)
            else:
                record.processed_rows = int(processed_rows)
                record.run_id = run_id
                record.status = str(status or "completed")
                record.updated_at = _utcnow()
            session.flush()
            return self._to_dict(record)

    def save_alert_payloads(self, tenant_id: str, run_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self.session(tenant_id=tenant_id) as session:
            # Deduplicate by internal alert key within the same batch and enforce minimal payload requirements.
            # Bank CSV uploads can contain repeated alert_id values and must not trigger collisions.
            deduped_by_id: dict[str, dict[str, Any]] = {}
            for incoming in records:
                payload = dict(incoming or {})
                alert_id = str(payload.get("alert_id") or payload.get("id") or "").strip()
                if not alert_id:
                    logger.warning("Skipping alert payload without alert_id")
                    continue
                payload["alert_id"] = alert_id
                payload.setdefault("id", alert_id)
                timestamp = payload.get("timestamp") or payload.get("created_at")
                if not timestamp:
                    # Explicit safe fallback for ingestion integrity.
                    timestamp = _utcnow().isoformat()
                    payload["ingestion_timestamp_fallback"] = True
                payload["timestamp"] = timestamp
                payload.setdefault("created_at", timestamp)
                internal_id = f"{tenant_id}:{alert_id}"
                deduped_by_id[internal_id] = payload

            valid_records = list(deduped_by_id.values())
            if not valid_records:
                return 0

            rows_to_upsert: list[dict[str, Any]] = []
            for payload in valid_records:
                alert_id = str(payload.get("alert_id") or payload.get("id"))
                internal_id = f"{tenant_id}:{alert_id}"
                explainability_data = {
                    "risk_explain_json": payload.get("risk_explain_json"),
                    "top_feature_contributions_json": payload.get("top_feature_contributions_json"),
                    "top_features_json": payload.get("top_features_json"),
                    "ml_service_explain_json": payload.get("ml_service_explain_json"),
                }
                raw_payload = dict(payload)
                structured_payload = self._coerce_json_object(payload.get("raw_payload_json"))
                if not structured_payload:
                    structured_payload = dict(raw_payload)
                structured_payload = self._sanitize_contract_object(structured_payload)
                source_system = str(payload.get("source_system") or "").strip() or None
                ingestion_run_id = str(payload.get("ingestion_run_id") or run_id or "").strip() or None
                schema_version = str(payload.get("schema_version") or "").strip() or None
                evaluation_label_is_sar = self._coerce_optional_bool(
                    payload.get("evaluation_label_is_sar", payload.get("is_sar"))
                )
                ingestion_metadata_json = self._build_ingestion_metadata(
                    payload=payload,
                    run_id=run_id,
                    source_system=source_system,
                    schema_version=schema_version,
                )
                risk_score = float(payload.get("risk_score", 0.0) or 0.0)
                status = str(payload.get("status") or payload.get("governance_status") or "new")
                risk_band = str(payload.get("risk_band") or "") or None
                priority = str(payload.get("priority") or risk_band or "low")
                rows_to_upsert.append(
                    {
                        "id": internal_id,
                        "tenant_id": tenant_id,
                        "run_id": run_id,
                        "alert_id": alert_id,
                        "risk_score": risk_score,
                        "risk_band": risk_band,
                        "priority": priority,
                        "status": status,
                        "payload_json": raw_payload,
                        "raw_payload": raw_payload,
                        "raw_payload_json": structured_payload,
                        "source_system": source_system,
                        "ingestion_run_id": ingestion_run_id,
                        "schema_version": schema_version,
                        "evaluation_label_is_sar": evaluation_label_is_sar,
                        "ingestion_metadata_json": ingestion_metadata_json,
                        "explainability_data": explainability_data,
                        "created_at": _utcnow(),
                    }
                )

            if self._is_postgres and postgres_insert is not None:
                stmt = postgres_insert(AlertRecord).values(rows_to_upsert)
                update_cols = {
                    "tenant_id": stmt.excluded.tenant_id,
                    "run_id": stmt.excluded.run_id,
                    "alert_id": stmt.excluded.alert_id,
                    "risk_score": stmt.excluded.risk_score,
                    "risk_band": stmt.excluded.risk_band,
                    "priority": stmt.excluded.priority,
                    "status": stmt.excluded.status,
                    "payload_json": stmt.excluded.payload_json,
                    "raw_payload": stmt.excluded.raw_payload,
                    "raw_payload_json": stmt.excluded.raw_payload_json,
                    "source_system": stmt.excluded.source_system,
                    "ingestion_run_id": stmt.excluded.ingestion_run_id,
                    "schema_version": stmt.excluded.schema_version,
                    "evaluation_label_is_sar": stmt.excluded.evaluation_label_is_sar,
                    "ingestion_metadata_json": stmt.excluded.ingestion_metadata_json,
                    "explainability_data": stmt.excluded.explainability_data,
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[AlertRecord.id],
                    set_=update_cols,
                )
                session.execute(stmt)
                session.flush()
                return len(rows_to_upsert)

            if self._database_url.startswith("sqlite"):
                stmt = sqlite_insert(AlertRecord).values(rows_to_upsert)
                update_cols = {
                    "tenant_id": stmt.excluded.tenant_id,
                    "run_id": stmt.excluded.run_id,
                    "alert_id": stmt.excluded.alert_id,
                    "risk_score": stmt.excluded.risk_score,
                    "risk_band": stmt.excluded.risk_band,
                    "priority": stmt.excluded.priority,
                    "status": stmt.excluded.status,
                    "payload_json": stmt.excluded.payload_json,
                    "raw_payload": stmt.excluded.raw_payload,
                    "raw_payload_json": stmt.excluded.raw_payload_json,
                    "source_system": stmt.excluded.source_system,
                    "ingestion_run_id": stmt.excluded.ingestion_run_id,
                    "schema_version": stmt.excluded.schema_version,
                    "evaluation_label_is_sar": stmt.excluded.evaluation_label_is_sar,
                    "ingestion_metadata_json": stmt.excluded.ingestion_metadata_json,
                    "explainability_data": stmt.excluded.explainability_data,
                }
                stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_cols)
                session.execute(stmt)
                session.flush()
                return len(rows_to_upsert)

            # Defensive fallback for non-postgres/non-sqlite engines.
            for payload in rows_to_upsert:
                existing = session.execute(
                    select(AlertRecord).where(AlertRecord.id == payload["id"])
                ).scalar_one_or_none()
                if existing is None:
                    session.add(AlertRecord(**payload))
                    continue
                existing.tenant_id = payload["tenant_id"]
                existing.run_id = payload["run_id"]
                existing.alert_id = payload["alert_id"]
                existing.risk_score = payload["risk_score"]
                existing.risk_band = payload["risk_band"]
                existing.priority = payload["priority"]
                existing.status = payload["status"]
                existing.payload_json = payload["payload_json"]
                existing.raw_payload = payload["raw_payload"]
                existing.raw_payload_json = payload["raw_payload_json"]
                existing.source_system = payload["source_system"]
                existing.ingestion_run_id = payload["ingestion_run_id"]
                existing.schema_version = payload["schema_version"]
                existing.evaluation_label_is_sar = payload["evaluation_label_is_sar"]
                existing.ingestion_metadata_json = payload["ingestion_metadata_json"]
                existing.explainability_data = payload["explainability_data"]
            session.flush()
            return len(rows_to_upsert)

    def save_alert_payload(self, tenant_id: str, run_id: str, alert_dict: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        payload = dict(alert_dict or {})
        alert_id = str(payload.get("alert_id") or payload.get("id") or "").strip()
        if not alert_id:
            raise ValueError("alert_id is required")
        self.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=[payload])
        stored = self.get_alert_payload(tenant_id=tenant_id, alert_id=alert_id, run_id=run_id)
        return stored or {}

    def get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None = None) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        clean_alert_id = str(alert_id or "").strip()
        if not clean_alert_id:
            return None
        with self.session(tenant_id=tenant_id) as session:
            query = select(AlertRecord).where(
                AlertRecord.tenant_id == tenant_id,
                AlertRecord.alert_id == clean_alert_id,
            )
            if run_id:
                query = query.where(AlertRecord.run_id == run_id)
            row = session.execute(query.order_by(AlertRecord.created_at.desc())).scalars().first()
            return self._alert_to_payload(row) if row else None

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(AlertRecord)
                .where(AlertRecord.tenant_id == tenant_id, AlertRecord.run_id == run_id)
                .order_by(AlertRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [self._alert_to_payload(row) for row in rows]

    def list_latest_alert_payloads_for_alert_ids(
        self,
        tenant_id: str,
        alert_ids: list[str],
        limit: int = 50000,
    ) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        clean_ids = [str(item or "").strip() for item in alert_ids if str(item or "").strip()]
        if not clean_ids:
            return []
        wanted = []
        seen_ids: set[str] = set()
        for alert_id in clean_ids:
            if alert_id in seen_ids:
                continue
            seen_ids.add(alert_id)
            wanted.append(alert_id)
        bounded_limit = max(1, min(int(limit or len(wanted)), len(wanted)))
        out: list[dict[str, Any]] = []
        collected: set[str] = set()
        chunk_size = 500
        with self.session(tenant_id=tenant_id) as session:
            for start in range(0, len(wanted), chunk_size):
                chunk = wanted[start : start + chunk_size]
                rows = session.execute(
                    select(AlertRecord)
                    .where(
                        AlertRecord.tenant_id == tenant_id,
                        AlertRecord.alert_id.in_(chunk),
                    )
                    .order_by(AlertRecord.created_at.desc())
                ).scalars()
                for row in rows:
                    alert_id = str(row.alert_id or "").strip()
                    if not alert_id or alert_id in collected:
                        continue
                    collected.add(alert_id)
                    out.append(self._alert_to_payload(row))
                    if len(out) >= bounded_limit:
                        return out
        return out

    def list_recent_alert_ids(self, tenant_id: str, limit: int = 5000) -> list[str]:
        tenant_id = self._require_tenant(tenant_id)
        bounded_limit = max(1, min(int(limit or 5000), 50000))
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(AlertRecord.alert_id)
                .where(AlertRecord.tenant_id == tenant_id)
                .order_by(AlertRecord.created_at.desc())
                .limit(bounded_limit)
            ).all()
            out: list[str] = []
            for row in rows:
                value = str(row[0] or "").strip()
                if value:
                    out.append(value)
            return out

    def list_alert_payloads_paginated(
        self,
        tenant_id: str,
        run_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(AlertRecord)
                .where(AlertRecord.tenant_id == tenant_id, AlertRecord.run_id == run_id)
                .order_by(AlertRecord.created_at.desc())
                .limit(limit)
                .offset(offset)
            ).scalars()
            total = session.execute(
                select(func.count()).select_from(AlertRecord).where(AlertRecord.tenant_id == tenant_id, AlertRecord.run_id == run_id)
            ).scalar_one()
            return {
                "items": [self._alert_to_payload(row) for row in rows],
                "limit": int(limit),
                "offset": int(offset),
                "total": int(total or 0),
            }

    def create_user(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        role_name = str(payload.get("role") or "analyst").lower().strip()
        actor_id = str(payload.pop("created_by_actor_id", "") or payload.get("id") or "").strip() or None
        provision_mode = str(payload.pop("provision_mode", "") or "ADMIN_INVITE").upper().strip()
        with self.session(tenant_id=payload["tenant_id"]) as session:
            record = UserRecord(**payload)
            session.add(record)
            session.flush()
            self._ensure_tenant_roles_seed(session, payload["tenant_id"])
            self._upsert_user_role_records(
                session=session,
                tenant_id=payload["tenant_id"],
                user_id=record.id,
                roles=[role_name],
                created_by=actor_id or payload.get("id"),
                replace=True,
            )
            session.add(
                AuthAuditLogRecord(
                    tenant_id=payload["tenant_id"],
                    user_id=record.id,
                    actor_id=actor_id,
                    action="user_created",
                    old_role=None,
                    new_role=role_name,
                    details_json={"email": str(record.email or ""), "provision_mode": provision_mode},
                    timestamp=_utcnow(),
                )
            )
            return self._to_dict(record)

    def get_user_by_email(self, tenant_id: str, email: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.email == email.lower())
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def get_user_by_id(self, tenant_id: str, user_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def list_users(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id).order_by(UserRecord.created_at.desc())
            ).scalars()
            users = [self._to_dict(row) for row in rows]
            role_rows = session.execute(
                select(UserRoleRecord.user_id, UserRoleRecord.role_name).where(UserRoleRecord.tenant_id == tenant_id)
            ).all()
            role_map: dict[str, list[str]] = {}
            for user_id, role_name in role_rows:
                role_map.setdefault(str(user_id), []).append(str(role_name).lower())
            for user in users:
                roles = sorted(set(role_map.get(str(user.get("id") or ""), [])))
                if roles:
                    user["roles"] = roles
                    user["role"] = roles[0]
            return users

    def update_user_role(self, tenant_id: str, user_id: str, role: str, actor_id: str | None = None) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        role = str(role or "").lower().strip()
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            old_role = str(row.role or "").lower().strip() or None
            row.role = role
            self._ensure_tenant_roles_seed(session, tenant_id)
            self._upsert_user_role_records(
                session=session,
                tenant_id=tenant_id,
                user_id=user_id,
                roles=[role],
                created_by=actor_id or user_id,
                replace=True,
            )
            session.add(
                AuthAuditLogRecord(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    actor_id=actor_id,
                    action="role_changed",
                    old_role=old_role,
                    new_role=role,
                    details_json={},
                    timestamp=_utcnow(),
                )
            )
            session.flush()
            return self._to_dict(row)

    def set_user_active(self, tenant_id: str, user_id: str, is_active: bool, actor_id: str | None = None) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            row.is_active = bool(is_active)
            revoked_sessions = 0
            if not bool(is_active):
                active_sessions = session.execute(
                    select(UserSessionRecord).where(
                        UserSessionRecord.tenant_id == tenant_id,
                        UserSessionRecord.user_id == user_id,
                        UserSessionRecord.revoked == False,
                    )
                ).scalars()
                for session_row in active_sessions:
                    session_row.revoked = True
                    revoked_sessions += 1
            action = "user_enabled" if bool(is_active) else "user_disabled"
            session.add(
                AuthAuditLogRecord(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    actor_id=actor_id,
                    action=action,
                    old_role=str(row.role or "").lower().strip() or None,
                    new_role=str(row.role or "").lower().strip() or None,
                    details_json={"is_active": bool(is_active), "revoked_sessions": revoked_sessions},
                    timestamp=_utcnow(),
                )
            )
            session.flush()
            return self._to_dict(row)

    def create_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=payload["tenant_id"]) as session:
            record = UserSessionRecord(**payload)
            session.add(record)
            session.flush()
            return self._to_dict(record)

    def get_session(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserSessionRecord).where(
                    UserSessionRecord.tenant_id == tenant_id,
                    UserSessionRecord.session_id == session_id,
                )
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def revoke_session(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserSessionRecord).where(
                    UserSessionRecord.tenant_id == tenant_id,
                    UserSessionRecord.session_id == session_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.revoked = True
            session.flush()
            return self._to_dict(row)

    def update_session_refresh_token(
        self,
        tenant_id: str,
        session_id: str,
        refresh_token_hash: str,
        expires_at: datetime,
    ) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(UserSessionRecord).where(
                    UserSessionRecord.tenant_id == tenant_id,
                    UserSessionRecord.session_id == session_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.refresh_token_hash = refresh_token_hash
            row.expires_at = expires_at
            session.flush()
            return self._to_dict(row)

    def revoke_all_user_sessions(self, tenant_id: str, user_id: str) -> int:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(UserSessionRecord).where(
                    UserSessionRecord.tenant_id == tenant_id,
                    UserSessionRecord.user_id == user_id,
                    UserSessionRecord.revoked == False,
                )
            ).scalars()
            count = 0
            for row in rows:
                row.revoked = True
                count += 1
            session.flush()
            return count

    def upsert_assignment(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=payload["tenant_id"]) as session:
            row = session.execute(
                select(AlertAssignmentRecord).where(
                    AlertAssignmentRecord.id == payload["id"],
                    AlertAssignmentRecord.tenant_id == payload["tenant_id"],
                )
            ).scalar_one_or_none()
            if row is None:
                row = AlertAssignmentRecord(**payload)
                session.add(row)
            else:
                for key, value in payload.items():
                    setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def get_latest_assignment(self, tenant_id: str, alert_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(AlertAssignmentRecord)
                .where(AlertAssignmentRecord.tenant_id == tenant_id, AlertAssignmentRecord.alert_id == alert_id)
                .order_by(AlertAssignmentRecord.updated_at.desc())
            ).scalars().first()
            return self._to_dict(row) if row else None

    def create_alert_note(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=tenant_id) as session:
            row = AlertNoteRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_alert_notes(self, tenant_id: str, alert_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(AlertNoteRecord)
                .where(AlertNoteRecord.tenant_id == tenant_id, AlertNoteRecord.alert_id == alert_id)
                .order_by(AlertNoteRecord.created_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def save_case(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=payload["tenant_id"]) as session:
            row = session.execute(
                select(CaseRecord).where(
                    CaseRecord.case_id == payload["case_id"],
                    CaseRecord.tenant_id == payload["tenant_id"],
                )
            ).scalar_one_or_none()
            if row is None:
                row = CaseRecord(**payload)
                session.add(row)
            else:
                for key, value in payload.items():
                    setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def get_case(self, tenant_id: str, case_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id, CaseRecord.case_id == case_id)
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def list_cases(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id).order_by(CaseRecord.updated_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def list_cases_paginated(self, tenant_id: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(CaseRecord)
                .where(CaseRecord.tenant_id == tenant_id)
                .order_by(CaseRecord.updated_at.desc())
                .limit(limit)
                .offset(offset)
            ).scalars()
            total = session.execute(
                select(func.count()).select_from(CaseRecord).where(CaseRecord.tenant_id == tenant_id)
            ).scalar_one()
            return {"items": [self._to_dict(row) for row in rows], "limit": int(limit), "offset": int(offset), "total": int(total or 0)}

    def delete_case(self, tenant_id: str, case_id: str) -> bool:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id, CaseRecord.case_id == case_id)
            ).scalar_one_or_none()
            if row is None:
                return False
            session.delete(row)
            return True

    def append_investigation_log(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=tenant_id) as session:
            row = InvestigationLogRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def append_auth_audit_log(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=tenant_id) as session:
            row = AuthAuditLogRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_auth_audit_logs(self, tenant_id: str, limit: int = 300) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(AuthAuditLogRecord)
                .where(AuthAuditLogRecord.tenant_id == tenant_id)
                .order_by(AuthAuditLogRecord.timestamp.desc())
                .limit(limit)
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def upsert_identity_provider(
        self,
        tenant_id: str,
        provider_type: str,
        name: str,
        config_json: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(IdentityProviderRecord).where(
                    IdentityProviderRecord.tenant_id == tenant_id,
                    IdentityProviderRecord.name == name,
                )
            ).scalar_one_or_none()
            if row is None:
                row = IdentityProviderRecord(
                    tenant_id=tenant_id,
                    provider_type=provider_type.lower().strip(),
                    name=name,
                    config_json=config_json or {},
                    enabled=bool(enabled),
                    created_at=_utcnow(),
                    updated_at=_utcnow(),
                )
                session.add(row)
            else:
                row.provider_type = provider_type.lower().strip()
                row.config_json = config_json or {}
                row.enabled = bool(enabled)
                row.updated_at = _utcnow()
            session.flush()
            return self._to_dict(row)

    def list_identity_providers(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(IdentityProviderRecord).where(IdentityProviderRecord.tenant_id == tenant_id)
            ).scalars().all()
            return [self._to_dict(row) for row in rows]

    def link_external_identity(self, tenant_id: str, user_id: str, provider_id: str, external_subject: str) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(ExternalIdentityRecord).where(
                    ExternalIdentityRecord.tenant_id == tenant_id,
                    ExternalIdentityRecord.provider_id == provider_id,
                    ExternalIdentityRecord.external_subject == external_subject,
                )
            ).scalar_one_or_none()
            if row is None:
                row = ExternalIdentityRecord(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    provider_id=provider_id,
                    external_subject=external_subject,
                    created_at=_utcnow(),
                )
                session.add(row)
            else:
                row.user_id = user_id
            session.flush()
            return self._to_dict(row)

    def list_investigation_logs(
        self,
        tenant_id: str,
        case_id: str | None = None,
        alert_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            stmt = select(InvestigationLogRecord).where(InvestigationLogRecord.tenant_id == tenant_id)
            if case_id:
                stmt = stmt.where(InvestigationLogRecord.case_id == case_id)
            if alert_id:
                stmt = stmt.where(InvestigationLogRecord.alert_id == alert_id)
            rows = session.execute(stmt.order_by(InvestigationLogRecord.timestamp.desc()).limit(limit)).scalars()
            return [self._to_dict(row) for row in rows]

    def register_model_version(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["tenant_id"] = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=payload["tenant_id"]) as session:
            row = ModelVersionRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_model_versions(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(ModelVersionRecord)
                .where(ModelVersionRecord.tenant_id == tenant_id)
                .order_by(ModelVersionRecord.created_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def get_model_version(self, tenant_id: str, model_version: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(ModelVersionRecord).where(
                    ModelVersionRecord.tenant_id == tenant_id,
                    ModelVersionRecord.model_version == model_version,
                )
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def update_model_version(self, tenant_id: str, model_version: str, **payload: Any) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(ModelVersionRecord).where(
                    ModelVersionRecord.tenant_id == tenant_id,
                    ModelVersionRecord.model_version == model_version,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            for key, value in payload.items():
                setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def store_feature_rows(
        self,
        tenant_id: str,
        run_id: str,
        feature_schema_hash: str,
        feature_rows: list[dict[str, Any]],
    ) -> int:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            alert_ids = [
                str(row.get("alert_id") or row.get("id") or "").strip()
                for row in feature_rows
                if str(row.get("alert_id") or row.get("id") or "").strip()
            ]
            if alert_ids:
                existing_rows = session.execute(
                    select(FeatureStoreRecord).where(
                        FeatureStoreRecord.tenant_id == tenant_id,
                        FeatureStoreRecord.run_id == run_id,
                        FeatureStoreRecord.alert_id.in_(alert_ids),
                    )
                ).scalars().all()
                for existing in existing_rows:
                    # Idempotent resume: replace previously persisted rows for the same run + alert id.
                    session.delete(existing)
            for row in feature_rows:
                alert_id = str(row.get("alert_id") or row.get("id") or uuid.uuid4().hex)
                session.add(
                    FeatureStoreRecord(
                        tenant_id=tenant_id,
                        run_id=run_id,
                        alert_id=alert_id,
                        feature_schema_hash=feature_schema_hash,
                        features_json=row,
                    )
                )
            session.flush()
            return len(feature_rows)

    def list_feature_rows(self, tenant_id: str, run_id: str, limit: int = 200000) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(FeatureStoreRecord)
                .where(FeatureStoreRecord.tenant_id == tenant_id, FeatureStoreRecord.run_id == run_id)
                .order_by(FeatureStoreRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [dict(row.features_json or {}) for row in rows]

    def save_model_monitoring(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(payload.get("tenant_id", ""))
        safe_payload = dict(payload)
        safe_payload["psi_score"] = self._json_safe(safe_payload.get("psi_score"))
        safe_payload["drift_score"] = self._json_safe(safe_payload.get("drift_score"))
        safe_payload["metrics_json"] = self._json_safe(dict(safe_payload.get("metrics_json") or {}))
        if safe_payload.get("psi_score") is None:
            safe_payload["psi_score"] = 0.0
        if safe_payload.get("drift_score") is None:
            safe_payload["drift_score"] = 0.0
        with self.session(tenant_id=tenant_id) as session:
            row = ModelMonitoringRecord(**safe_payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def save_decision_audit_records(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self.session(tenant_id=tenant_id) as session:
            for payload in records:
                safe = dict(payload or {})
                session.add(
                    DecisionAuditRecord(
                        id=str(safe.get("id") or uuid.uuid4().hex),
                        tenant_id=tenant_id,
                        alert_id=str(safe.get("alert_id") or ""),
                        run_id=str(safe.get("run_id") or "") or None,
                        model_version=str(safe.get("model_version") or "") or None,
                        priority_score=self._json_safe(safe.get("priority_score")),
                        escalation_prob=self._json_safe(safe.get("escalation_prob")),
                        graph_risk_score=self._json_safe(safe.get("graph_risk_score")),
                        similar_suspicious_strength=self._json_safe(safe.get("similar_suspicious_strength")),
                        p50_hours=self._json_safe(safe.get("p50_hours")),
                        p90_hours=self._json_safe(safe.get("p90_hours")),
                        governance_status=str(safe.get("governance_status") or "") or None,
                        queue_action=str(safe.get("queue_action") or "") or None,
                        priority_bucket=str(safe.get("priority_bucket") or "") or None,
                        compliance_flags_json=self._json_safe(dict(safe.get("compliance_flags_json") or {})),
                        signals_json=self._json_safe(dict(safe.get("signals_json") or {})),
                        decided_at=safe.get("decided_at") or _utcnow(),
                    )
                )
            session.flush()
            return len(records)

    def list_model_monitoring(self, tenant_id: str, limit: int = 200) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(ModelMonitoringRecord)
                .where(ModelMonitoringRecord.tenant_id == tenant_id)
                .order_by(ModelMonitoringRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def list_case_timeline(self, tenant_id: str, case_id: str, limit: int = 500) -> list[dict[str, Any]]:
        logs = self.list_investigation_logs(tenant_id=tenant_id, case_id=case_id, limit=limit)
        timeline: list[dict[str, Any]] = []
        for item in reversed(logs):
            timeline.append(
                {
                    "event_id": item.get("id"),
                    "case_id": case_id,
                    "ts": item.get("timestamp"),
                    "actor": item.get("performed_by"),
                    "action": item.get("action"),
                    "payload": item.get("details_json", {}),
                }
            )
        return timeline

    def save_ai_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(payload.get("tenant_id", ""))
        with self.session(tenant_id=tenant_id) as session:
            existing = session.execute(
                select(AISummaryRecord).where(
                    AISummaryRecord.tenant_id == tenant_id,
                    AISummaryRecord.entity_type == payload["entity_type"],
                    AISummaryRecord.entity_id == payload["entity_id"],
                )
            ).scalar_one_or_none()
            if existing is None:
                row = AISummaryRecord(
                    tenant_id=tenant_id,
                    entity_type=payload["entity_type"],
                    entity_id=payload["entity_id"],
                    summary=payload["summary"],
                    run_id=payload.get("run_id"),
                    actor=payload.get("actor"),
                    ts=_utcnow(),
                )
                session.add(row)
            else:
                row = existing
                row.summary = payload["summary"]
                row.run_id = payload.get("run_id")
                row.actor = payload.get("actor")
                row.ts = _utcnow()
            session.flush()
            return self._to_dict(row)

    def get_ai_summary(self, tenant_id: str, entity_type: str, entity_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(AISummaryRecord).where(
                    AISummaryRecord.tenant_id == tenant_id,
                    AISummaryRecord.entity_type == entity_type,
                    AISummaryRecord.entity_id == entity_id,
                )
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def delete_ai_summary(self, tenant_id: str, entity_type: str, entity_id: str) -> bool:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(AISummaryRecord).where(
                    AISummaryRecord.tenant_id == tenant_id,
                    AISummaryRecord.entity_type == entity_type,
                    AISummaryRecord.entity_id == entity_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            session.delete(row)
            return True

    def count_users(self, tenant_id: str) -> int:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            count = session.execute(select(func.count()).select_from(UserRecord).where(UserRecord.tenant_id == tenant_id)).scalar_one()
            return int(count or 0)

    def list_user_roles(self, tenant_id: str, user_id: str) -> list[str]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            self._ensure_tenant_roles_seed(session, tenant_id)
            rows = session.execute(
                select(UserRoleRecord.role_name).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).all()
            roles = sorted({str(row[0]).lower() for row in rows if row and row[0]})
            if roles:
                return roles
            user = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            if user and user.role:
                return [str(user.role).lower()]
            return []

    def get_user_permissions(self, tenant_id: str, user_id: str, fallback_role: str | None = None) -> list[str]:
        tenant_id = self._require_tenant(tenant_id)
        with self.session(tenant_id=tenant_id) as session:
            self._ensure_tenant_roles_seed(session, tenant_id)
            role_rows = session.execute(
                select(UserRoleRecord.role_name).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).all()
            role_names = sorted({str(row[0]).lower() for row in role_rows if row and row[0]})
            if not role_names:
                user = session.execute(
                    select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
                ).scalar_one_or_none()
                if user and user.role:
                    role_names = [str(user.role).lower()]
            if not role_names and fallback_role:
                role_names = [str(fallback_role).lower()]
            if not role_names:
                return []
            permissions: set[str] = set()
            # Primary source of truth: relational role_permissions table.
            permission_rows = session.execute(
                select(RolePermissionRecord.permission_name).where(
                    RolePermissionRecord.tenant_id.in_([tenant_id, "_system"]),
                    RolePermissionRecord.role_name.in_(role_names),
                )
            ).all()
            permissions.update(str(row[0]) for row in permission_rows if row and row[0])

            # Compatibility fallback for legacy deployments that still use permissions_json in rbac_roles.
            legacy_role_rows = session.execute(
                select(RBACRoleRecord).where(
                    RBACRoleRecord.tenant_id.in_([tenant_id, "_system"]),
                    RBACRoleRecord.role_name.in_(role_names),
                )
            ).scalars().all()
            for row in legacy_role_rows:
                permissions.update(str(item) for item in (row.permissions_json or []) if item)
            return sorted(permissions)

    def assign_user_roles(
        self,
        tenant_id: str,
        user_id: str,
        roles: list[str],
        created_by: str | None = None,
        replace: bool = True,
    ) -> list[str]:
        tenant_id = self._require_tenant(tenant_id)
        normalized = sorted({str(role).lower().strip() for role in roles if str(role).strip()})
        with self.session(tenant_id=tenant_id) as session:
            self._ensure_tenant_roles_seed(session, tenant_id)
            before_rows = session.execute(
                select(UserRoleRecord.role_name).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).all()
            before_roles = sorted({str(row[0]).lower() for row in before_rows if row and row[0]})
            self._upsert_user_role_records(
                session=session,
                tenant_id=tenant_id,
                user_id=user_id,
                roles=normalized,
                created_by=created_by,
                replace=replace,
            )
            rows = session.execute(
                select(UserRoleRecord.role_name).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).all()
            final_roles = sorted({str(row[0]).lower() for row in rows if row and row[0]})
            if before_roles != final_roles:
                session.add(
                    AuthAuditLogRecord(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        actor_id=created_by,
                        action="role_membership_changed",
                        old_role=",".join(before_roles) if before_roles else None,
                        new_role=",".join(final_roles) if final_roles else None,
                        details_json={"replace": bool(replace)},
                        timestamp=_utcnow(),
                    )
                )
            return final_roles

    def set_role_permissions(
        self,
        tenant_id: str,
        role_name: str,
        permissions: list[str],
        actor_id: str | None = None,
    ) -> list[str]:
        tenant_id = self._require_tenant(tenant_id)
        clean_role = str(role_name or "").lower().strip()
        clean_permissions = sorted({str(item).strip() for item in permissions if str(item).strip()})
        with self.session(tenant_id=tenant_id) as session:
            existing_rows = session.execute(
                select(RolePermissionRecord).where(
                    RolePermissionRecord.tenant_id == tenant_id,
                    RolePermissionRecord.role_name == clean_role,
                )
            ).scalars().all()
            old_permissions = sorted({str(row.permission_name) for row in existing_rows})
            for row in existing_rows:
                session.delete(row)
            for permission_name in clean_permissions:
                session.add(
                    RolePermissionRecord(
                        tenant_id=tenant_id,
                        role_name=clean_role,
                        permission_name=permission_name,
                        created_at=_utcnow(),
                    )
                )
            session.add(
                AuthAuditLogRecord(
                    tenant_id=tenant_id,
                    user_id=None,
                    actor_id=actor_id,
                    action="permissions_changed",
                    old_role=clean_role,
                    new_role=clean_role,
                    details_json={"old_permissions": old_permissions, "new_permissions": clean_permissions},
                    timestamp=_utcnow(),
                )
            )
            session.flush()
            return clean_permissions

    def _ensure_tenant_roles_seed(self, session: Session, tenant_id: str) -> None:
        existing = session.execute(select(RBACRoleRecord).where(RBACRoleRecord.tenant_id == tenant_id)).scalars().all()
        existing_names = {row.role_name for row in existing}
        for role_name, permissions in DEFAULT_RBAC_PERMISSIONS.items():
            if role_name in existing_names:
                continue
            session.add(
                RBACRoleRecord(
                    tenant_id=tenant_id,
                    role_name=role_name,
                    permissions_json=permissions,
                    is_system=True,
                    created_at=_utcnow(),
                )
            )
            for permission_name in permissions:
                role_permission = session.execute(
                    select(RolePermissionRecord).where(
                        RolePermissionRecord.tenant_id == tenant_id,
                        RolePermissionRecord.role_name == role_name,
                        RolePermissionRecord.permission_name == permission_name,
                    )
                ).scalar_one_or_none()
                if role_permission is None:
                    session.add(
                        RolePermissionRecord(
                            tenant_id=tenant_id,
                            role_name=role_name,
                            permission_name=permission_name,
                            created_at=_utcnow(),
                        )
                    )
        session.flush()

    def _upsert_user_role_records(
        self,
        session: Session,
        tenant_id: str,
        user_id: str,
        roles: list[str],
        created_by: str | None,
        replace: bool,
    ) -> None:
        allowed_roles = set(DEFAULT_RBAC_PERMISSIONS)
        clean_roles = [role for role in roles if role in allowed_roles]
        if not clean_roles:
            clean_roles = ["analyst"]

        if replace:
            existing = session.execute(
                select(UserRoleRecord).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).scalars().all()
            for row in existing:
                session.delete(row)

        existing_names = {
            str(row.role_name).lower()
            for row in session.execute(
                select(UserRoleRecord).where(
                    UserRoleRecord.tenant_id == tenant_id,
                    UserRoleRecord.user_id == user_id,
                )
            ).scalars().all()
        }
        now = _utcnow()
        for role in clean_roles:
            if role in existing_names:
                continue
            session.add(
                UserRoleRecord(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    role_name=role,
                    created_by=created_by,
                    created_at=now,
                )
            )
        session.flush()

    @staticmethod
    def _alert_to_payload(row: AlertRecord) -> dict[str, Any]:
        structured_payload = dict(EnterpriseRepository._coerce_json_object(getattr(row, "raw_payload_json", {})))
        legacy_payload = dict(EnterpriseRepository._coerce_json_object(row.raw_payload))
        if not legacy_payload:
            legacy_payload = dict(EnterpriseRepository._coerce_json_object(row.payload_json))

        # Compatibility layer:
        # - Prefer new alert-centric raw_payload_json when present.
        # - Fall back to legacy raw_payload/payload_json for older runs.
        payload = dict(legacy_payload)
        if structured_payload:
            payload.update(structured_payload)
        payload.setdefault("raw_payload_json", structured_payload or legacy_payload)
        payload.pop("evaluation_label_is_sar", None)
        payload.setdefault("id", row.alert_id)
        payload["alert_id"] = row.alert_id
        payload["risk_score"] = float(row.risk_score or 0.0)
        payload["risk_band"] = row.risk_band
        payload["priority"] = row.priority or row.risk_band or "low"
        payload["status"] = row.status
        row_source_system = str(getattr(row, "source_system", "") or "").strip()
        if row_source_system:
            payload["source_system"] = row_source_system
        row_ingestion_run_id = str(getattr(row, "ingestion_run_id", "") or "").strip()
        if row_ingestion_run_id:
            payload.setdefault("ingestion_run_id", row_ingestion_run_id)
        row_schema_version = str(getattr(row, "schema_version", "") or "").strip()
        if row_schema_version:
            payload.setdefault("schema_version", row_schema_version)

        explain = dict(EnterpriseRepository._coerce_json_object(row.explainability_data))
        for key in ("risk_explain_json", "top_feature_contributions_json", "top_features_json", "ml_service_explain_json"):
            value = explain.get(key)
            if value is not None:
                payload[key] = value
        return payload

    @staticmethod
    def _coerce_json_object(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    @staticmethod
    def _coerce_optional_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value in {0, 0.0}:
                return False
            if value in {1, 1.0}:
                return True
        raw = str(value).strip().lower()
        if not raw:
            return None
        if raw in {"1", "true", "yes", "y"}:
            return True
        if raw in {"0", "false", "no", "n"}:
            return False
        return None

    @staticmethod
    def _sanitize_contract_object(
        value: dict[str, Any],
        *,
        max_items: int = 200,
        max_string_length: int = 2048,
    ) -> dict[str, Any]:
        blocked_tokens = {"password", "secret", "token", "apikey", "api_key", "authorization"}
        out: dict[str, Any] = {}
        for idx, (raw_key, raw_val) in enumerate(value.items()):
            if idx >= max_items:
                break
            key = str(raw_key)
            lowered = key.lower()
            if any(token in lowered for token in blocked_tokens):
                continue
            if isinstance(raw_val, dict):
                out[key] = EnterpriseRepository._sanitize_contract_object(
                    dict(raw_val),
                    max_items=max_items,
                    max_string_length=max_string_length,
                )
                continue
            if isinstance(raw_val, list):
                bounded_list = list(raw_val)[:max_items]
                normalized_items: list[Any] = []
                for item in bounded_list:
                    if isinstance(item, dict):
                        normalized_items.append(
                            EnterpriseRepository._sanitize_contract_object(
                                dict(item),
                                max_items=max_items,
                                max_string_length=max_string_length,
                            )
                        )
                    elif isinstance(item, str):
                        normalized_items.append(item[:max_string_length])
                    else:
                        normalized_items.append(EnterpriseRepository._json_safe(item))
                out[key] = normalized_items
                continue
            if isinstance(raw_val, str):
                out[key] = raw_val[:max_string_length]
                continue
            out[key] = EnterpriseRepository._json_safe(raw_val)
        return out

    @staticmethod
    def _build_ingestion_metadata(
        payload: dict[str, Any],
        run_id: str,
        source_system: str | None,
        schema_version: str | None,
    ) -> dict[str, Any]:
        metadata_payload = EnterpriseRepository._coerce_json_object(payload.get("ingestion_metadata_json"))
        warnings: list[str] = []
        raw_warnings = payload.get("ingestion_warnings")
        if isinstance(raw_warnings, list):
            warnings.extend(str(item) for item in raw_warnings if str(item).strip())
        if payload.get("ingestion_timestamp_fallback"):
            warnings.append("timestamp_fallback_applied")
        base = {
            "source_system": source_system or str(metadata_payload.get("source_system") or "unknown"),
            "schema_version": schema_version or str(metadata_payload.get("schema_version") or "unknown"),
            "ingestion_run_id": str(payload.get("ingestion_run_id") or run_id or ""),
            "ingestion_timestamp": str(payload.get("ingestion_timestamp") or payload.get("timestamp") or ""),
            "warnings": warnings[:20],
        }
        cleaned = EnterpriseRepository._sanitize_contract_object(base, max_items=100, max_string_length=512)
        metadata_context = EnterpriseRepository._sanitize_contract_object(
            metadata_payload,
            max_items=50,
            max_string_length=512,
        )
        if metadata_context:
            cleaned["context"] = metadata_context
        return cleaned

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {str(key): EnterpriseRepository._json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [EnterpriseRepository._json_safe(item) for item in value]
        return value

    def _get_latest_runtime_context_record(self, session: Session, tenant_id: str, user_scope: str) -> RuntimeContext | None:
        records = list(
            session.execute(
                select(RuntimeContext)
                .where(
                    RuntimeContext.tenant_id == tenant_id,
                    RuntimeContext.user_scope == user_scope,
                )
                .order_by(desc(RuntimeContext.updated_at), desc(RuntimeContext.id))
            ).scalars()
        )
        if not records:
            return None
        latest = records[0]
        for duplicate in records[1:]:
            session.delete(duplicate)
        return latest

    @staticmethod
    def _to_dict(record: Any) -> dict[str, Any]:
        if record is None:
            return {}
        out: dict[str, Any] = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            out[column.name] = value.isoformat() if isinstance(value, datetime) else value
        return out
