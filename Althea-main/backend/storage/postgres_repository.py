from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text, create_engine, desc, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


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

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    email: Mapped[str] = mapped_column(String(256), unique=True, index=True)
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


class EnterpriseRepository:
    def __init__(self, database_url: str) -> None:
        connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        self.engine = create_engine(database_url, future=True, connect_args=connect_args)
        self._session_factory = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False, future=True)
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_runtime_context(self, tenant_id: str, user_scope: str, **payload: Any) -> dict[str, Any]:
        with self.session() as session:
            record = self._get_latest_runtime_context_record(session, tenant_id, user_scope)
            if record is None:
                record = RuntimeContext(tenant_id=tenant_id, user_scope=user_scope)
                session.add(record)
            for key, value in payload.items():
                setattr(record, key, value)
            session.flush()
            return self._to_dict(record)

    def get_runtime_context(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
        with self.session() as session:
            record = self._get_latest_runtime_context_record(session, tenant_id, user_scope)
            if record is None:
                return self.upsert_runtime_context(tenant_id, user_scope)
            return self._to_dict(record)

    def clear_runtime_context(self, tenant_id: str, user_scope: str) -> dict[str, Any]:
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
        with self.session() as session:
            record = PipelineRunRecord(**payload)
            session.add(record)
            session.flush()
            return self._to_dict(record)

    def update_pipeline_job(self, job_id: str, **payload: Any) -> dict[str, Any] | None:
        with self.session() as session:
            record = session.get(PipelineRunRecord, job_id)
            if record is None:
                return None
            for key, value in payload.items():
                setattr(record, key, value)
            session.flush()
            return self._to_dict(record)

    def get_pipeline_job(self, tenant_id: str, job_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            record = session.execute(
                select(PipelineRunRecord).where(
                    PipelineRunRecord.tenant_id == tenant_id,
                    PipelineRunRecord.id == job_id,
                )
            ).scalar_one_or_none()
            return self._to_dict(record) if record else None

    def list_pipeline_runs(self, tenant_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(PipelineRunRecord)
                .where(PipelineRunRecord.tenant_id == tenant_id)
                .order_by(PipelineRunRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def save_alert_payloads(self, tenant_id: str, run_id: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        with self.session() as session:
            valid_records: list[dict[str, Any]] = []
            alert_ids: list[str] = []
            for payload in records:
                alert_id = str(payload.get("alert_id") or payload.get("id") or "").strip()
                if not alert_id:
                    continue
                valid_records.append(payload)
                alert_ids.append(alert_id)
            if not valid_records:
                return 0

            existing_rows = session.execute(
                select(AlertRecord).where(AlertRecord.id.in_(alert_ids))
            ).scalars()
            existing_by_id = {row.id: row for row in existing_rows}

            for payload in valid_records:
                alert_id = str(payload.get("alert_id") or payload.get("id"))
                row = existing_by_id.get(alert_id)
                if row is None:
                    session.add(
                        AlertRecord(
                            id=alert_id,
                            tenant_id=tenant_id,
                            run_id=run_id,
                            payload_json=payload,
                            created_at=_utcnow(),
                        )
                    )
                    continue
                row.tenant_id = tenant_id
                row.run_id = run_id
                row.payload_json = payload
            session.flush()
            return len(valid_records)

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(AlertRecord)
                .where(AlertRecord.tenant_id == tenant_id, AlertRecord.run_id == run_id)
                .order_by(AlertRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [dict(row.payload_json or {}) for row in rows]

    def create_user(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            record = UserRecord(**payload)
            session.add(record)
            session.flush()
            return self._to_dict(record)

    def get_user_by_email(self, tenant_id: str, email: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.email == email.lower())
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def get_user_by_id(self, tenant_id: str, user_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def list_users(self, tenant_id: str) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id).order_by(UserRecord.created_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def update_user_role(self, tenant_id: str, user_id: str, role: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(UserRecord).where(UserRecord.tenant_id == tenant_id, UserRecord.id == user_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            row.role = role
            session.flush()
            return self._to_dict(row)

    def create_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            record = UserSessionRecord(**payload)
            session.add(record)
            session.flush()
            return self._to_dict(record)

    def get_session(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(UserSessionRecord).where(
                    UserSessionRecord.tenant_id == tenant_id,
                    UserSessionRecord.session_id == session_id,
                )
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def revoke_session(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        with self.session() as session:
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
        with self.session() as session:
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
        with self.session() as session:
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
        with self.session() as session:
            row = session.get(AlertAssignmentRecord, payload["id"])
            if row is None:
                row = AlertAssignmentRecord(**payload)
                session.add(row)
            else:
                for key, value in payload.items():
                    setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def get_latest_assignment(self, tenant_id: str, alert_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(AlertAssignmentRecord)
                .where(AlertAssignmentRecord.tenant_id == tenant_id, AlertAssignmentRecord.alert_id == alert_id)
                .order_by(AlertAssignmentRecord.updated_at.desc())
            ).scalars().first()
            return self._to_dict(row) if row else None

    def create_alert_note(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            row = AlertNoteRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_alert_notes(self, tenant_id: str, alert_id: str) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(AlertNoteRecord)
                .where(AlertNoteRecord.tenant_id == tenant_id, AlertNoteRecord.alert_id == alert_id)
                .order_by(AlertNoteRecord.created_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def save_case(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            row = session.get(CaseRecord, payload["case_id"])
            if row is None:
                row = CaseRecord(**payload)
                session.add(row)
            else:
                for key, value in payload.items():
                    setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def get_case(self, tenant_id: str, case_id: str) -> dict[str, Any] | None:
        with self.session() as session:
            row = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id, CaseRecord.case_id == case_id)
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def list_cases(self, tenant_id: str) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id).order_by(CaseRecord.updated_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def delete_case(self, tenant_id: str, case_id: str) -> bool:
        with self.session() as session:
            row = session.execute(
                select(CaseRecord).where(CaseRecord.tenant_id == tenant_id, CaseRecord.case_id == case_id)
            ).scalar_one_or_none()
            if row is None:
                return False
            session.delete(row)
            return True

    def append_investigation_log(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            row = InvestigationLogRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_investigation_logs(
        self,
        tenant_id: str,
        case_id: str | None = None,
        alert_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with self.session() as session:
            stmt = select(InvestigationLogRecord).where(InvestigationLogRecord.tenant_id == tenant_id)
            if case_id:
                stmt = stmt.where(InvestigationLogRecord.case_id == case_id)
            if alert_id:
                stmt = stmt.where(InvestigationLogRecord.alert_id == alert_id)
            rows = session.execute(stmt.order_by(InvestigationLogRecord.timestamp.desc()).limit(limit)).scalars()
            return [self._to_dict(row) for row in rows]

    def register_model_version(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            row = ModelVersionRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_model_versions(self, tenant_id: str) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(ModelVersionRecord)
                .where(ModelVersionRecord.tenant_id == tenant_id)
                .order_by(ModelVersionRecord.created_at.desc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def store_feature_rows(
        self,
        tenant_id: str,
        run_id: str,
        feature_schema_hash: str,
        feature_rows: list[dict[str, Any]],
    ) -> int:
        with self.session() as session:
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
        with self.session() as session:
            rows = session.execute(
                select(FeatureStoreRecord)
                .where(FeatureStoreRecord.tenant_id == tenant_id, FeatureStoreRecord.run_id == run_id)
                .order_by(FeatureStoreRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [dict(row.features_json or {}) for row in rows]

    def save_model_monitoring(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.session() as session:
            row = ModelMonitoringRecord(**payload)
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_model_monitoring(self, tenant_id: str, limit: int = 200) -> list[dict[str, Any]]:
        with self.session() as session:
            rows = session.execute(
                select(ModelMonitoringRecord)
                .where(ModelMonitoringRecord.tenant_id == tenant_id)
                .order_by(ModelMonitoringRecord.created_at.desc())
                .limit(limit)
            ).scalars()
            return [self._to_dict(row) for row in rows]

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
