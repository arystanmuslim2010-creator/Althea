from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EnrichmentBase(DeclarativeBase):
    pass


class EnrichmentAccountEventRecord(EnrichmentBase):
    __tablename__ = "enrichment_account_events"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "source_name",
            "source_record_id",
            name="uq_enrichment_account_events_tenant_source_record",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    source_record_id: Mapped[str] = mapped_column(String(256), index=True)
    entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    counterparty_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    counterparty_account_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    bank_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    counterparty_bank_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), default="transaction")
    direction: Mapped[str | None] = mapped_column(String(32), nullable=True)
    amount: Mapped[float] = mapped_column(Float, default=0.0)
    currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    channel: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payment_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_cross_border: Mapped[bool] = mapped_column(Boolean, default=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    raw_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EnrichmentAlertOutcomeRecord(EnrichmentBase):
    __tablename__ = "enrichment_alert_outcomes"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    alert_id: Mapped[str] = mapped_column(String(128), index=True)
    case_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    decision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reason_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    decided_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EnrichmentCaseActionRecord(EnrichmentBase):
    __tablename__ = "enrichment_case_actions"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    case_id: Mapped[str] = mapped_column(String(128), index=True)
    alert_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(128))
    actor_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EnrichmentSyncStateRecord(EnrichmentBase):
    __tablename__ = "enrichment_sync_state"
    __table_args__ = (
        UniqueConstraint("tenant_id", "source_name", name="uq_enrichment_sync_state_tenant_source"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    cursor: Mapped[str | None] = mapped_column(String(512), nullable=True)
    last_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="idle")
    records_read: Mapped[int] = mapped_column(Integer, default=0)
    records_written: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)


class EnrichmentSourceHealthRecord(EnrichmentBase):
    __tablename__ = "enrichment_source_health"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    measured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    freshness_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    lag_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    coverage_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    error_rate: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(32), default="healthy")
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class MasterCustomerRecord(EnrichmentBase):
    __tablename__ = "master_customers"
    __table_args__ = (
        UniqueConstraint("tenant_id", "source_name", "customer_id", name="uq_master_customers_tenant_source_customer"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    customer_id: Mapped[str] = mapped_column(String(128), index=True)
    external_customer_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    risk_tier: Mapped[str | None] = mapped_column(String(64), nullable=True)
    segment: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    pep_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    sanctions_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    kyc_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class MasterAccountRecord(EnrichmentBase):
    __tablename__ = "master_accounts"
    __table_args__ = (
        UniqueConstraint("tenant_id", "source_name", "account_id", name="uq_master_accounts_tenant_source_account"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    account_id: Mapped[str] = mapped_column(String(128), index=True)
    external_account_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    customer_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    bank_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    account_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class MasterCounterpartyRecord(EnrichmentBase):
    __tablename__ = "master_counterparties"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "source_name",
            "counterparty_id",
            name="uq_master_counterparties_tenant_source_counterparty",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    counterparty_id: Mapped[str] = mapped_column(String(128), index=True)
    external_counterparty_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    bank_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EntityAliasRecord(EnrichmentBase):
    __tablename__ = "entity_aliases"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "entity_type",
            "source_name",
            "external_id",
            name="uq_entity_aliases_tenant_entity_source_external",
        ),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    canonical_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    external_id: Mapped[str] = mapped_column(String(256), index=True)
    alias_type: Mapped[str] = mapped_column(String(64), default="source_exact")
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class EntityLinkRecord(EnrichmentBase):
    __tablename__ = "entity_links"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    left_entity_type: Mapped[str] = mapped_column(String(64), index=True)
    left_entity_id: Mapped[str] = mapped_column(String(128), index=True)
    right_entity_type: Mapped[str] = mapped_column(String(64), index=True)
    right_entity_id: Mapped[str] = mapped_column(String(128), index=True)
    link_type: Mapped[str] = mapped_column(String(64), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class MasterDataOverrideRecord(EnrichmentBase):
    __tablename__ = "master_data_overrides"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    override_type: Mapped[str] = mapped_column(String(64), index=True)
    left_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    left_entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    right_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    right_entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target_entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class EnrichmentAuditLogRecord(EnrichmentBase):
    __tablename__ = "enrichment_audit_log"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(128), index=True)
    actor_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class EnrichmentDeadLetterRecord(EnrichmentBase):
    __tablename__ = "enrichment_dead_letter"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    source_record_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    error_code: Mapped[str] = mapped_column(String(128), index=True)
    error_message: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    replay_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="pending")


class EnrichmentSchemaRegistryRecord(EnrichmentBase):
    __tablename__ = "enrichment_schema_registry"
    __table_args__ = (
        UniqueConstraint("source_name", "schema_version", name="uq_enrichment_schema_registry_source_version"),
    )

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    schema_version: Mapped[str] = mapped_column(String(128), index=True)
    observed_fields_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    drift_status: Mapped[str] = mapped_column(String(32), default="known")


class EnrichmentCoverageSnapshotRecord(EnrichmentBase):
    __tablename__ = "enrichment_coverage_snapshots"

    id: Mapped[str] = mapped_column(String(128), primary_key=True, default=lambda: uuid.uuid4().hex)
    tenant_id: Mapped[str] = mapped_column(String(128), index=True)
    source_name: Mapped[str] = mapped_column(String(64), index=True)
    alert_type: Mapped[str] = mapped_column(String(64), default="all", index=True)
    coverage_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    matched_alerts: Mapped[int] = mapped_column(Integer, default=0)
    total_alerts: Mapped[int] = mapped_column(Integer, default=0)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    measured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
