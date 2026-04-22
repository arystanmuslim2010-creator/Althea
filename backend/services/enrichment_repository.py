from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import desc, func, select, text

from storage.enrichment_models import (
    EnrichmentAccountEventRecord,
    EnrichmentAlertOutcomeRecord,
    EnrichmentAuditLogRecord,
    EnrichmentCaseActionRecord,
    EnrichmentCoverageSnapshotRecord,
    EnrichmentDeadLetterRecord,
    EnrichmentSchemaRegistryRecord,
    EnrichmentSourceHealthRecord,
    EnrichmentSyncStateRecord,
    EntityAliasRecord,
    EntityLinkRecord,
    MasterAccountRecord,
    MasterCounterpartyRecord,
    MasterCustomerRecord,
    MasterDataOverrideRecord,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EnrichmentRepository:
    def __init__(self, repository) -> None:
        self._repository = repository

    @property
    def engine(self):
        return self._repository.engine

    def _require_tenant(self, tenant_id: str) -> str:
        return self._repository._require_tenant(tenant_id)

    @staticmethod
    def _to_dict(record: Any) -> dict[str, Any]:
        if record is None:
            return {}
        out: dict[str, Any] = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            out[column.name] = value.isoformat() if isinstance(value, datetime) else value
        return out

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): EnrichmentRepository._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [EnrichmentRepository._json_safe(v) for v in value]
        return value

    @staticmethod
    def _sanitize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        blocked_tokens = {"password", "secret", "token", "authorization", "apikey", "api_key"}
        out: dict[str, Any] = {}
        for key, value in payload.items():
            clean_key = str(key)
            if any(token in clean_key.lower() for token in blocked_tokens):
                continue
            if isinstance(value, str):
                out[clean_key] = value[:4096]
            elif isinstance(value, dict):
                out[clean_key] = EnrichmentRepository._sanitize_payload(value)
            elif isinstance(value, list):
                out[clean_key] = [
                    EnrichmentRepository._sanitize_payload(item) if isinstance(item, dict) else EnrichmentRepository._json_safe(item)
                    for item in value[:200]
                ]
            else:
                out[clean_key] = EnrichmentRepository._json_safe(value)
        return out

    @staticmethod
    def _parse_dt(value: Any) -> datetime | None:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            return parsed.to_pydatetime()
        return parsed

    def list_registered_sources(self) -> list[str]:
        return [
            "internal_case",
            "internal_outcome",
            "kyc",
            "watchlist",
            "device",
            "channel",
        ]

    def has_canonical_history(self, tenant_id: str) -> bool:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            total = session.execute(
                select(func.count()).select_from(EnrichmentAccountEventRecord).where(
                    EnrichmentAccountEventRecord.tenant_id == tenant_id,
                )
            ).scalar_one()
            return int(total or 0) > 0

    def append_account_events(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                source_name = str(payload.get("source_name") or "unknown").strip() or "unknown"
                source_record_id = str(payload.get("source_record_id") or payload.get("id") or uuid.uuid4().hex).strip()
                existing = session.execute(
                    select(EnrichmentAccountEventRecord).where(
                        EnrichmentAccountEventRecord.tenant_id == tenant_id,
                        EnrichmentAccountEventRecord.source_name == source_name,
                        EnrichmentAccountEventRecord.source_record_id == source_record_id,
                    )
                ).scalar_one_or_none()
                event_time = self._parse_dt(payload.get("event_time")) or _utcnow()
                row = existing or EnrichmentAccountEventRecord(
                    tenant_id=tenant_id,
                    source_name=source_name,
                    source_record_id=source_record_id,
                )
                row.entity_id = str(payload.get("entity_id") or "").strip() or None
                row.account_id = str(payload.get("account_id") or "").strip() or None
                row.counterparty_id = str(payload.get("counterparty_id") or "").strip() or None
                row.counterparty_account_id = str(payload.get("counterparty_account_id") or "").strip() or None
                row.bank_id = str(payload.get("bank_id") or "").strip() or None
                row.counterparty_bank_id = str(payload.get("counterparty_bank_id") or "").strip() or None
                row.event_type = str(payload.get("event_type") or "transaction").strip() or "transaction"
                row.direction = str(payload.get("direction") or "").strip() or None
                row.amount = float(payload.get("amount") or 0.0)
                row.currency = str(payload.get("currency") or "").strip() or None
                row.country = str(payload.get("country") or "").strip() or None
                row.channel = str(payload.get("channel") or "").strip() or None
                row.payment_type = str(payload.get("payment_type") or "").strip() or None
                row.is_cross_border = bool(payload.get("is_cross_border", False))
                row.event_time = event_time
                row.ingested_at = _utcnow()
                row.raw_ref = str(payload.get("raw_ref") or "").strip() or None
                row.raw_payload_json = self._sanitize_payload(dict(payload.get("raw_payload_json") or payload))
                if existing is None:
                    session.add(row)
                written += 1
            return written

    def append_alert_outcomes(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                row = EnrichmentAlertOutcomeRecord(
                    id=str(payload.get("id") or uuid.uuid4().hex),
                    tenant_id=tenant_id,
                    source_name=str(payload.get("source_name") or "internal_outcome"),
                    alert_id=str(payload.get("alert_id") or ""),
                    case_id=str(payload.get("case_id") or "").strip() or None,
                    entity_id=str(payload.get("entity_id") or "").strip() or None,
                    decision=str(payload.get("decision") or "").strip() or None,
                    status=str(payload.get("status") or "").strip() or None,
                    reason_code=str(payload.get("reason_code") or "").strip() or None,
                    decided_by=str(payload.get("decided_by") or "").strip() or None,
                    event_time=self._parse_dt(payload.get("event_time")) or _utcnow(),
                    ingested_at=_utcnow(),
                    payload_json=self._sanitize_payload(dict(payload.get("payload_json") or payload)),
                )
                session.add(row)
                written += 1
            return written

    def append_case_actions(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                row = EnrichmentCaseActionRecord(
                    id=str(payload.get("id") or uuid.uuid4().hex),
                    tenant_id=tenant_id,
                    source_name=str(payload.get("source_name") or "internal_case"),
                    case_id=str(payload.get("case_id") or ""),
                    alert_id=str(payload.get("alert_id") or "").strip() or None,
                    entity_id=str(payload.get("entity_id") or "").strip() or None,
                    action=str(payload.get("action") or "case_event"),
                    actor_id=str(payload.get("actor_id") or "").strip() or None,
                    actor_role=str(payload.get("actor_role") or "").strip() or None,
                    event_time=self._parse_dt(payload.get("event_time")) or _utcnow(),
                    ingested_at=_utcnow(),
                    details_json=self._sanitize_payload(dict(payload.get("details_json") or payload)),
                )
                session.add(row)
                written += 1
            return written

    def get_sync_state(self, tenant_id: str, source_name: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(EnrichmentSyncStateRecord).where(
                    EnrichmentSyncStateRecord.tenant_id == tenant_id,
                    EnrichmentSyncStateRecord.source_name == source_name,
                )
            ).scalar_one_or_none()
            return self._to_dict(row) if row else None

    def list_sync_states(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(EnrichmentSyncStateRecord)
                .where(EnrichmentSyncStateRecord.tenant_id == tenant_id)
                .order_by(EnrichmentSyncStateRecord.source_name.asc())
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def upsert_sync_state(self, tenant_id: str, source_name: str, **payload: Any) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(EnrichmentSyncStateRecord).where(
                    EnrichmentSyncStateRecord.tenant_id == tenant_id,
                    EnrichmentSyncStateRecord.source_name == source_name,
                )
            ).scalar_one_or_none()
            if row is None:
                row = EnrichmentSyncStateRecord(
                    tenant_id=tenant_id,
                    source_name=source_name,
                    status=str(payload.get("status") or "idle"),
                )
                session.add(row)
            for key, value in payload.items():
                if key in {"last_event_time", "last_success_at", "last_attempt_at"}:
                    setattr(row, key, self._parse_dt(value))
                else:
                    setattr(row, key, value)
            session.flush()
            return self._to_dict(row)

    def write_source_health(self, tenant_id: str, source_name: str, **payload: Any) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = EnrichmentSourceHealthRecord(
                tenant_id=tenant_id,
                source_name=source_name,
                measured_at=self._parse_dt(payload.get("measured_at")) or _utcnow(),
                freshness_seconds=float(payload.get("freshness_seconds") or 0.0),
                lag_seconds=float(payload.get("lag_seconds") or 0.0),
                coverage_ratio=float(payload.get("coverage_ratio") or 0.0),
                error_rate=float(payload.get("error_rate") or 0.0),
                status=str(payload.get("status") or "healthy"),
                details_json=self._sanitize_payload(dict(payload.get("details_json") or {})),
            )
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_latest_source_health(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        items: list[dict[str, Any]] = []
        with self._repository.session(tenant_id=tenant_id) as session:
            for source_name in self.list_registered_sources():
                row = session.execute(
                    select(EnrichmentSourceHealthRecord)
                    .where(
                        EnrichmentSourceHealthRecord.tenant_id == tenant_id,
                        EnrichmentSourceHealthRecord.source_name == source_name,
                    )
                    .order_by(desc(EnrichmentSourceHealthRecord.measured_at))
                    .limit(1)
                ).scalar_one_or_none()
                if row is not None:
                    items.append(self._to_dict(row))
        return items

    def list_account_events_before(
        self,
        tenant_id: str,
        *,
        entity_ids: list[str],
        as_of_timestamp: Any = None,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        clean_ids = [str(item).strip() for item in entity_ids if str(item).strip()]
        if not clean_ids:
            return []
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = (
                select(EnrichmentAccountEventRecord)
                .where(
                    EnrichmentAccountEventRecord.tenant_id == tenant_id,
                    EnrichmentAccountEventRecord.entity_id.in_(clean_ids),
                )
                .order_by(desc(EnrichmentAccountEventRecord.event_time))
                .limit(max(1, int(limit)))
            )
            cutoff = self._parse_dt(as_of_timestamp)
            if cutoff is not None:
                stmt = stmt.where(EnrichmentAccountEventRecord.event_time <= cutoff)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def list_alert_outcomes_before(
        self,
        tenant_id: str,
        *,
        entity_ids: list[str] | None = None,
        alert_ids: list[str] | None = None,
        as_of_timestamp: Any = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        clean_entities = [str(item).strip() for item in (entity_ids or []) if str(item).strip()]
        clean_alerts = [str(item).strip() for item in (alert_ids or []) if str(item).strip()]
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = (
                select(EnrichmentAlertOutcomeRecord)
                .where(EnrichmentAlertOutcomeRecord.tenant_id == tenant_id)
                .order_by(desc(EnrichmentAlertOutcomeRecord.event_time))
                .limit(max(1, int(limit)))
            )
            if clean_entities:
                stmt = stmt.where(EnrichmentAlertOutcomeRecord.entity_id.in_(clean_entities))
            if clean_alerts:
                stmt = stmt.where(EnrichmentAlertOutcomeRecord.alert_id.in_(clean_alerts))
            cutoff = self._parse_dt(as_of_timestamp)
            if cutoff is not None:
                stmt = stmt.where(EnrichmentAlertOutcomeRecord.event_time <= cutoff)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def list_case_actions_before(
        self,
        tenant_id: str,
        *,
        entity_ids: list[str] | None = None,
        alert_ids: list[str] | None = None,
        as_of_timestamp: Any = None,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        clean_entities = [str(item).strip() for item in (entity_ids or []) if str(item).strip()]
        clean_alerts = [str(item).strip() for item in (alert_ids or []) if str(item).strip()]
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = (
                select(EnrichmentCaseActionRecord)
                .where(EnrichmentCaseActionRecord.tenant_id == tenant_id)
                .order_by(desc(EnrichmentCaseActionRecord.event_time))
                .limit(max(1, int(limit)))
            )
            if clean_entities:
                stmt = stmt.where(EnrichmentCaseActionRecord.entity_id.in_(clean_entities))
            if clean_alerts:
                stmt = stmt.where(EnrichmentCaseActionRecord.alert_id.in_(clean_alerts))
            cutoff = self._parse_dt(as_of_timestamp)
            if cutoff is not None:
                stmt = stmt.where(EnrichmentCaseActionRecord.event_time <= cutoff)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def get_enrichment_context_snapshot(self, tenant_id: str, alert_id: str, as_of_timestamp: Any = None) -> dict[str, Any]:
        payload = self._repository.get_alert_payload(tenant_id=tenant_id, alert_id=alert_id, run_id=None) or {}
        entity_candidates = [
            str(payload.get("user_id") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            str(payload.get("account_id") or "").strip(),
        ]
        for account in payload.get("accounts") or []:
            if isinstance(account, dict):
                entity_candidates.extend(
                    [
                        str(account.get("customer_id") or "").strip(),
                        str(account.get("account_id") or "").strip(),
                    ]
                )
        entity_ids = [value for value in entity_candidates if value]
        cutoff = as_of_timestamp or payload.get("timestamp") or payload.get("created_at")
        return {
            "alert_payload": payload,
            "entity_ids": entity_ids,
            "account_events": self.list_account_events_before(tenant_id, entity_ids=entity_ids, as_of_timestamp=cutoff),
            "alert_outcomes": self.list_alert_outcomes_before(
                tenant_id,
                entity_ids=entity_ids,
                alert_ids=[alert_id],
                as_of_timestamp=cutoff,
            ),
            "case_actions": self.list_case_actions_before(
                tenant_id,
                entity_ids=entity_ids,
                alert_ids=[alert_id],
                as_of_timestamp=cutoff,
            ),
        }

    def append_master_customers(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                source_name = str(payload.get("source_name") or "unknown")
                customer_id = str(payload.get("customer_id") or "").strip()
                if not customer_id:
                    continue
                existing = session.execute(
                    select(MasterCustomerRecord).where(
                        MasterCustomerRecord.tenant_id == tenant_id,
                        MasterCustomerRecord.source_name == source_name,
                        MasterCustomerRecord.customer_id == customer_id,
                    )
                ).scalar_one_or_none()
                row = existing or MasterCustomerRecord(
                    tenant_id=tenant_id,
                    source_name=source_name,
                    customer_id=customer_id,
                )
                row.external_customer_id = str(payload.get("external_customer_id") or "").strip() or None
                row.risk_tier = str(payload.get("risk_tier") or "").strip() or None
                row.segment = str(payload.get("segment") or "").strip() or None
                row.country = str(payload.get("country") or "").strip() or None
                row.pep_flag = bool(payload.get("pep_flag", False))
                row.sanctions_flag = bool(payload.get("sanctions_flag", False))
                row.kyc_status = str(payload.get("kyc_status") or "").strip() or None
                row.effective_from = self._parse_dt(payload.get("effective_from"))
                row.effective_to = self._parse_dt(payload.get("effective_to"))
                row.payload_json = self._sanitize_payload(dict(payload.get("payload_json") or payload))
                if existing is None:
                    session.add(row)
                written += 1
            return written

    def append_master_accounts(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                source_name = str(payload.get("source_name") or "unknown")
                account_id = str(payload.get("account_id") or "").strip()
                if not account_id:
                    continue
                existing = session.execute(
                    select(MasterAccountRecord).where(
                        MasterAccountRecord.tenant_id == tenant_id,
                        MasterAccountRecord.source_name == source_name,
                        MasterAccountRecord.account_id == account_id,
                    )
                ).scalar_one_or_none()
                row = existing or MasterAccountRecord(
                    tenant_id=tenant_id,
                    source_name=source_name,
                    account_id=account_id,
                )
                row.external_account_id = str(payload.get("external_account_id") or "").strip() or None
                row.customer_id = str(payload.get("customer_id") or "").strip() or None
                row.bank_id = str(payload.get("bank_id") or "").strip() or None
                row.account_type = str(payload.get("account_type") or "").strip() or None
                row.country = str(payload.get("country") or "").strip() or None
                row.opened_at = self._parse_dt(payload.get("opened_at"))
                row.closed_at = self._parse_dt(payload.get("closed_at"))
                row.status = str(payload.get("status") or "").strip() or None
                row.effective_from = self._parse_dt(payload.get("effective_from"))
                row.effective_to = self._parse_dt(payload.get("effective_to"))
                row.payload_json = self._sanitize_payload(dict(payload.get("payload_json") or payload))
                if existing is None:
                    session.add(row)
                written += 1
            return written

    def append_master_counterparties(self, tenant_id: str, records: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        if not records:
            return 0
        with self._repository.session(tenant_id=tenant_id) as session:
            written = 0
            for payload in records:
                source_name = str(payload.get("source_name") or "unknown")
                counterparty_id = str(payload.get("counterparty_id") or "").strip()
                if not counterparty_id:
                    continue
                existing = session.execute(
                    select(MasterCounterpartyRecord).where(
                        MasterCounterpartyRecord.tenant_id == tenant_id,
                        MasterCounterpartyRecord.source_name == source_name,
                        MasterCounterpartyRecord.counterparty_id == counterparty_id,
                    )
                ).scalar_one_or_none()
                row = existing or MasterCounterpartyRecord(
                    tenant_id=tenant_id,
                    source_name=source_name,
                    counterparty_id=counterparty_id,
                )
                row.external_counterparty_id = str(payload.get("external_counterparty_id") or "").strip() or None
                row.bank_id = str(payload.get("bank_id") or "").strip() or None
                row.country = str(payload.get("country") or "").strip() or None
                row.entity_type = str(payload.get("entity_type") or "").strip() or None
                row.effective_from = self._parse_dt(payload.get("effective_from"))
                row.effective_to = self._parse_dt(payload.get("effective_to"))
                row.payload_json = self._sanitize_payload(dict(payload.get("payload_json") or payload))
                if existing is None:
                    session.add(row)
                written += 1
            return written

    def list_master_customers(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(select(MasterCustomerRecord).where(MasterCustomerRecord.tenant_id == tenant_id)).scalars()
            return [self._to_dict(row) for row in rows]

    def list_master_accounts(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(select(MasterAccountRecord).where(MasterAccountRecord.tenant_id == tenant_id)).scalars()
            return [self._to_dict(row) for row in rows]

    def list_master_counterparties(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(select(MasterCounterpartyRecord).where(MasterCounterpartyRecord.tenant_id == tenant_id)).scalars()
            return [self._to_dict(row) for row in rows]

    def replace_entity_aliases(self, tenant_id: str, source_name: str, aliases: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            existing = session.execute(
                select(EntityAliasRecord).where(
                    EntityAliasRecord.tenant_id == tenant_id,
                    EntityAliasRecord.source_name == source_name,
                )
            ).scalars().all()
            for row in existing:
                session.delete(row)
            for payload in aliases:
                session.add(
                    EntityAliasRecord(
                        tenant_id=tenant_id,
                        entity_type=str(payload.get("entity_type") or ""),
                        canonical_id=str(payload.get("canonical_id") or ""),
                        source_name=source_name,
                        external_id=str(payload.get("external_id") or ""),
                        alias_type=str(payload.get("alias_type") or "source_exact"),
                        confidence=float(payload.get("confidence") or 1.0),
                        created_at=self._parse_dt(payload.get("created_at")) or _utcnow(),
                    )
                )
            return len(aliases)

    def replace_entity_links(self, tenant_id: str, source_name: str, links: list[dict[str, Any]]) -> int:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            existing = session.execute(
                select(EntityLinkRecord).where(
                    EntityLinkRecord.tenant_id == tenant_id,
                    EntityLinkRecord.source_name == source_name,
                )
            ).scalars().all()
            for row in existing:
                session.delete(row)
            for payload in links:
                session.add(
                    EntityLinkRecord(
                        tenant_id=tenant_id,
                        left_entity_type=str(payload.get("left_entity_type") or ""),
                        left_entity_id=str(payload.get("left_entity_id") or ""),
                        right_entity_type=str(payload.get("right_entity_type") or ""),
                        right_entity_id=str(payload.get("right_entity_id") or ""),
                        link_type=str(payload.get("link_type") or "related"),
                        confidence=float(payload.get("confidence") or 1.0),
                        source_name=source_name,
                        created_at=self._parse_dt(payload.get("created_at")) or _utcnow(),
                        metadata_json=self._sanitize_payload(dict(payload.get("metadata_json") or {})),
                    )
                )
            return len(links)

    def list_entity_aliases(self, tenant_id: str, *, source_name: str | None = None, external_id: str | None = None, canonical_id: str | None = None) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = select(EntityAliasRecord).where(EntityAliasRecord.tenant_id == tenant_id)
            if source_name:
                stmt = stmt.where(EntityAliasRecord.source_name == source_name)
            if external_id:
                stmt = stmt.where(EntityAliasRecord.external_id == external_id)
            if canonical_id:
                stmt = stmt.where(EntityAliasRecord.canonical_id == canonical_id)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def list_entity_links(self, tenant_id: str, entity_id: str | None = None) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = select(EntityLinkRecord).where(EntityLinkRecord.tenant_id == tenant_id)
            if entity_id:
                stmt = stmt.where(
                    (EntityLinkRecord.left_entity_id == entity_id) | (EntityLinkRecord.right_entity_id == entity_id)
                )
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def create_master_data_override(self, tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = MasterDataOverrideRecord(
                tenant_id=tenant_id,
                override_type=str(payload.get("override_type") or ""),
                left_entity_type=str(payload.get("left_entity_type") or "").strip() or None,
                left_entity_id=str(payload.get("left_entity_id") or "").strip() or None,
                right_entity_type=str(payload.get("right_entity_type") or "").strip() or None,
                right_entity_id=str(payload.get("right_entity_id") or "").strip() or None,
                target_entity_type=str(payload.get("target_entity_type") or "").strip() or None,
                target_entity_id=str(payload.get("target_entity_id") or "").strip() or None,
                source_name=str(payload.get("source_name") or "").strip() or None,
                external_id=str(payload.get("external_id") or "").strip() or None,
                created_by=str(payload.get("created_by") or "").strip() or None,
                reason=str(payload.get("reason") or "").strip() or None,
                payload_json=self._sanitize_payload(dict(payload.get("payload_json") or payload)),
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_master_data_overrides(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(MasterDataOverrideRecord)
                .where(MasterDataOverrideRecord.tenant_id == tenant_id)
                .order_by(desc(MasterDataOverrideRecord.created_at))
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def append_audit_log(self, tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = EnrichmentAuditLogRecord(
                tenant_id=tenant_id,
                source_name=str(payload.get("source_name") or "").strip() or None,
                action=str(payload.get("action") or ""),
                actor_id=str(payload.get("actor_id") or "").strip() or None,
                status=str(payload.get("status") or "").strip() or None,
                details_json=self._sanitize_payload(dict(payload.get("details_json") or {})),
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_audit_logs(self, tenant_id: str, limit: int = 200) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(EnrichmentAuditLogRecord)
                .where(EnrichmentAuditLogRecord.tenant_id == tenant_id)
                .order_by(desc(EnrichmentAuditLogRecord.created_at))
                .limit(max(1, int(limit)))
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def append_dead_letter(self, tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = EnrichmentDeadLetterRecord(
                tenant_id=tenant_id,
                source_name=str(payload.get("source_name") or ""),
                source_record_id=str(payload.get("source_record_id") or "").strip() or None,
                error_code=str(payload.get("error_code") or "unknown_error"),
                error_message=str(payload.get("error_message") or ""),
                payload_json=self._sanitize_payload(dict(payload.get("payload_json") or {})),
                failed_at=_utcnow(),
                replay_count=int(payload.get("replay_count") or 0),
                status=str(payload.get("status") or "pending"),
            )
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_dead_letters(self, tenant_id: str, source_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            stmt = (
                select(EnrichmentDeadLetterRecord)
                .where(EnrichmentDeadLetterRecord.tenant_id == tenant_id)
                .order_by(desc(EnrichmentDeadLetterRecord.failed_at))
                .limit(max(1, int(limit)))
            )
            if source_name:
                stmt = stmt.where(EnrichmentDeadLetterRecord.source_name == source_name)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def mark_dead_letter_replayed(self, tenant_id: str, dead_letter_id: str) -> dict[str, Any] | None:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                select(EnrichmentDeadLetterRecord).where(
                    EnrichmentDeadLetterRecord.tenant_id == tenant_id,
                    EnrichmentDeadLetterRecord.id == dead_letter_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            row.replay_count = int(row.replay_count or 0) + 1
            row.status = "replayed"
            session.flush()
            return self._to_dict(row)

    def upsert_schema_registry(self, source_name: str, schema_version: str, observed_fields_json: dict[str, Any], drift_status: str) -> dict[str, Any]:
        with self._repository.session() as session:
            row = session.execute(
                select(EnrichmentSchemaRegistryRecord).where(
                    EnrichmentSchemaRegistryRecord.source_name == source_name,
                    EnrichmentSchemaRegistryRecord.schema_version == schema_version,
                )
            ).scalar_one_or_none()
            if row is None:
                row = EnrichmentSchemaRegistryRecord(
                    source_name=source_name,
                    schema_version=schema_version,
                    observed_fields_json=self._sanitize_payload(observed_fields_json),
                    first_seen_at=_utcnow(),
                    last_seen_at=_utcnow(),
                    drift_status=drift_status,
                )
                session.add(row)
            else:
                row.observed_fields_json = self._sanitize_payload(observed_fields_json)
                row.last_seen_at = _utcnow()
                row.drift_status = drift_status
            session.flush()
            return self._to_dict(row)

    def list_schema_registry(self, source_name: str | None = None) -> list[dict[str, Any]]:
        with self._repository.session() as session:
            stmt = select(EnrichmentSchemaRegistryRecord).order_by(desc(EnrichmentSchemaRegistryRecord.last_seen_at))
            if source_name:
                stmt = stmt.where(EnrichmentSchemaRegistryRecord.source_name == source_name)
            rows = session.execute(stmt).scalars()
            return [self._to_dict(row) for row in rows]

    def write_coverage_snapshot(self, tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            row = EnrichmentCoverageSnapshotRecord(
                tenant_id=tenant_id,
                source_name=str(payload.get("source_name") or ""),
                alert_type=str(payload.get("alert_type") or "all"),
                coverage_ratio=float(payload.get("coverage_ratio") or 0.0),
                matched_alerts=int(payload.get("matched_alerts") or 0),
                total_alerts=int(payload.get("total_alerts") or 0),
                details_json=self._sanitize_payload(dict(payload.get("details_json") or {})),
                measured_at=self._parse_dt(payload.get("measured_at")) or _utcnow(),
            )
            session.add(row)
            session.flush()
            return self._to_dict(row)

    def list_coverage_snapshots(self, tenant_id: str, limit: int = 200) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                select(EnrichmentCoverageSnapshotRecord)
                .where(EnrichmentCoverageSnapshotRecord.tenant_id == tenant_id)
                .order_by(desc(EnrichmentCoverageSnapshotRecord.measured_at))
                .limit(max(1, int(limit)))
            ).scalars()
            return [self._to_dict(row) for row in rows]

    def extract_internal_case_actions(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, case_id, alert_id, action, performed_by, timestamp, details_json
                    FROM investigation_logs
                    WHERE tenant_id = :tenant_id
                    ORDER BY timestamp ASC
                    """
                ),
                {"tenant_id": tenant_id},
            ).fetchall()
            return [
                {
                    "id": f"case_action_{row[0]}",
                    "source_name": "internal_case",
                    "case_id": str(row[1] or ""),
                    "alert_id": str(row[2] or "").strip() or None,
                    "action": str(row[3] or "case_event"),
                    "actor_id": str(row[4] or "").strip() or None,
                    "actor_role": None,
                    "event_time": row[5],
                    "details_json": row[6] or {},
                }
                for row in rows
            ]

    def extract_internal_alert_outcomes(self, tenant_id: str) -> list[dict[str, Any]]:
        tenant_id = self._require_tenant(tenant_id)
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, alert_id, analyst_decision, decision_reason, analyst_id, timestamp
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                    ORDER BY timestamp ASC
                    """
                ),
                {"tenant_id": tenant_id},
            ).fetchall()
            return [
                {
                    "id": f"outcome_{row[0]}",
                    "source_name": "internal_outcome",
                    "alert_id": str(row[1] or ""),
                    "decision": str(row[2] or "").strip() or None,
                    "status": str(row[2] or "").strip() or None,
                    "reason_code": str(row[3] or "").strip() or None,
                    "decided_by": str(row[4] or "").strip() or None,
                    "event_time": row[5],
                    "payload_json": {
                        "decision_reason": row[3],
                    },
                }
                for row in rows
            ]
