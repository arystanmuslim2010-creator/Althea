"""Global Pattern Service — cross-tenant anonymized intelligence signals."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger("althea.intelligence.global_patterns")

# Signal types that are collected across tenants
SIGNAL_TYPES = frozenset(
    {"device_fingerprint", "transaction_pattern", "typology_indicator", "risk_velocity", "beneficiary_pattern"}
)


def _anonymize(value: str) -> str:
    """One-way SHA-256 hash of a raw signal value to prevent cross-tenant data exposure."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class GlobalPatternService:
    """Detect and report patterns across tenants using anonymized signal hashes.

    Raw data is NEVER stored — only irreversible hashes.
    This allows cross-tenant intelligence without violating tenant data isolation.
    """

    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _parse_json(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw.strip()) if raw.strip() else default
            except Exception:
                return default
        return default

    def _extract_signals_from_payload(self, payload: dict) -> list[tuple[str, str]]:
        """Extract (signal_type, raw_value) pairs from alert payload."""
        signals: list[tuple[str, str]] = []

        # Device fingerprint signal
        device_id = str(payload.get("device_id") or payload.get("device_fingerprint") or "")
        if device_id:
            signals.append(("device_fingerprint", device_id))

        # Beneficiary pattern
        benef = str(payload.get("beneficiary_id") or payload.get("beneficiary_account") or "")
        if benef:
            signals.append(("beneficiary_pattern", benef))

        # Typology indicator
        typology = str(payload.get("typology") or "")
        country = str(payload.get("country") or "")
        if typology and country:
            signals.append(("typology_indicator", f"{typology}:{country}"))
        elif typology:
            signals.append(("typology_indicator", typology))

        # Risk velocity indicator — bucket the score to avoid fingerprinting individuals
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        bucket = int(risk_score // 10) * 10  # e.g., 87 → 80
        segment = str(payload.get("segment") or "")
        if segment:
            signals.append(("risk_velocity", f"{segment}:{bucket}"))

        return signals

    def ingest_alert_signals(self, tenant_id: str, alert_id: str, run_id: str | None = None) -> int:
        """Extract signals from an alert and upsert into global_pattern_signals.

        Returns the number of signals ingested.
        """
        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=5)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})
        if not payload:
            logger.warning("Alert not found for signal ingestion", extra={"alert_id": alert_id})
            return 0

        signals = self._extract_signals_from_payload(payload)
        count = 0
        now = datetime.now(timezone.utc)

        # global_pattern_signals has NO RLS — use session without tenant_id to write
        with self._repository.session() as session:
            for signal_type, raw_value in signals:
                signal_hash = _anonymize(raw_value)
                existing = session.execute(
                    text(
                        "SELECT id, tenant_count, alert_count FROM global_pattern_signals "
                        "WHERE signal_type = :stype AND signal_hash = :shash"
                    ),
                    {"stype": signal_type, "shash": signal_hash},
                ).fetchone()

                if existing:
                    session.execute(
                        text(
                            """
                            UPDATE global_pattern_signals SET
                                tenant_count = tenant_count + 1,
                                alert_count = alert_count + 1,
                                last_seen_at = :now
                            WHERE signal_type = :stype AND signal_hash = :shash
                            """
                        ),
                        {"now": now, "stype": signal_type, "shash": signal_hash},
                    )
                else:
                    session.execute(
                        text(
                            """
                            INSERT INTO global_pattern_signals (
                                id, signal_type, signal_hash, tenant_count, alert_count,
                                first_seen_at, last_seen_at, metadata_json
                            ) VALUES (
                                :id, :stype, :shash, 1, 1, :now, :now, :meta
                            )
                            """
                        ),
                        {
                            "id": uuid.uuid4().hex,
                            "stype": signal_type,
                            "shash": signal_hash,
                            "now": now,
                            "meta": "{}",
                        },
                    )
                count += 1

        logger.info(
            "Ingested global pattern signals",
            extra={"alert_id": alert_id, "signal_count": count},
        )
        return count

    def get_signals_for_alert(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
        min_tenant_count: int = 2,
    ) -> list[dict[str, Any]]:
        """Return global risk signals matching this alert's anonymized fingerprints.

        Only returns signals seen across >= min_tenant_count tenants to avoid
        single-tenant data exposure.
        """
        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=5)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})
        if not payload:
            return []

        signals = self._extract_signals_from_payload(payload)
        if not signals:
            return []

        matches: list[dict[str, Any]] = []

        with self._repository.session() as session:
            for signal_type, raw_value in signals:
                signal_hash = _anonymize(raw_value)
                row = session.execute(
                    text(
                        """
                        SELECT signal_type, tenant_count, alert_count, first_seen_at, last_seen_at
                        FROM global_pattern_signals
                        WHERE signal_type = :stype AND signal_hash = :shash
                          AND tenant_count >= :min_count
                        """
                    ),
                    {"stype": signal_type, "shash": signal_hash, "min_count": min_tenant_count},
                ).fetchone()

                if row:
                    sig_type = str(row[0])
                    tenant_count = int(row[1])
                    alert_count = int(row[2])

                    description_map = {
                        "device_fingerprint": f"Device fingerprint seen in alerts across {tenant_count} institutions",
                        "beneficiary_pattern": f"Beneficiary linked to investigations at {tenant_count} institutions",
                        "typology_indicator": f"Typology/geography pattern observed across {tenant_count} institutions",
                        "risk_velocity": f"Risk velocity pattern shared across {tenant_count} institutions",
                        "beneficiary_pattern": f"Beneficiary account pattern seen at {tenant_count} institutions",
                    }

                    matches.append(
                        {
                            "signal_type": sig_type,
                            "description": description_map.get(
                                sig_type,
                                f"{sig_type} pattern seen at {tenant_count} institutions",
                            ),
                            "tenant_count": tenant_count,
                            "alert_count": alert_count,
                            "first_seen_at": str(row[3]) if row[3] else None,
                            "last_seen_at": str(row[4]) if row[4] else None,
                        }
                    )

        logger.info(
            "Global signals retrieved for alert",
            extra={"alert_id": alert_id, "matches": len(matches)},
        )
        return matches
