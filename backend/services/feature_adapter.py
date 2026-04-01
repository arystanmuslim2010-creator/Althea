from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from schemas.alert_ingestion_schema import AlertPayload


class AlertFeatureAdapter:
    @staticmethod
    def _to_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if value in (None, ""):
            return datetime.now(timezone.utc)
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(parsed):
            return datetime.now(timezone.utc)
        if hasattr(parsed, "to_pydatetime"):
            return parsed.to_pydatetime()
        return datetime.now(timezone.utc)

    def alert_to_feature_dict(self, alert: AlertPayload | dict[str, Any]) -> dict[str, Any]:
        payload = alert.model_dump(mode="json") if isinstance(alert, AlertPayload) else dict(alert)
        transactions = list(payload.get("transactions") or [])
        accounts = list(payload.get("accounts") or [])
        metadata = dict(payload.get("metadata") or {})

        tx_amounts = [float(item.get("amount", 0.0) or 0.0) for item in transactions]
        total_amount = float(sum(tx_amounts)) if tx_amounts else 0.0

        tx_times = [self._to_datetime(item.get("timestamp")) for item in transactions]
        tx_times_sorted = sorted(tx_times)
        if len(tx_times_sorted) >= 2:
            gaps = [
                (tx_times_sorted[idx] - tx_times_sorted[idx - 1]).total_seconds()
                for idx in range(1, len(tx_times_sorted))
            ]
            average_gap = float(sum(gaps) / max(1, len(gaps)))
        else:
            average_gap = 3600.0

        first_account = accounts[0] if accounts else {}
        user_id = str(first_account.get("account_id") or metadata.get("user_id") or "")
        created_at = payload.get("created_at")
        timestamp = tx_times_sorted[0].isoformat() if tx_times_sorted else created_at
        timestamp = timestamp or datetime.now(timezone.utc).isoformat()

        return {
            "alert_id": str(payload.get("alert_id") or ""),
            "user_id": user_id or f"user_{payload.get('alert_id')}",
            "amount": total_amount,
            "timestamp": timestamp,
            "segment": str(first_account.get("segment") or metadata.get("segment") or "retail"),
            "typology": str(payload.get("typology") or metadata.get("typology") or "anomaly"),
            "country": str(first_account.get("country") or metadata.get("country") or "UNKNOWN"),
            "source_system": str(metadata.get("source_system") or "alert_jsonl"),
            "time_gap": average_gap,
            "num_transactions": max(1, len(transactions)),
            "is_sar": payload.get("is_sar"),
            "accounts": accounts,
            "transactions": transactions,
            "metadata": metadata,
            "ingestion_timestamp": payload.get("ingestion_timestamp"),
            "run_id": payload.get("run_id"),
            "raw_payload_json": dict(payload.get("raw_payload_json") or payload),
        }

    def alerts_to_dataframe(self, alerts: list[dict[str, Any]]) -> pd.DataFrame:
        if not alerts:
            return pd.DataFrame()
        rows = [self.alert_to_feature_dict(item) for item in alerts]
        return pd.DataFrame(rows)

