"""Counterparty context signals for alert investigations.

This module only uses bank-provided alert, transaction, outcome, and case data.
It does not perform external screening, scraping, or filing decisions.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


RECENT_WINDOW_HOURS = 24
TOP_COUNTERPARTY_LIMIT = 10
FAN_IN_COUNTERPARTY_THRESHOLD = 4
FAN_OUT_COUNTERPARTY_THRESHOLD = 4
HIGH_CONCENTRATION_TOP_SHARE = 0.60
HIGH_CONCENTRATION_TOP3_SHARE = 0.80
MEDIUM_CONCENTRATION_TOP_SHARE = 0.35
MEDIUM_CONCENTRATION_TOP3_SHARE = 0.65
ESCALATED_OUTCOME_TOKENS = {
    "escalated",
    "str",
    "sar",
    "high_suspicion",
    "high suspicion",
    "suspicious",
    "true_positive",
    "confirmed_suspicious",
}

_HASH_LIKE_RE = re.compile(r"^(hash[:_-]|[a-f0-9]{16,}|[a-z]+_hash[:_-]?[a-z0-9]{8,})", re.IGNORECASE)
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{2,64}$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"unknown", "n/a", "na", "none", "null", "undefined", "-", "{}", "[]"}:
        return None
    return text


def _mask_identifier(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"masked:{digest}"


def _safe_identifier(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if text.isdigit() and len(text) > 6:
        return _mask_identifier(text)
    if _HASH_LIKE_RE.match(text):
        return text[:96]
    if len(text) > 64 or re.search(r"\s|@|\+|\(|\)", text):
        return _mask_identifier(text)
    if _SAFE_ID_RE.match(text):
        return text
    return _mask_identifier(text)


def normalize_counterparty_id(tx: dict, alert_account_id: str | None = None) -> str | None:
    """Derive a safe counterparty identifier from available transaction fields."""
    if not isinstance(tx, dict):
        return None
    for key in (
        "counterparty_id",
        "counterparty_id_hash",
        "counterparty_account",
        "counterparty_account_id",
        "counterparty_account_hash",
        "beneficiary_id",
        "beneficiary_account",
        "beneficiary_account_hash",
        "counterparty",
    ):
        value = _safe_identifier(tx.get(key))
        if value:
            return value

    alert_account = _clean_text(alert_account_id)
    from_account = _clean_text(tx.get("from_account") or tx.get("from_account_hash") or tx.get("sender"))
    to_account = _clean_text(tx.get("to_account") or tx.get("to_account_hash") or tx.get("receiver"))
    if alert_account and from_account and to_account:
        if from_account == alert_account:
            return _safe_identifier(to_account)
        if to_account == alert_account:
            return _safe_identifier(from_account)
    return None


def _direction(tx: dict, alert_account_id: str | None = None) -> str:
    raw = str(tx.get("direction") or "").strip().lower()
    if raw in {"in", "inbound", "incoming", "credit", "received", "receive"}:
        return "inbound"
    if raw in {"out", "outbound", "outgoing", "debit", "sent", "send"}:
        return "outbound"
    alert_account = _clean_text(alert_account_id)
    from_account = _clean_text(tx.get("from_account") or tx.get("from_account_hash") or tx.get("sender"))
    to_account = _clean_text(tx.get("to_account") or tx.get("to_account_hash") or tx.get("receiver"))
    if alert_account and from_account == alert_account:
        return "outbound"
    if alert_account and to_account == alert_account:
        return "inbound"
    return "unknown"


def _amount(tx: dict) -> float:
    try:
        value = float(tx.get("amount") or tx.get("transaction_amount") or 0.0)
    except Exception:
        return 0.0
    return abs(value)


def _to_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _to_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _resolve_alert_account_id(payload: dict[str, Any]) -> str | None:
    for key in (
        "account_id",
        "source_account_id",
        "source_account_key",
        "account_hash",
        "customer_account_id",
    ):
        value = _clean_text(payload.get(key))
        if value:
            return value
    for account in _to_list(payload.get("accounts")):
        account_obj = _to_dict(account)
        value = _clean_text(account_obj.get("account_id") or account_obj.get("account_hash"))
        if value:
            return value
    return None


def _resolve_entity_values(payload: dict[str, Any]) -> set[str]:
    values = {
        _clean_text(payload.get("entity_id")),
        _clean_text(payload.get("customer_id")),
        _clean_text(payload.get("user_id")),
        _clean_text(payload.get("account_id")),
        _clean_text(payload.get("source_account_key")),
    }
    for account in _to_list(payload.get("accounts")):
        account_obj = _to_dict(account)
        values.add(_clean_text(account_obj.get("customer_id")))
        values.add(_clean_text(account_obj.get("account_id")))
    return {item for item in values if item}


def _tx_timestamp(tx: dict, fallback: datetime | None = None) -> datetime | None:
    return (
        _parse_dt(tx.get("timestamp"))
        or _parse_dt(tx.get("event_time"))
        or _parse_dt(tx.get("transaction_timestamp"))
        or _parse_dt(tx.get("created_at"))
        or fallback
    )


def _payload_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    txs = [_to_dict(item) for item in _to_list(payload.get("transactions")) if isinstance(item, dict)]
    if txs:
        return txs
    if any(payload.get(key) is not None for key in ("amount", "counterparty_id", "counterparty_account", "beneficiary_id")):
        return [
            {
                "transaction_id": payload.get("transaction_id") or payload.get("alert_id"),
                "timestamp": payload.get("timestamp") or payload.get("alert_timestamp") or payload.get("created_at"),
                "amount": payload.get("amount"),
                "currency": payload.get("currency"),
                "counterparty_id": payload.get("counterparty_id") or payload.get("counterparty_id_hash"),
                "counterparty_account": payload.get("counterparty_account"),
                "beneficiary_id": payload.get("beneficiary_id"),
                "beneficiary_account": payload.get("beneficiary_account"),
                "from_account": payload.get("from_account"),
                "to_account": payload.get("to_account"),
                "direction": payload.get("direction"),
            }
        ]
    return []


def _normalize_tx(
    tx: dict,
    *,
    payload: dict[str, Any],
    alert_account_id: str | None,
    fallback_timestamp: datetime | None,
) -> dict[str, Any] | None:
    cp_id = normalize_counterparty_id(tx, alert_account_id=alert_account_id)
    if not cp_id:
        return None
    timestamp = _tx_timestamp(tx, fallback_timestamp)
    return {
        "alert_id": str(payload.get("alert_id") or ""),
        "alert_timestamp": _parse_dt(payload.get("alert_timestamp") or payload.get("timestamp") or payload.get("created_at")),
        "status": payload.get("status") or payload.get("case_status") or payload.get("governance_status"),
        "outcome": payload.get("outcome") or payload.get("analyst_decision") or payload.get("decision"),
        "priority_score": float(payload.get("priority_score") or payload.get("risk_score") or 0.0),
        "entity_values": _resolve_entity_values(payload),
        "counterparty_id": cp_id,
        "direction": _direction(tx, alert_account_id=alert_account_id),
        "amount": _amount(tx),
        "currency": _clean_text(tx.get("currency")),
        "timestamp": timestamp,
        "transaction_id": _clean_text(tx.get("transaction_id") or tx.get("id")),
    }


def _case_alert_ids(case: dict[str, Any]) -> set[str]:
    ids = {_clean_text(case.get("alert_id"))}
    payload = _to_dict(case.get("payload_json"))
    for key in ("alert_ids", "alerts"):
        for item in _to_list(payload.get(key)):
            if isinstance(item, dict):
                ids.add(_clean_text(item.get("alert_id") or item.get("id")))
            else:
                ids.add(_clean_text(item))
    return {item for item in ids if item}


def _is_escalated_like(*values: Any) -> bool:
    text = " ".join(str(value or "").lower() for value in values)
    return any(token in text for token in ESCALATED_OUTCOME_TOKENS)


def _fetch_alerts(repository: Any, tenant_id: str, alert_payload: dict[str, Any]) -> list[dict[str, Any]]:
    run_id = _clean_text(alert_payload.get("run_id"))
    if run_id and hasattr(repository, "list_alert_payloads_by_run"):
        try:
            rows = repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=200000)
            if rows:
                return [dict(row) for row in rows if isinstance(row, dict)]
        except Exception:
            pass
    return [alert_payload]


def _fetch_enrichment_events(enrichment_repository: Any, tenant_id: str, alert_id: str, alert_payload: dict[str, Any]) -> list[dict[str, Any]]:
    if enrichment_repository is None or not hasattr(enrichment_repository, "get_enrichment_context_snapshot"):
        return []
    try:
        snapshot = enrichment_repository.get_enrichment_context_snapshot(
            tenant_id=tenant_id,
            alert_id=alert_id,
            as_of_timestamp=alert_payload.get("alert_timestamp") or alert_payload.get("timestamp") or alert_payload.get("created_at"),
        )
    except Exception:
        return []
    return [_to_dict(item) for item in _to_list(_to_dict(snapshot).get("account_events")) if isinstance(item, dict)]


def _shared_window_match(timestamp: datetime | None, alert_ts: datetime, linked_alert_window_days: int) -> bool:
    if timestamp is None:
        return True
    return abs((alert_ts - timestamp).total_seconds()) <= linked_alert_window_days * 86400


class CounterpartyIntelligenceService:
    def __init__(self, repository: Any, enrichment_repository: Any | None = None) -> None:
        self.repository = repository
        self.enrichment_repository = enrichment_repository

    def get_counterparty_intelligence(
        self,
        tenant_id: str,
        alert_id: str,
        lookback_days: int = 90,
        linked_alert_window_days: int = 30,
    ) -> dict[str, Any]:
        return get_counterparty_intelligence(
            self.repository,
            tenant_id=tenant_id,
            alert_id=alert_id,
            lookback_days=lookback_days,
            linked_alert_window_days=linked_alert_window_days,
            enrichment_repository=self.enrichment_repository,
        )


def get_counterparty_intelligence(
    repository: Any,
    tenant_id: str,
    alert_id: str,
    lookback_days: int = 90,
    linked_alert_window_days: int = 30,
    enrichment_repository: Any | None = None,
) -> dict[str, Any]:
    alert_payload = repository.get_alert_payload(tenant_id=tenant_id, alert_id=str(alert_id), run_id=None)
    if not alert_payload:
        raise ValueError("Alert not found")
    alert_payload = dict(alert_payload)
    alert_ts = (
        _parse_dt(alert_payload.get("alert_timestamp"))
        or _parse_dt(alert_payload.get("timestamp"))
        or _parse_dt(alert_payload.get("created_at"))
        or _utcnow()
    )
    recent_start = alert_ts - timedelta(hours=RECENT_WINDOW_HOURS)
    history_start = recent_start - timedelta(days=max(1, int(lookback_days or 90)))
    alert_account_id = _resolve_alert_account_id(alert_payload)
    alert_entity_values = _resolve_entity_values(alert_payload)

    missing_fields: set[str] = set()
    warnings: list[str] = []
    all_payloads = _fetch_alerts(repository, tenant_id, alert_payload)
    all_txs: list[dict[str, Any]] = []
    current_txs: list[dict[str, Any]] = []

    for payload in all_payloads:
        payload = dict(payload or {})
        payload_ts = _parse_dt(payload.get("alert_timestamp") or payload.get("timestamp") or payload.get("created_at"))
        account_id = _resolve_alert_account_id(payload) or alert_account_id
        payload_txs = _payload_transactions(payload)
        if not payload_txs:
            missing_fields.add("transactions")
        for tx in payload_txs:
            normalized = _normalize_tx(
                tx,
                payload=payload,
                alert_account_id=account_id,
                fallback_timestamp=payload_ts,
            )
            if not normalized:
                missing_fields.add("counterparty_id")
                continue
            all_txs.append(normalized)
            if str(payload.get("alert_id") or "") == str(alert_id):
                current_txs.append(normalized)

    enrichment_events = _fetch_enrichment_events(enrichment_repository, tenant_id, str(alert_id), alert_payload)
    for event in enrichment_events:
        pseudo_payload = {**alert_payload, "alert_id": "", "timestamp": event.get("event_time")}
        normalized = _normalize_tx(
            event,
            payload=pseudo_payload,
            alert_account_id=alert_account_id,
            fallback_timestamp=_parse_dt(event.get("event_time")),
        )
        if normalized:
            normalized["alert_id"] = ""
            all_txs.append(normalized)

    if not current_txs:
        current_txs = [
            tx for tx in all_txs
            if tx.get("timestamp") is None or recent_start <= tx["timestamp"] <= alert_ts
        ]

    if alert_entity_values:
        relevant_history = [
            tx for tx in all_txs
            if not tx.get("entity_values") or tx["entity_values"] & alert_entity_values
        ]
    else:
        relevant_history = list(all_txs)
        missing_fields.add("account_or_customer_id")

    recent_txs = [
        tx for tx in current_txs
        if tx.get("timestamp") is None or recent_start <= tx["timestamp"] <= alert_ts
    ] or current_txs
    historical_txs = [
        tx for tx in relevant_history
        if tx.get("timestamp") is not None and history_start <= tx["timestamp"] < recent_start
    ]
    historical_counterparties = {tx["counterparty_id"] for tx in historical_txs}

    current_counterparties = {tx["counterparty_id"] for tx in recent_txs}
    total_counterparties = len(current_counterparties)
    new_counterparties = {cp for cp in current_counterparties if cp not in historical_counterparties}
    recurring_counterparties = current_counterparties - new_counterparties

    grouped: dict[str, dict[str, Any]] = {}
    for tx in recent_txs:
        cp = tx["counterparty_id"]
        item = grouped.setdefault(
            cp,
            {
                "counterparty_id": cp,
                "directions": set(),
                "transaction_count": 0,
                "total_amount": 0.0,
                "currencies": set(),
                "first_seen_at": None,
                "last_seen_at": None,
                "linked_alert_ids": set(),
                "linked_escalated_alert_ids": set(),
            },
        )
        item["directions"].add(tx.get("direction") or "unknown")
        item["transaction_count"] += 1
        item["total_amount"] += float(tx.get("amount") or 0.0)
        if tx.get("currency"):
            item["currencies"].add(tx["currency"])
        timestamp = tx.get("timestamp")
        if timestamp:
            item["first_seen_at"] = timestamp if item["first_seen_at"] is None else min(item["first_seen_at"], timestamp)
            item["last_seen_at"] = timestamp if item["last_seen_at"] is None else max(item["last_seen_at"], timestamp)

    linked_by_alert: dict[str, dict[str, Any]] = {}
    for tx in all_txs:
        linked_alert_id = _clean_text(tx.get("alert_id"))
        cp = tx.get("counterparty_id")
        if not linked_alert_id or linked_alert_id == str(alert_id) or cp not in current_counterparties:
            continue
        timestamp = tx.get("alert_timestamp") or tx.get("timestamp")
        if not _shared_window_match(timestamp, alert_ts, linked_alert_window_days):
            continue
        item = linked_by_alert.setdefault(
            linked_alert_id,
            {
                "alert_id": linked_alert_id,
                "shared_counterparty_id": cp,
                "alert_timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else None,
                "status": tx.get("status"),
                "outcome": tx.get("outcome"),
                "priority_score": float(tx.get("priority_score") or 0.0),
            },
        )
        item["shared_counterparty_id"] = item.get("shared_counterparty_id") or cp
        if cp in grouped:
            grouped[cp]["linked_alert_ids"].add(linked_alert_id)
            if _is_escalated_like(tx.get("status"), tx.get("outcome")):
                grouped[cp]["linked_escalated_alert_ids"].add(linked_alert_id)

    linked_cases = []
    if hasattr(repository, "list_cases"):
        try:
            linked_cases = [dict(case) for case in repository.list_cases(tenant_id) if isinstance(case, dict)]
        except Exception:
            linked_cases = []
            warnings.append("Case history was unavailable for linked-case checks.")

    linked_escalated_case_ids: set[str] = set()
    for case in linked_cases:
        ids = _case_alert_ids(case)
        if not ids or not (ids & set(linked_by_alert.keys()) or str(alert_id) in ids):
            continue
        payload = _to_dict(case.get("payload_json"))
        if _is_escalated_like(case.get("status"), case.get("outcome"), payload.get("outcome"), payload.get("decision")):
            case_id = _clean_text(case.get("case_id") or case.get("id"))
            if case_id:
                linked_escalated_case_ids.add(case_id)

    total_volume = sum(float(item["total_amount"]) for item in grouped.values())
    sorted_groups = sorted(grouped.values(), key=lambda item: (item["total_amount"], item["transaction_count"]), reverse=True)
    top_share = (sorted_groups[0]["total_amount"] / total_volume) if total_volume > 0 and sorted_groups else 0.0
    top3_share = (sum(item["total_amount"] for item in sorted_groups[:3]) / total_volume) if total_volume > 0 else 0.0
    if top_share > HIGH_CONCENTRATION_TOP_SHARE or top3_share > HIGH_CONCENTRATION_TOP3_SHARE:
        concentration = "high"
    elif top_share > MEDIUM_CONCENTRATION_TOP_SHARE or top3_share > MEDIUM_CONCENTRATION_TOP3_SHARE:
        concentration = "medium"
    else:
        concentration = "low"

    inbound_counterparties = {tx["counterparty_id"] for tx in recent_txs if tx.get("direction") == "inbound"}
    outbound_counterparties = {tx["counterparty_id"] for tx in recent_txs if tx.get("direction") == "outbound"}
    fan_in_detected = len(inbound_counterparties) >= FAN_IN_COUNTERPARTY_THRESHOLD
    fan_out_detected = len(outbound_counterparties) >= FAN_OUT_COUNTERPARTY_THRESHOLD

    top_counterparties = []
    for item in sorted_groups[:TOP_COUNTERPARTY_LIMIT]:
        directions = item["directions"] or {"unknown"}
        direction = next(iter(directions)) if len(directions) == 1 else "mixed"
        linked_alert_ids = item["linked_alert_ids"]
        escalated_count = len(item["linked_escalated_alert_ids"]) + len(linked_escalated_case_ids)
        top_counterparties.append(
            {
                "counterparty_id": item["counterparty_id"],
                "direction": direction,
                "transaction_count": int(item["transaction_count"]),
                "total_amount": round(float(item["total_amount"]), 2),
                "currency_count": len(item["currencies"]),
                "first_seen_at": item["first_seen_at"].isoformat() if item["first_seen_at"] else None,
                "last_seen_at": item["last_seen_at"].isoformat() if item["last_seen_at"] else None,
                "is_new": item["counterparty_id"] in new_counterparties,
                "linked_alert_count": len(linked_alert_ids),
                "linked_escalated_case_count": escalated_count,
                "volume_share": round((item["total_amount"] / total_volume) if total_volume > 0 else 0.0, 4),
            }
        )

    summary = {
        "total_counterparties": total_counterparties,
        "new_counterparties": len(new_counterparties),
        "recurring_counterparties": len(recurring_counterparties),
        "new_counterparty_share": round(len(new_counterparties) / total_counterparties, 4) if total_counterparties else 0.0,
        "recurring_counterparty_share": round(len(recurring_counterparties) / total_counterparties, 4) if total_counterparties else 0.0,
        "top_counterparty_volume_share": round(top_share, 4),
        "top_3_counterparty_volume_share": round(top3_share, 4),
        "counterparty_concentration": concentration,
        "inbound_counterparty_count": len(inbound_counterparties),
        "outbound_counterparty_count": len(outbound_counterparties),
        "shared_counterparty_alerts": len(linked_by_alert),
        "linked_escalated_cases": len(linked_escalated_case_ids),
        "fan_in_detected": fan_in_detected,
        "fan_out_detected": fan_out_detected,
    }

    signals = _build_signals(summary, top_counterparties, len(linked_by_alert))
    analyst_takeaway = _build_takeaway(summary, signals)
    if missing_fields:
        warnings.append("Some fields are unavailable because the pilot dataset does not include full counterparty metadata.")

    return {
        "alert_id": str(alert_id),
        "tenant_id": str(tenant_id),
        "lookback_days": int(lookback_days),
        "linked_alert_window_days": int(linked_alert_window_days),
        "summary": summary,
        "top_counterparties": top_counterparties,
        "linked_alerts": sorted(linked_by_alert.values(), key=lambda item: item["alert_id"])[:50],
        "signals": signals,
        "analyst_takeaway": analyst_takeaway,
        "data_quality": {
            "partial": bool(missing_fields or warnings),
            "missing_fields": sorted(missing_fields),
            "warnings": warnings,
        },
        "graph_preview": _build_graph_preview(alert_account_id, top_counterparties),
    }


def _build_signals(summary: dict[str, Any], top_counterparties: list[dict[str, Any]], linked_alert_count: int) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []
    total = int(summary.get("total_counterparties") or 0)
    new_count = int(summary.get("new_counterparties") or 0)
    if total and summary.get("new_counterparty_share", 0) >= 0.5:
        signals.append(
            {
                "type": "new_counterparty_burst",
                "severity": "high" if summary.get("new_counterparty_share", 0) >= 0.75 else "medium",
                "label": "New counterparty burst",
                "explanation": f"{new_count} of {total} counterparties are new in the recent activity window and require analyst review.",
            }
        )
    if summary.get("counterparty_concentration") in {"medium", "high"}:
        share = int(round(float(summary.get("top_counterparty_volume_share") or 0.0) * 100))
        signals.append(
            {
                "type": "counterparty_concentration",
                "severity": "high" if summary.get("counterparty_concentration") == "high" else "medium",
                "label": "Counterparty concentration",
                "explanation": f"{share}% of recent volume is concentrated in the top counterparty, a linked-pattern signal for investigation context.",
            }
        )
    if linked_alert_count:
        signals.append(
            {
                "type": "shared_counterparty_alerts",
                "severity": "medium" if linked_alert_count < 4 else "high",
                "label": "Shared counterparties in similar alerts",
                "explanation": f"Shared counterparties appear in {linked_alert_count} other alerts within the linked-alert window.",
            }
        )
    if int(summary.get("linked_escalated_cases") or 0) > 0:
        signals.append(
            {
                "type": "linked_escalated_case",
                "severity": "high",
                "label": "Linked escalated case",
                "explanation": "At least one shared counterparty is linked to a previously escalated or high-suspicion case outcome.",
            }
        )
    if summary.get("fan_out_detected"):
        signals.append(
            {
                "type": "fan_out",
                "severity": "medium",
                "label": "Outgoing fan-out pattern",
                "explanation": "Outgoing fan-out pattern detected: funds moved to multiple counterparties in a short window and may indicate a review priority.",
            }
        )
    if summary.get("fan_in_detected"):
        signals.append(
            {
                "type": "fan_in",
                "severity": "medium",
                "label": "Incoming fan-in pattern",
                "explanation": "Incoming fan-in pattern detected: funds arrived from multiple counterparties in a short window and requires analyst review.",
            }
        )
    if not signals and top_counterparties:
        signals.append(
            {
                "type": "counterparty_context_available",
                "severity": "low",
                "label": "Counterparty context available",
                "explanation": "Counterparty context is available for analyst review; no elevated linked-pattern signal was generated.",
            }
        )
    return signals


def _build_takeaway(summary: dict[str, Any], signals: list[dict[str, str]]) -> str:
    if not int(summary.get("total_counterparties") or 0):
        return "No counterparty intelligence is available from the current pilot data for this alert."
    high = [item for item in signals if item.get("severity") == "high"]
    if high:
        return "Counterparty context includes high-severity linked-pattern signals; prioritize human review before any workflow decision."
    if signals:
        return "Counterparty context shows patterns that may affect investigation priority and should be reviewed by an analyst."
    return "Counterparty context is available as investigation support; no elevated linked-pattern signal was identified."


def _build_graph_preview(alert_account_id: str | None, top_counterparties: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    account_node = f"account:{_safe_identifier(alert_account_id) or 'alerted-account'}"
    nodes = [{"id": account_node, "type": "account", "label": "Alerted account"}]
    edges = []
    for item in top_counterparties[:5]:
        cp_node = f"counterparty:{item['counterparty_id']}"
        nodes.append({"id": cp_node, "type": "counterparty", "label": "Counterparty"})
        edges.append(
            {
                "source": account_node,
                "target": cp_node,
                "amount": item.get("total_amount", 0.0),
                "tx_count": item.get("transaction_count", 0),
            }
        )
    return {"nodes": nodes, "edges": edges}
