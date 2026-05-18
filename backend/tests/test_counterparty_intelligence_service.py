from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.counterparty_intelligence_service import (
    get_counterparty_intelligence,
    normalize_counterparty_id,
)

pytestmark = pytest.mark.unit


def _ts(days: int = 0, hours: int = 0) -> str:
    base = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
    return (base + timedelta(days=days, hours=hours)).isoformat()


class _Repo:
    def __init__(self, alerts: list[dict], cases: list[dict] | None = None) -> None:
        self.alerts = {row["alert_id"]: dict(row) for row in alerts}
        self.cases = list(cases or [])

    def get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        if tenant_id != "tenant-a":
            return None
        row = self.alerts.get(alert_id)
        if not row:
            return None
        if run_id and row.get("run_id") != run_id:
            return None
        return dict(row)

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000):
        if tenant_id != "tenant-a":
            return []
        return [dict(row) for row in self.alerts.values() if row.get("run_id") == run_id]

    def list_cases(self, tenant_id: str):
        return list(self.cases) if tenant_id == "tenant-a" else []


def test_computes_new_recurring_concentration_fan_out_and_links():
    repo = _Repo(
        alerts=[
            {
                "alert_id": "A1",
                "run_id": "run-1",
                "account_id": "ACCT-A",
                "timestamp": _ts(),
                "risk_score": 88,
                "transactions": [
                    {"transaction_id": "t1", "timestamp": _ts(hours=-1), "amount": 7000, "currency": "USD", "direction": "out", "counterparty_id": "CP-NEW1"},
                    {"transaction_id": "t2", "timestamp": _ts(hours=-1), "amount": 1000, "currency": "USD", "direction": "out", "counterparty_id": "CP-OLD"},
                    {"transaction_id": "t3", "timestamp": _ts(hours=-1), "amount": 500, "currency": "USD", "direction": "out", "counterparty_id": "CP-NEW2"},
                    {"transaction_id": "t4", "timestamp": _ts(hours=-1), "amount": 400, "currency": "USD", "direction": "out", "counterparty_id": "CP-NEW3"},
                    {"transaction_id": "t5", "timestamp": _ts(hours=-1), "amount": 300, "currency": "USD", "direction": "out", "counterparty_id": "CP-NEW4"},
                ],
            },
            {
                "alert_id": "A0",
                "run_id": "run-1",
                "account_id": "ACCT-A",
                "timestamp": _ts(days=-10),
                "transactions": [{"timestamp": _ts(days=-10), "amount": 200, "direction": "out", "counterparty_id": "CP-OLD"}],
            },
            {
                "alert_id": "A2",
                "run_id": "run-1",
                "account_id": "ACCT-B",
                "timestamp": _ts(days=-1),
                "status": "closed",
                "outcome": "escalated",
                "risk_score": 91,
                "transactions": [{"timestamp": _ts(days=-1), "amount": 1200, "direction": "in", "counterparty_id": "CP-NEW1"}],
            },
        ],
        cases=[
            {
                "case_id": "CASE-1",
                "alert_id": "A2",
                "status": "escalated",
                "payload_json": {"outcome": "high_suspicion"},
            }
        ],
    )

    payload = get_counterparty_intelligence(repo, "tenant-a", "A1")
    summary = payload["summary"]

    assert summary["total_counterparties"] == 5
    assert summary["new_counterparties"] == 4
    assert summary["recurring_counterparties"] == 1
    assert summary["counterparty_concentration"] == "high"
    assert summary["fan_out_detected"] is True
    assert summary["shared_counterparty_alerts"] == 2
    assert summary["linked_escalated_cases"] == 1
    assert payload["top_counterparties"][0]["counterparty_id"] == "CP-NEW1"
    assert payload["top_counterparties"][0]["volume_share"] > 0.6
    assert any(signal["type"] == "linked_escalated_case" for signal in payload["signals"])


def test_detects_fan_in():
    repo = _Repo(
        alerts=[
            {
                "alert_id": "A1",
                "run_id": "run-1",
                "account_id": "ACCT-A",
                "timestamp": _ts(),
                "transactions": [
                    {"timestamp": _ts(hours=-1), "amount": 100, "direction": "in", "counterparty_id": f"CP-IN{i}"}
                    for i in range(4)
                ],
            }
        ]
    )

    payload = get_counterparty_intelligence(repo, "tenant-a", "A1")

    assert payload["summary"]["fan_in_detected"] is True
    assert any(signal["type"] == "fan_in" for signal in payload["signals"])


def test_handles_missing_optional_fields_safely():
    repo = _Repo(alerts=[{"alert_id": "A1", "run_id": "run-1", "timestamp": _ts(), "amount": 1000}])

    payload = get_counterparty_intelligence(repo, "tenant-a", "A1")

    assert payload["summary"]["total_counterparties"] == 0
    assert payload["top_counterparties"] == []
    assert payload["data_quality"]["partial"] is True
    assert "counterparty_id" in payload["data_quality"]["missing_fields"]


def test_masks_unsafe_raw_identifiers():
    assert normalize_counterparty_id({"counterparty_id": "Jane Customer jane@example.com"}) != "Jane Customer jane@example.com"
    assert normalize_counterparty_id({"counterparty_account": "1234567890123456"}).startswith("masked:")
    assert normalize_counterparty_id({"counterparty_id": "CP-1"}) == "CP-1"
