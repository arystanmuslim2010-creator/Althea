from __future__ import annotations

from datetime import datetime, timedelta, timezone

from investigation.analyst_workspace_enrichment_service import AnalystWorkspaceEnrichmentService


class _Repo:
    def __init__(self) -> None:
        self.payload = {
            "alert_id": "A1",
            "user_id": "USR00098",
            "customer_name": "",
            "source_account_key": "ACCT-1",
            "amount": 18531.76,
            "currency": "USD",
            "country": "US",
            "segment": "retail",
            "typology": "flow_through",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payment_channel": "wire",
        }

    def get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        return dict(self.payload) if alert_id == "A1" else None

    def list_pipeline_runs(self, tenant_id: str, limit: int = 20):
        return [{"run_id": "run-1"}]

    def get_user_by_id(self, tenant_id: str, user_id: str):
        if user_id == "analyst-1":
            return {"id": "analyst-1", "email": "ops.analyst@althea.local"}
        return None


class _EnrichmentRepo:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.snapshot = {
            "alert_payload": {},
            "entity_ids": ["USR00098"],
            "account_events": [
                {
                    "entity_id": "USR00098",
                    "event_time": (now - timedelta(days=3)).isoformat(),
                    "amount": 2000.0,
                    "direction": "out",
                    "country": "US",
                    "currency": "USD",
                    "channel": "wire",
                    "payment_type": "wire",
                    "counterparty_id": "CP-1",
                    "counterparty_bank_id": "BANK-1",
                },
                {
                    "entity_id": "USR00098",
                    "event_time": (now - timedelta(days=2)).isoformat(),
                    "amount": 2400.0,
                    "direction": "out",
                    "country": "GB",
                    "currency": "USD",
                    "channel": "wire",
                    "payment_type": "wire",
                    "counterparty_id": "CP-1",
                    "counterparty_bank_id": "BANK-1",
                },
            ],
            "alert_outcomes": [
                {"alert_id": "A0", "entity_id": "USR00098", "event_time": (now - timedelta(days=10)).isoformat()},
                {"alert_id": "A-9", "entity_id": "USR00098", "event_time": (now - timedelta(days=40)).isoformat()},
            ],
            "case_actions": [
                {"case_id": "CASE-1", "alert_id": "A0", "entity_id": "USR00098", "event_time": (now - timedelta(days=8)).isoformat()},
                {"case_id": "CASE-2", "alert_id": "A-9", "entity_id": "USR00098", "event_time": (now - timedelta(days=70)).isoformat()},
            ],
        }

    def get_enrichment_context_snapshot(self, tenant_id: str, alert_id: str, as_of_timestamp=None):
        return dict(self.snapshot)

    def list_master_customers(self, tenant_id: str):
        return [
            {
                "customer_id": "USR00098",
                "external_customer_id": "Customer Retail 98",
                "risk_tier": "high",
                "segment": "retail",
                "country": "US",
                "pep_flag": True,
                "sanctions_flag": False,
                "kyc_status": "current",
                "effective_from": "2025-01-01T00:00:00+00:00",
            }
        ]

    def list_master_accounts(self, tenant_id: str):
        return [
            {
                "account_id": "ACCT-1",
                "external_account_id": "Retail Checking 1",
                "account_type": "checking",
                "status": "active",
                "opened_at": "2024-01-01T00:00:00+00:00",
            }
        ]

    def list_master_counterparties(self, tenant_id: str):
        return [
            {
                "counterparty_id": "CP-1",
                "external_counterparty_id": "Counterparty Alpha",
                "effective_from": "2025-02-01T00:00:00+00:00",
                "payload_json": {
                    "watchlist_hits": [],
                    "adverse_media_hits": [],
                },
            }
        ]

    def list_latest_source_health(self, tenant_id: str):
        return [
            {"source_name": "kyc", "status": "healthy"},
            {"source_name": "watchlist", "status": "healthy"},
        ]


class _EmptyEnrichmentRepo:
    def get_enrichment_context_snapshot(self, tenant_id: str, alert_id: str, as_of_timestamp=None):
        return {"alert_payload": {}, "entity_ids": [], "account_events": [], "alert_outcomes": [], "case_actions": []}

    def list_master_customers(self, tenant_id: str):
        return []

    def list_master_accounts(self, tenant_id: str):
        return []

    def list_master_counterparties(self, tenant_id: str):
        return []

    def list_latest_source_health(self, tenant_id: str):
        return []


def test_analyst_workspace_enrichment_service_uses_canonical_and_master_data() -> None:
    service = AnalystWorkspaceEnrichmentService(_Repo(), _EnrichmentRepo())

    payload = service.generate_sections(
        tenant_id="tenant-a",
        alert_id="A1",
        case_status={"assigned_to": "analyst-1"},
        network_graph={"nodes": [{"id": "cp-1", "type": "counterparty", "label": "Counterparty Alpha"}]},
    )

    assert payload["customer_profile"]["customer_label"] == "Customer Retail 98"
    assert payload["customer_profile"]["assigned_analyst_label"] == "ops.analyst@althea.local"
    assert payload["account_profile"]["account_type"] == "checking"
    assert payload["behavior_baseline"]["baseline_avg_amount"] == 2200.0
    assert payload["behavior_baseline"]["prior_alert_count_30d"] == 1
    assert payload["counterparty_summary"]["counterparty_bank_count"] == 1
    assert payload["screening_summary"]["screening_status"] == "hits_found"
    assert payload["screening_summary"]["pep_hits"] == ["Customer PEP flag present"]
    assert payload["data_availability"]["coverage_status"] in {"partial", "enriched"}


def test_analyst_workspace_enrichment_service_keeps_missing_screening_unavailable() -> None:
    service = AnalystWorkspaceEnrichmentService(_Repo(), _EmptyEnrichmentRepo())

    payload = service.generate_sections(tenant_id="tenant-a", alert_id="A1")

    assert payload["screening_summary"]["screening_status"] == "unavailable"
    assert "screening_summary" in payload["data_availability"]["missing_sections"]
    assert payload["customer_profile"]["customer_label"] == "USR00098"
    assert payload["behavior_baseline"]["deviation_summary"]
