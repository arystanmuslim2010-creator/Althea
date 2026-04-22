"""Analyst workspace enrichment assembly for pilot-ready alert investigations."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from statistics import mean
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AnalystWorkspaceEnrichmentService:
    """Build analyst-facing enrichment blocks for the unified investigation screen."""

    _SCREENING_SOURCE_NAMES = {"kyc", "watchlist"}

    def __init__(self, repository, enrichment_repository) -> None:
        self._repository = repository
        self._enrichment_repository = enrichment_repository

    @staticmethod
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

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "Yes" if value else "No"
        text = str(value).strip()
        if not text:
            return None
        lowered = text.lower()
        if lowered in {"unknown", "n/a", "na", "none", "null", "undefined", "{}", "[]", "-"}:
            return None
        if lowered.startswith("unknown "):
            return None
        return text

    @staticmethod
    def _to_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, bool):
            return True
        if isinstance(value, list):
            return any(AnalystWorkspaceEnrichmentService._has_value(item) for item in value)
        if isinstance(value, dict):
            return any(AnalystWorkspaceEnrichmentService._has_value(item) for item in value.values())
        return False

    def _get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None) -> dict[str, Any]:
        payload = self._repository.get_alert_payload(tenant_id=tenant_id, alert_id=alert_id, run_id=run_id)
        if payload:
            return dict(payload)
        recent_runs = list(self._repository.list_pipeline_runs(tenant_id, limit=20) or [])
        for row in recent_runs:
            candidate = str(row.get("run_id") or "").strip()
            if not candidate:
                continue
            payload = self._repository.get_alert_payload(tenant_id=tenant_id, alert_id=alert_id, run_id=candidate)
            if payload:
                return dict(payload)
        raise ValueError(f"Alert {alert_id} not found")

    @staticmethod
    def _resolve_entity_id(payload: dict[str, Any]) -> str | None:
        for key in ("customer_id", "user_id", "customer_key", "entity_id", "source_account_key", "account_id"):
            value = AnalystWorkspaceEnrichmentService._clean_text(payload.get(key))
            if value:
                return value
        for account in AnalystWorkspaceEnrichmentService._to_list(payload.get("accounts")):
            account_obj = AnalystWorkspaceEnrichmentService._to_dict(account)
            for key in ("customer_id", "customer_key", "account_id"):
                value = AnalystWorkspaceEnrichmentService._clean_text(account_obj.get(key))
                if value:
                    return value
        return None

    @staticmethod
    def _resolve_account_id(payload: dict[str, Any]) -> str | None:
        for key in ("account_id", "source_account_key", "source_account_id"):
            value = AnalystWorkspaceEnrichmentService._clean_text(payload.get(key))
            if value:
                return value
        for account in AnalystWorkspaceEnrichmentService._to_list(payload.get("accounts")):
            account_obj = AnalystWorkspaceEnrichmentService._to_dict(account)
            value = AnalystWorkspaceEnrichmentService._clean_text(account_obj.get("account_id"))
            if value:
                return value
        return None

    @staticmethod
    def _payload_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            AnalystWorkspaceEnrichmentService._to_dict(item)
            for item in AnalystWorkspaceEnrichmentService._to_list(payload.get("transactions"))
            if isinstance(item, dict)
        ]

    def _match_master_customer(self, customers: list[dict[str, Any]], payload: dict[str, Any], entity_id: str | None) -> dict[str, Any]:
        customer_id = self._clean_text(payload.get("customer_id")) or entity_id
        for row in customers:
            if customer_id and str(row.get("customer_id") or "").strip() == customer_id:
                return row
            external = str(row.get("external_customer_id") or "").strip()
            if external and external in {str(payload.get("user_id") or "").strip(), str(payload.get("customer_key") or "").strip()}:
                return row
        return {}

    def _match_master_account(self, accounts: list[dict[str, Any]], payload: dict[str, Any], account_id: str | None) -> dict[str, Any]:
        for row in accounts:
            if account_id and str(row.get("account_id") or "").strip() == account_id:
                return row
            external = str(row.get("external_account_id") or "").strip()
            if external and external in {
                str(payload.get("source_account_key") or "").strip(),
                str(payload.get("account_id") or "").strip(),
            }:
                return row
        return {}

    def _resolve_user_label(self, tenant_id: str, user_id: str | None) -> str | None:
        if not user_id:
            return None
        try:
            user = self._repository.get_user_by_id(tenant_id, user_id)
        except Exception:
            user = None
        if not user:
            return None
        return self._clean_text(user.get("email")) or self._clean_text(user.get("id"))

    def _customer_profile(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        customer_record: dict[str, Any],
        case_status: dict[str, Any] | None,
    ) -> dict[str, Any]:
        customer_label = (
            self._clean_text(payload.get("customer_name"))
            or self._clean_text(payload.get("customer_label"))
            or self._clean_text(customer_record.get("external_customer_id"))
            or self._resolve_user_label(tenant_id, self._clean_text(payload.get("user_id")))
            or self._clean_text(payload.get("user_id"))
            or self._clean_text(customer_record.get("customer_id"))
        )
        assigned_to = self._clean_text((case_status or {}).get("assigned_to"))
        assigned_label = self._resolve_user_label(tenant_id, assigned_to) if assigned_to else None
        return {
            "customer_label": customer_label,
            "segment": self._clean_text(customer_record.get("segment")) or self._clean_text(payload.get("segment")),
            "risk_tier": self._clean_text(customer_record.get("risk_tier")) or self._clean_text(payload.get("risk_tier")) or self._clean_text(payload.get("risk_band")),
            "country": self._clean_text(customer_record.get("country")) or self._clean_text(payload.get("country")),
            "business_purpose": self._clean_text(payload.get("business_purpose")) or self._clean_text(payload.get("declared_business_purpose")) or self._clean_text(payload.get("expected_activity")),
            "kyc_status": self._clean_text(customer_record.get("kyc_status")),
            "pep_flag": customer_record.get("pep_flag") if customer_record else None,
            "sanctions_flag": customer_record.get("sanctions_flag") if customer_record else None,
            "onboarded_at": customer_record.get("effective_from") or payload.get("customer_since") or payload.get("opened_at"),
            "assigned_analyst_label": assigned_label,
        }

    def _account_profile(self, *, payload: dict[str, Any], account_record: dict[str, Any], as_of: datetime | None) -> dict[str, Any]:
        opened_at = account_record.get("opened_at") or payload.get("opened_at") or account_record.get("effective_from")
        opened_dt = self._parse_dt(opened_at)
        account_age_days = None
        if opened_dt and as_of:
            account_age_days = max(0, int((as_of - opened_dt).total_seconds() // 86400))
        account_label = (
            self._clean_text(payload.get("account_label"))
            or self._clean_text(payload.get("source_account_key"))
            or self._clean_text(payload.get("account_id"))
            or self._clean_text(account_record.get("external_account_id"))
            or self._clean_text(account_record.get("account_id"))
        )
        return {
            "account_label": account_label,
            "account_type": self._clean_text(account_record.get("account_type")) or self._clean_text(payload.get("account_type")),
            "account_status": self._clean_text(account_record.get("status")) or self._clean_text(payload.get("account_status")),
            "opened_at": opened_at,
            "account_age_days": account_age_days,
        }

    def _behavior_baseline(
        self,
        *,
        payload: dict[str, Any],
        account_events: list[dict[str, Any]],
        outcomes: list[dict[str, Any]],
        case_actions: list[dict[str, Any]],
        as_of: datetime | None,
    ) -> dict[str, Any]:
        transactions = self._payload_transactions(payload)
        payload_amount = payload.get("amount")
        try:
            payload_amount_value = float(payload_amount) if payload_amount is not None else None
        except Exception:
            payload_amount_value = None

        current_window_tx_count = len(transactions) or int(payload.get("transaction_count") or (1 if payload_amount_value is not None else 0))
        inbound_amount = 0.0
        outbound_amount = 0.0
        for item in transactions:
            amount = item.get("amount")
            try:
                numeric = float(amount)
            except Exception:
                continue
            direction = str(item.get("direction") or "").strip().lower()
            if direction == "in":
                inbound_amount += numeric
            else:
                outbound_amount += numeric
        if not transactions and payload_amount_value is not None:
            outbound_amount = max(payload_amount_value, 0.0)

        event_amounts: list[float] = []
        event_timestamps: list[datetime] = []
        thirty_day_events: list[dict[str, Any]] = []
        for item in account_events:
            timestamp = self._parse_dt(item.get("event_time") or item.get("timestamp"))
            if timestamp:
                event_timestamps.append(timestamp)
            amount = item.get("amount")
            try:
                numeric = float(amount)
                event_amounts.append(numeric)
            except Exception:
                numeric = None
            if as_of and timestamp and (as_of - timestamp).days <= 30:
                thirty_day_events.append(item)

        baseline_avg_amount = mean(event_amounts) if event_amounts else None
        baseline_monthly_inflow = 0.0
        baseline_monthly_outflow = 0.0
        for item in thirty_day_events:
            try:
                numeric = float(item.get("amount") or 0.0)
            except Exception:
                continue
            direction = str(item.get("direction") or "").strip().lower()
            if direction == "in":
                baseline_monthly_inflow += numeric
            else:
                baseline_monthly_outflow += numeric
        baseline_tx_count = len(thirty_day_events) if thirty_day_events else len(account_events)

        prior_alert_count_30d = 0
        prior_alert_count_90d = 0
        for item in outcomes:
            timestamp = self._parse_dt(item.get("event_time") or item.get("timestamp"))
            if not as_of or not timestamp:
                continue
            age_days = (as_of - timestamp).days
            if age_days <= 30:
                prior_alert_count_30d += 1
            if age_days <= 90:
                prior_alert_count_90d += 1

        prior_case_ids: set[str] = set()
        for item in case_actions:
            timestamp = self._parse_dt(item.get("event_time") or item.get("timestamp"))
            if as_of and timestamp and (as_of - timestamp).days <= 90:
                case_id = self._clean_text(item.get("case_id"))
                if case_id:
                    prior_case_ids.add(case_id)

        deviation_summary = None
        if payload_amount_value is not None and baseline_avg_amount and baseline_avg_amount > 0:
            ratio = payload_amount_value / baseline_avg_amount
            if ratio >= 2.0:
                deviation_summary = "Current alert amount is materially above the recent account activity baseline."
            elif ratio <= 0.5:
                deviation_summary = "Current alert amount is materially below the recent account activity baseline."
            else:
                deviation_summary = "Current alert amount is broadly consistent with recent account activity."
        elif current_window_tx_count and baseline_tx_count:
            if current_window_tx_count > baseline_tx_count:
                deviation_summary = "Current alert reflects elevated activity volume relative to recent history."
            else:
                deviation_summary = "Recent account history is limited; baseline comparison is only partially available."
        else:
            deviation_summary = "Historical account activity is limited; baseline comparison is not yet established."

        return {
            "baseline_avg_amount": baseline_avg_amount,
            "baseline_monthly_inflow": baseline_monthly_inflow if baseline_monthly_inflow > 0 else None,
            "baseline_monthly_outflow": baseline_monthly_outflow if baseline_monthly_outflow > 0 else None,
            "baseline_tx_count": baseline_tx_count or None,
            "current_window_inflow": inbound_amount if inbound_amount > 0 else None,
            "current_window_outflow": outbound_amount if outbound_amount > 0 else None,
            "current_window_tx_count": current_window_tx_count or None,
            "deviation_summary": deviation_summary,
            "prior_alert_count_30d": prior_alert_count_30d,
            "prior_alert_count_90d": prior_alert_count_90d,
            "prior_case_count_90d": len(prior_case_ids),
        }

    def _counterparty_summary(
        self,
        *,
        payload: dict[str, Any],
        account_events: list[dict[str, Any]],
        network_graph: dict[str, Any] | None,
    ) -> dict[str, Any]:
        transactions = self._payload_transactions(payload)
        current_counterparties = {
            self._clean_text(item.get("counterparty_id") or item.get("counterparty"))
            for item in transactions
        }
        current_counterparties = {item for item in current_counterparties if item}

        historical_counterparties: list[str] = []
        counterparty_countries: set[str] = set()
        counterparty_banks: set[str] = set()
        for item in account_events:
            counterparty = self._clean_text(item.get("counterparty_id"))
            if counterparty:
                historical_counterparties.append(counterparty)
            country = self._clean_text(item.get("country"))
            if country:
                counterparty_countries.add(country)
            bank = self._clean_text(item.get("counterparty_bank_id"))
            if bank:
                counterparty_banks.add(bank)

        counts = Counter(historical_counterparties)
        top_counterparties = [
            f"{counterparty} ({count} interactions)"
            for counterparty, count in counts.most_common(4)
        ]
        if not top_counterparties and current_counterparties:
            top_counterparties = sorted(current_counterparties)

        new_counterparty_share = None
        recurring_counterparty_share = None
        if current_counterparties:
            unseen = [item for item in current_counterparties if item not in counts]
            new_counterparty_share = len(unseen) / max(len(current_counterparties), 1)
            recurring_counterparty_share = 1.0 - new_counterparty_share

        if network_graph:
            for node in self._to_list(network_graph.get("nodes")):
                node_obj = self._to_dict(node)
                if str(node_obj.get("type") or "").lower() == "counterparty":
                    label = self._clean_text(node_obj.get("label"))
                    if label and label not in top_counterparties:
                        top_counterparties.append(label)

        return {
            "top_counterparties": top_counterparties[:5],
            "new_counterparty_share": new_counterparty_share,
            "recurring_counterparty_share": recurring_counterparty_share,
            "counterparty_countries": sorted(counterparty_countries)[:6],
            "counterparty_bank_count": len(counterparty_banks) or None,
        }

    def _geography_payment_summary(self, *, payload: dict[str, Any], account_events: list[dict[str, Any]]) -> dict[str, Any]:
        transactions = self._payload_transactions(payload)
        countries = {
            self._clean_text(payload.get("country")),
            self._clean_text(payload.get("destination_country")),
        }
        channels = {
            self._clean_text(payload.get("payment_channel")),
            self._clean_text(payload.get("channel")),
            self._clean_text(payload.get("payment_type")),
        }
        currencies = {
            self._clean_text(payload.get("currency")),
        }
        is_cross_border = payload.get("is_cross_border")

        for item in transactions:
            countries.add(self._clean_text(item.get("country")))
            countries.add(self._clean_text(item.get("destination_country")))
            channels.add(self._clean_text(item.get("channel")))
            channels.add(self._clean_text(item.get("payment_type")))
            currencies.add(self._clean_text(item.get("currency")))

        for item in account_events:
            countries.add(self._clean_text(item.get("country")))
            channels.add(self._clean_text(item.get("channel")))
            channels.add(self._clean_text(item.get("payment_type")))
            currencies.add(self._clean_text(item.get("currency")))
            if is_cross_border is None and item.get("is_cross_border") is not None:
                is_cross_border = bool(item.get("is_cross_border"))

        return {
            "is_cross_border": None if is_cross_border is None else bool(is_cross_border),
            "countries_involved": sorted(item for item in countries if item),
            "payment_channels": sorted(item for item in channels if item),
            "currency_mix": sorted(item for item in currencies if item),
        }

    def _screening_summary(
        self,
        *,
        customer_record: dict[str, Any],
        counterparty_records: list[dict[str, Any]],
        latest_source_health: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        sanctions_hits: list[str] = []
        watchlist_hits: list[str] = []
        pep_hits: list[str] = []
        adverse_media_hits: list[str] = []
        screening_checked_at = None

        if customer_record:
            if customer_record.get("sanctions_flag"):
                sanctions_hits.append("Customer sanctions screening flag present")
            if customer_record.get("pep_flag"):
                pep_hits.append("Customer PEP flag present")
            screening_checked_at = customer_record.get("effective_from") or customer_record.get("effective_to")

        for record in counterparty_records:
            payload_json = self._to_dict(record.get("payload_json"))
            if payload_json.get("sanctions_flag"):
                sanctions_hits.append("Counterparty sanctions screening flag present")
            if payload_json.get("watchlist_flag"):
                watchlist_hits.append("Counterparty watchlist screening flag present")
            for item in self._to_list(payload_json.get("watchlist_hits")):
                text = self._clean_text(item)
                if text:
                    watchlist_hits.append(text)
            for item in self._to_list(payload_json.get("adverse_media_hits")):
                text = self._clean_text(item)
                if text:
                    adverse_media_hits.append(text)
            screening_checked_at = screening_checked_at or record.get("effective_from") or record.get("effective_to")

        source_records_available = any(
            name in self._SCREENING_SOURCE_NAMES
            for name in latest_source_health
        ) or bool(customer_record) or bool(counterparty_records)

        if sanctions_hits or watchlist_hits or pep_hits or adverse_media_hits:
            screening_status = "hits_found"
        elif source_records_available and screening_checked_at:
            screening_status = "no_hits"
        else:
            screening_status = "unavailable"

        return {
            "sanctions_hits": sorted(set(sanctions_hits)),
            "watchlist_hits": sorted(set(watchlist_hits)),
            "pep_hits": sorted(set(pep_hits)),
            "adverse_media_hits": sorted(set(adverse_media_hits)),
            "screening_checked_at": screening_checked_at,
            "screening_status": screening_status,
        }

    def _data_availability(
        self,
        *,
        customer_profile: dict[str, Any],
        account_profile: dict[str, Any],
        behavior_baseline: dict[str, Any],
        counterparty_summary: dict[str, Any],
        geography_payment_summary: dict[str, Any],
        screening_summary: dict[str, Any],
        latest_source_health: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        missing_sections: list[str] = []
        sections = {
            "customer_profile": customer_profile,
            "account_profile": account_profile,
            "behavior_baseline": behavior_baseline,
            "counterparty_summary": counterparty_summary,
            "geography_payment_summary": geography_payment_summary,
        }
        for name, payload in sections.items():
            if not self._has_value(payload):
                missing_sections.append(name)
        if screening_summary.get("screening_status") == "unavailable":
            missing_sections.append("screening_summary")

        missing_count = len(missing_sections)
        coverage_status = "enriched" if missing_count == 0 else "partial" if missing_count <= 2 else "limited"

        if not latest_source_health:
            freshness_status = "legacy_only"
        else:
            degraded = {str(item.get("status") or "").strip().lower() for item in latest_source_health.values()}
            if degraded & {"stale", "degraded", "error"}:
                freshness_status = "stale"
            else:
                freshness_status = "current"

        return {
            "missing_sections": missing_sections,
            "coverage_status": coverage_status,
            "freshness_status": freshness_status,
        }

    def generate_sections(
        self,
        *,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
        network_graph: dict[str, Any] | None = None,
        case_status: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._get_alert_payload(tenant_id=tenant_id, alert_id=alert_id, run_id=run_id)
        snapshot = self._enrichment_repository.get_enrichment_context_snapshot(
            tenant_id=tenant_id,
            alert_id=alert_id,
            as_of_timestamp=payload.get("timestamp") or payload.get("created_at"),
        )
        entity_id = self._resolve_entity_id(payload)
        account_id = self._resolve_account_id(payload)
        customers = list(self._enrichment_repository.list_master_customers(tenant_id) or [])
        accounts = list(self._enrichment_repository.list_master_accounts(tenant_id) or [])
        counterparties = list(self._enrichment_repository.list_master_counterparties(tenant_id) or [])
        latest_source_health = {
            str(item.get("source_name") or ""): item
            for item in self._enrichment_repository.list_latest_source_health(tenant_id)
            if item.get("source_name")
        }
        as_of = self._parse_dt(payload.get("timestamp") or payload.get("created_at")) or _utcnow()

        customer_record = self._match_master_customer(customers, payload, entity_id)
        account_record = self._match_master_account(accounts, payload, account_id)

        current_counterparty_ids = {
            self._clean_text(item.get("counterparty_id"))
            for item in self._to_list(snapshot.get("account_events"))
        }
        current_counterparty_ids = {item for item in current_counterparty_ids if item}
        counterparty_records = [
            row for row in counterparties
            if str(row.get("counterparty_id") or "").strip() in current_counterparty_ids
            or str(row.get("external_counterparty_id") or "").strip() in current_counterparty_ids
        ]

        customer_profile = self._customer_profile(
            tenant_id=tenant_id,
            payload=payload,
            customer_record=customer_record,
            case_status=case_status,
        )
        account_profile = self._account_profile(payload=payload, account_record=account_record, as_of=as_of)
        behavior_baseline = self._behavior_baseline(
            payload=payload,
            account_events=self._to_list(snapshot.get("account_events")),
            outcomes=self._to_list(snapshot.get("alert_outcomes")),
            case_actions=self._to_list(snapshot.get("case_actions")),
            as_of=as_of,
        )
        counterparty_summary = self._counterparty_summary(
            payload=payload,
            account_events=self._to_list(snapshot.get("account_events")),
            network_graph=self._to_dict(network_graph),
        )
        geography_payment_summary = self._geography_payment_summary(
            payload=payload,
            account_events=self._to_list(snapshot.get("account_events")),
        )
        screening_summary = self._screening_summary(
            customer_record=customer_record,
            counterparty_records=counterparty_records,
            latest_source_health=latest_source_health,
        )
        data_availability = self._data_availability(
            customer_profile=customer_profile,
            account_profile=account_profile,
            behavior_baseline=behavior_baseline,
            counterparty_summary=counterparty_summary,
            geography_payment_summary=geography_payment_summary,
            screening_summary=screening_summary,
            latest_source_health=latest_source_health,
        )
        return {
            "customer_profile": customer_profile,
            "account_profile": account_profile,
            "behavior_baseline": behavior_baseline,
            "counterparty_summary": counterparty_summary,
            "geography_payment_summary": geography_payment_summary,
            "screening_summary": screening_summary,
            "data_availability": data_availability,
        }
