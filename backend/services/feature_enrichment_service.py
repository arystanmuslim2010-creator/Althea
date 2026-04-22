from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from core.observability import record_enrichment_context_build
from features.builders.base import BuilderContext


@dataclass(slots=True)
class FeatureEnrichmentStats:
    recent_runs_considered: int
    alert_payload_rows_loaded: int
    transaction_history_rows: int
    outcome_history_rows: int
    peer_stat_rows: int
    case_history_rows: int
    graph_feature_rows: int


class FeatureEnrichmentService:
    """Assemble point-in-time-ish enrichment tables for feature builders.

    The repository does not yet expose dedicated history loaders, so this
    service derives the enrichment context from existing alert payloads and
    case records. The intent is to make runtime feature enrichment materially
    useful in the live pipeline without introducing synthetic external
    dependencies.
    """

    def __init__(
        self,
        repository,
        enrichment_repository=None,
        graph_feature_service=None,
        *,
        max_history_runs: int = 5,
        max_payloads_per_run: int = 5000,
    ) -> None:
        self._repository = repository
        self._enrichment_repository = enrichment_repository
        self._graph_feature_service = graph_feature_service
        self._max_history_runs = max(1, int(max_history_runs or 5))
        self._max_payloads_per_run = max(100, int(max_payloads_per_run or 5000))

    def build_context(
        self,
        *,
        tenant_id: str,
        alerts_df: pd.DataFrame,
        run_id: str | None = None,
    ) -> BuilderContext:
        current_records = self._records_from_frame(alerts_df)
        as_of_timestamp = self._resolve_as_of_timestamp(alerts_df)
        graph_features = self._build_graph_features(alerts_df)

        canonical = self._build_context_from_canonical_store(
            tenant_id=tenant_id,
            alerts_df=alerts_df,
            as_of_timestamp=as_of_timestamp,
        )

        historical_payloads: list[dict[str, Any]] = []
        recent_runs: list[str] = []
        transaction_history = canonical.get("transaction_history") if canonical else pd.DataFrame()
        outcome_history = canonical.get("outcome_history") if canonical else pd.DataFrame()
        case_history = canonical.get("case_history") if canonical else pd.DataFrame()
        peer_stats = pd.DataFrame()

        if transaction_history.empty or outcome_history.empty or case_history.empty or peer_stats.empty:
            historical_payloads, recent_runs = self._load_historical_payloads(tenant_id=tenant_id, run_id=run_id)
            combined_records = historical_payloads + current_records
            fallback_transaction_history = self._build_transaction_history(combined_records)
            fallback_peer_stats = self._build_peer_stats(combined_records)
            cases = self._safe_list_cases(tenant_id)
            entity_lookup = self._entity_lookup(combined_records)
            fallback_outcome_history = self._build_outcome_history(cases, entity_lookup)
            fallback_case_history = self._build_case_history(cases, entity_lookup)
            if transaction_history.empty:
                transaction_history = fallback_transaction_history
            if outcome_history.empty:
                outcome_history = fallback_outcome_history
            if case_history.empty:
                case_history = fallback_case_history
            peer_stats = fallback_peer_stats
        else:
            peer_stats = self._build_peer_stats(current_records)

        context = BuilderContext(
            transaction_history=transaction_history,
            outcome_history=outcome_history,
            peer_stats=peer_stats,
            graph_features=graph_features,
            case_history=case_history,
            tenant_id=str(tenant_id or ""),
            as_of_timestamp=as_of_timestamp,
        )
        context.enrichment_stats = FeatureEnrichmentStats(
            recent_runs_considered=len(recent_runs),
            alert_payload_rows_loaded=len(historical_payloads),
            transaction_history_rows=int(len(transaction_history)),
            outcome_history_rows=int(len(outcome_history)),
            peer_stat_rows=int(len(peer_stats)),
            case_history_rows=int(len(case_history)),
            graph_feature_rows=int(len(graph_features)),
        )
        status = "canonical" if canonical and not pd.DataFrame(canonical.get("transaction_history")).empty else "fallback"
        record_enrichment_context_build(status)
        return context

    def _build_context_from_canonical_store(
        self,
        *,
        tenant_id: str,
        alerts_df: pd.DataFrame,
        as_of_timestamp: Any,
    ) -> dict[str, pd.DataFrame] | None:
        if self._enrichment_repository is None:
            return None
        try:
            if not self._enrichment_repository.has_canonical_history(tenant_id):
                return None
            current_records = self._records_from_frame(alerts_df)
            entity_ids: list[str] = []
            alert_ids: list[str] = []
            for payload in current_records:
                alert_id = str(payload.get("alert_id") or "").strip()
                if alert_id:
                    alert_ids.append(alert_id)
                entity_id = self._extract_entity_id(payload)
                if entity_id:
                    entity_ids.append(entity_id)
            entity_ids = sorted({item for item in entity_ids if item})
            alert_ids = sorted({item for item in alert_ids if item})
            account_events = self._enrichment_repository.list_account_events_before(
                tenant_id=tenant_id,
                entity_ids=entity_ids,
                as_of_timestamp=as_of_timestamp,
                limit=5000,
            )
            outcomes = self._enrichment_repository.list_alert_outcomes_before(
                tenant_id=tenant_id,
                entity_ids=entity_ids,
                alert_ids=alert_ids,
                as_of_timestamp=as_of_timestamp,
                limit=1000,
            )
            case_actions = self._enrichment_repository.list_case_actions_before(
                tenant_id=tenant_id,
                entity_ids=entity_ids,
                alert_ids=alert_ids,
                as_of_timestamp=as_of_timestamp,
                limit=2000,
            )
            transaction_history = self._build_transaction_history_from_events(account_events)
            outcome_history = self._build_outcome_history_from_records(outcomes)
            case_history = self._build_case_history_from_actions(case_actions)
            if transaction_history.empty and outcome_history.empty and case_history.empty:
                return None
            return {
                "transaction_history": transaction_history,
                "outcome_history": outcome_history,
                "case_history": case_history,
            }
        except Exception:
            return None

    @staticmethod
    def _build_transaction_history_from_events(records: list[dict[str, Any]]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(columns=["entity_id", "timestamp", "amount", "country", "counterparty_id", "is_cross_border", "direction"])
        frame = pd.DataFrame(records)
        for src, dst in (
            ("event_time", "timestamp"),
            ("amount", "amount"),
            ("country", "country"),
            ("counterparty_id", "counterparty_id"),
            ("is_cross_border", "is_cross_border"),
            ("direction", "direction"),
            ("entity_id", "entity_id"),
        ):
            if src != dst and src in frame.columns and dst not in frame.columns:
                frame[dst] = frame[src]
        frame["timestamp"] = pd.to_datetime(frame.get("timestamp"), utc=True, errors="coerce")
        return frame[["entity_id", "timestamp", "amount", "country", "counterparty_id", "is_cross_border", "direction"]].dropna(subset=["entity_id", "timestamp"])

    @staticmethod
    def _build_outcome_history_from_records(records: list[dict[str, Any]]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(columns=["entity_id", "alert_id", "analyst_decision", "timestamp", "typology"])
        rows = []
        for item in records:
            rows.append(
                {
                    "entity_id": str(item.get("entity_id") or "").strip(),
                    "alert_id": str(item.get("alert_id") or "").strip(),
                    "analyst_decision": str(item.get("decision") or item.get("status") or "unknown").strip().lower(),
                    "timestamp": pd.to_datetime(item.get("event_time"), utc=True, errors="coerce"),
                    "typology": str(((item.get("payload_json") or {}).get("typology")) or "anomaly"),
                }
            )
        frame = pd.DataFrame(rows)
        return frame.dropna(subset=["entity_id"]).reset_index(drop=True)

    @staticmethod
    def _build_case_history_from_actions(records: list[dict[str, Any]]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(columns=["entity_id", "resolution_hours", "touch_count", "typology"])
        frame = pd.DataFrame(records)
        if frame.empty:
            return pd.DataFrame(columns=["entity_id", "resolution_hours", "touch_count", "typology"])
        frame["event_time"] = pd.to_datetime(frame.get("event_time"), utc=True, errors="coerce")
        rows: list[dict[str, Any]] = []
        for _, group in frame.groupby("case_id", dropna=False):
            group = group.sort_values("event_time")
            entity_id = str(group["entity_id"].dropna().iloc[0]) if "entity_id" in group.columns and group["entity_id"].dropna().any() else ""
            if not entity_id:
                continue
            first = group["event_time"].dropna().min()
            last = group["event_time"].dropna().max()
            resolution_hours = 24.0
            if pd.notna(first) and pd.notna(last):
                resolution_hours = max(0.0, float((last - first).total_seconds() / 3600.0))
            rows.append(
                {
                    "entity_id": entity_id,
                    "resolution_hours": resolution_hours,
                    "touch_count": float(len(group)),
                    "typology": "anomaly",
                }
            )
        return pd.DataFrame(rows)

    def _safe_list_cases(self, tenant_id: str) -> list[dict[str, Any]]:
        try:
            return list(self._repository.list_cases(tenant_id) or [])
        except Exception:
            return []

    def _load_historical_payloads(self, *, tenant_id: str, run_id: str | None) -> tuple[list[dict[str, Any]], list[str]]:
        try:
            recent_runs = list(self._repository.list_pipeline_runs(tenant_id, limit=self._max_history_runs) or [])
        except Exception:
            recent_runs = []

        ordered_run_ids: list[str] = []
        seen: set[str] = set()
        if run_id:
            normalized = str(run_id).strip()
            if normalized:
                ordered_run_ids.append(normalized)
                seen.add(normalized)
        for row in recent_runs:
            candidate = str(row.get("run_id") or "").strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            ordered_run_ids.append(candidate)
            if len(ordered_run_ids) >= self._max_history_runs:
                break

        payloads: list[dict[str, Any]] = []
        for current_run_id in ordered_run_ids:
            try:
                rows = self._repository.list_alert_payloads_by_run(
                    tenant_id=tenant_id,
                    run_id=current_run_id,
                    limit=self._max_payloads_per_run,
                )
            except Exception:
                rows = []
            for row in rows or []:
                if isinstance(row, dict):
                    payloads.append(dict(row))
        return payloads, ordered_run_ids

    @staticmethod
    def _records_from_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
        if frame is None or frame.empty:
            return []
        out = frame.copy()
        for column in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[column]):
                out[column] = out[column].astype("datetime64[ns, UTC]").astype(str)
        return [dict(row) for row in out.to_dict("records")]

    @staticmethod
    def _resolve_as_of_timestamp(frame: pd.DataFrame) -> Any:
        if frame is None or frame.empty:
            return None
        for column in ("timestamp", "created_at", "timestamp_utc", "alert_created_at"):
            if column in frame.columns:
                parsed = pd.to_datetime(frame[column], utc=True, errors="coerce")
                parsed = parsed.dropna()
                if not parsed.empty:
                    return parsed.max()
        return None

    @staticmethod
    def _payload_accounts(payload: dict[str, Any]) -> list[dict[str, Any]]:
        accounts = payload.get("accounts")
        return list(accounts) if isinstance(accounts, list) else []

    @classmethod
    def _extract_entity_id(cls, payload: dict[str, Any]) -> str:
        candidates = [
            payload.get("user_id"),
            payload.get("customer_id"),
            payload.get("account_id"),
        ]
        for account in cls._payload_accounts(payload):
            candidates.extend(
                [
                    account.get("account_id"),
                    account.get("customer_id"),
                ]
            )
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value
        return ""

    @classmethod
    def _extract_country(cls, payload: dict[str, Any]) -> str:
        candidates = [payload.get("country")]
        for account in cls._payload_accounts(payload):
            candidates.append(account.get("country"))
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value.upper()
        return "UNKNOWN"

    @classmethod
    def _extract_segment(cls, payload: dict[str, Any]) -> str:
        candidates = [payload.get("segment")]
        for account in cls._payload_accounts(payload):
            candidates.append(account.get("segment"))
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value.lower()
        return "retail"

    @classmethod
    def _extract_typology(cls, payload: dict[str, Any]) -> str:
        value = str(payload.get("typology") or "").strip()
        return value.lower() if value else "anomaly"

    @staticmethod
    def _parse_timestamp(value: Any) -> pd.Timestamp | None:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed

    @classmethod
    def _build_transaction_history(cls, records: list[dict[str, Any]]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for payload in records:
            entity_id = cls._extract_entity_id(payload)
            if not entity_id:
                continue
            country = cls._extract_country(payload)
            transactions = payload.get("transactions")
            if isinstance(transactions, list) and transactions:
                for tx in transactions:
                    if not isinstance(tx, dict):
                        continue
                    ts = cls._parse_timestamp(tx.get("timestamp") or payload.get("timestamp") or payload.get("created_at"))
                    if ts is None:
                        continue
                    sender = str(tx.get("sender") or "").strip()
                    receiver = str(tx.get("receiver") or "").strip()
                    direction = None
                    counterparty_id = ""
                    if sender and sender == entity_id:
                        direction = "debit"
                        counterparty_id = receiver
                    elif receiver and receiver == entity_id:
                        direction = "credit"
                        counterparty_id = sender
                    else:
                        counterparty_id = receiver or sender or str(tx.get("counterparty_id") or "").strip()
                    rows.append(
                        {
                            "entity_id": entity_id,
                            "timestamp": ts,
                            "amount": float(tx.get("amount", 0.0) or 0.0),
                            "country": country,
                            "counterparty_id": counterparty_id,
                            "is_cross_border": float(bool(tx.get("is_cross_border", False))),
                            "direction": direction,
                        }
                    )
                continue

            ts = cls._parse_timestamp(payload.get("timestamp") or payload.get("created_at"))
            if ts is None:
                continue
            rows.append(
                {
                    "entity_id": entity_id,
                    "timestamp": ts,
                    "amount": float(payload.get("amount", 0.0) or 0.0),
                    "country": country,
                    "counterparty_id": str(payload.get("counterparty_id") or payload.get("counterparty_account") or ""),
                    "is_cross_border": 0.0,
                    "direction": None,
                }
            )

        if not rows:
            return pd.DataFrame(columns=["entity_id", "timestamp", "amount", "country", "counterparty_id", "is_cross_border", "direction"])
        frame = pd.DataFrame(rows)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["entity_id", "timestamp"])
        return frame.reset_index(drop=True)

    @classmethod
    def _build_peer_stats(cls, records: list[dict[str, Any]]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for payload in records:
            amount = float(payload.get("amount", 0.0) or 0.0)
            tx_count = float(payload.get("num_transactions", 0.0) or 0.0)
            rows.append(
                {
                    "segment": cls._extract_segment(payload),
                    "typology": cls._extract_typology(payload),
                    "country": cls._extract_country(payload),
                    "amount": amount,
                    "velocity": tx_count,
                }
            )
        if not rows:
            return pd.DataFrame(columns=["segment", "typology", "peer_amount_p50", "peer_amount_p90", "peer_amount_mean", "peer_amount_std", "peer_velocity_p50", "peer_velocity_p90", "segment_common_countries", "segment_typology_freq"])

        base = pd.DataFrame(rows)
        result_rows: list[dict[str, Any]] = []
        for segment, segment_frame in base.groupby("segment", dropna=False):
            amount_series = pd.to_numeric(segment_frame["amount"], errors="coerce").fillna(0.0)
            velocity_series = pd.to_numeric(segment_frame["velocity"], errors="coerce").fillna(0.0)
            common_countries = ",".join(
                sorted(
                    {
                        str(value).upper()
                        for value in segment_frame["country"].dropna().astype(str).tolist()
                        if str(value).strip()
                    }
                )[:5]
            )
            segment_size = max(1, len(segment_frame))
            typology_freq = (
                segment_frame["typology"].astype(str).value_counts(dropna=False).to_dict()
                if "typology" in segment_frame.columns
                else {}
            )
            for typology, count in typology_freq.items():
                result_rows.append(
                    {
                        "segment": str(segment),
                        "typology": str(typology),
                        "peer_amount_p50": float(amount_series.quantile(0.50)),
                        "peer_amount_p90": float(amount_series.quantile(0.90)),
                        "peer_amount_mean": float(amount_series.mean()),
                        "peer_amount_std": float(amount_series.std(ddof=0) or 1.0),
                        "peer_velocity_p50": float(velocity_series.quantile(0.50)),
                        "peer_velocity_p90": float(velocity_series.quantile(0.90)),
                        "segment_common_countries": common_countries,
                        "segment_typology_freq": float(count / segment_size),
                    }
                )
        return pd.DataFrame(result_rows)

    @classmethod
    def _entity_lookup(cls, records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        mapping: dict[str, dict[str, Any]] = {}
        for payload in records:
            alert_id = str(payload.get("alert_id") or "").strip()
            if not alert_id or alert_id in mapping:
                continue
            entity_id = cls._extract_entity_id(payload)
            if not entity_id:
                continue
            mapping[alert_id] = {
                "entity_id": entity_id,
                "typology": cls._extract_typology(payload),
            }
        return mapping

    @staticmethod
    def _case_decision(status: Any) -> str:
        normalized = str(status or "").strip().lower()
        if not normalized:
            return "unknown"
        return normalized

    @classmethod
    def _build_outcome_history(
        cls,
        cases: list[dict[str, Any]],
        entity_lookup: dict[str, dict[str, Any]],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for case in cases:
            alert_id = str(case.get("alert_id") or "").strip()
            entity_meta = entity_lookup.get(alert_id)
            if not entity_meta:
                continue
            ts = cls._parse_timestamp(case.get("updated_at") or case.get("created_at"))
            rows.append(
                {
                    "entity_id": entity_meta["entity_id"],
                    "alert_id": alert_id,
                    "analyst_decision": cls._case_decision(case.get("status")),
                    "timestamp": ts,
                    "typology": entity_meta.get("typology") or "anomaly",
                }
            )
        if not rows:
            return pd.DataFrame(columns=["entity_id", "alert_id", "analyst_decision", "timestamp", "typology"])
        frame = pd.DataFrame(rows)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        return frame.dropna(subset=["entity_id"]).reset_index(drop=True)

    @classmethod
    def _build_case_history(
        cls,
        cases: list[dict[str, Any]],
        entity_lookup: dict[str, dict[str, Any]],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for case in cases:
            alert_id = str(case.get("alert_id") or "").strip()
            entity_meta = entity_lookup.get(alert_id)
            if not entity_meta:
                continue
            created_at = cls._parse_timestamp(case.get("created_at"))
            updated_at = cls._parse_timestamp(case.get("updated_at"))
            resolution_hours = 24.0
            if created_at is not None and updated_at is not None:
                resolution_hours = max(0.0, float((updated_at - created_at).total_seconds() / 3600.0))
            rows.append(
                {
                    "entity_id": entity_meta["entity_id"],
                    "resolution_hours": resolution_hours,
                    "touch_count": float(case.get("touch_count") or 1.0),
                    "typology": entity_meta.get("typology") or "anomaly",
                }
            )
        if not rows:
            return pd.DataFrame(columns=["entity_id", "resolution_hours", "touch_count", "typology"])
        return pd.DataFrame(rows)

    def _build_graph_features(self, alerts_df: pd.DataFrame) -> pd.DataFrame:
        if alerts_df is None or alerts_df.empty or self._graph_feature_service is None:
            return pd.DataFrame()
        try:
            frame = self._graph_feature_service.extract_features_for_batch(alerts_df)
            return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()
