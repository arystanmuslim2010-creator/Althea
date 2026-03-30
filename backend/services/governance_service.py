from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd


class GovernanceService:
    """
    Enterprise governance policy adapter.

    Produces queue eligibility and suppression metadata from risk scores while
    preserving fields consumed by existing APIs/UI.
    """

    def __init__(
        self,
        repository=None,
        explainability_service=None,
        lifecycle_service=None,
        suppress_threshold: float = 50.0,
        mandatory_review_threshold: float = 75.0,
        p0_threshold: float = 90.0,
    ) -> None:
        self._repository = repository
        self._explainability_service = explainability_service
        self._lifecycle_service = lifecycle_service
        self._suppress_threshold = float(suppress_threshold)
        self._mandatory_review_threshold = float(mandatory_review_threshold)
        self._p0_threshold = float(p0_threshold)

    @staticmethod
    def _risk_band(score: float) -> str:
        if score >= 90:
            return "CRITICAL"
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _priority(score: float) -> str:
        if score >= 90:
            return "P0"
        if score >= 75:
            return "P1"
        return "P2"

    @staticmethod
    def _heuristic_uplift(frame: pd.DataFrame) -> pd.Series:
        if frame is None or frame.empty:
            return pd.Series(dtype=float)

        idx = frame.index
        typology = frame.get("typology", pd.Series("", index=idx)).astype(str).str.lower().str.strip()
        country = frame.get("country", pd.Series("", index=idx)).astype(str).str.upper().str.strip()
        amount = pd.to_numeric(frame.get("amount", pd.Series(0.0, index=idx)), errors="coerce").fillna(0.0)
        velocity_raw = frame.get("num_transactions")
        if velocity_raw is None:
            velocity_raw = frame.get("txn_count_24h", pd.Series(0.0, index=idx))
        velocity = pd.to_numeric(velocity_raw, errors="coerce").fillna(0.0)

        typology_boost = typology.map(
            {
                "sanctions": 18.0,
                "structuring": 12.0,
                "high_amount_outlier": 10.0,
                "cross_border": 8.0,
                "flow_through": 5.0,
            }
        ).fillna(0.0)
        country_boost = country.map({"IR": 20.0, "KP": 20.0, "RU": 12.0, "AE": 8.0}).fillna(0.0)
        amount_boost = pd.Series(
            np.select(
                [amount >= 20000, amount >= 10000, amount >= 5000, amount >= 2000],
                [20.0, 15.0, 10.0, 6.0],
                default=0.0,
            ),
            index=idx,
            dtype=float,
        )
        velocity_boost = pd.Series(
            np.select([velocity >= 12, velocity >= 8, velocity >= 5], [8.0, 5.0, 2.0], default=0.0),
            index=idx,
            dtype=float,
        )
        return np.clip(typology_boost + country_boost + amount_boost + velocity_boost, 0.0, 50.0)

    @staticmethod
    def _build_rule_evidence_row(row: dict[str, Any]) -> dict[str, Any]:
        evidence: dict[str, Any] = {}
        typology = str(row.get("typology") or "").lower().strip()
        country = str(row.get("country") or "").upper().strip()
        try:
            amount = float(row.get("amount", 0.0) or 0.0)
        except Exception:
            amount = 0.0
        velocity_raw = row.get("num_transactions", row.get("txn_count_24h", 0.0))
        try:
            velocity = float(velocity_raw or 0.0)
        except Exception:
            velocity = 0.0

        if typology in {"sanctions", "structuring", "high_amount_outlier", "cross_border", "flow_through"}:
            evidence["typology_signal"] = {"typology": typology, "weight": 1.0}
        if country in {"IR", "KP", "RU", "AE"}:
            evidence["geo_risk_signal"] = {"country": country, "weight": 1.0}
        if amount >= 2000.0:
            evidence["high_amount_signal"] = {"amount": amount, "weight": 1.0}
        if velocity >= 5.0:
            evidence["velocity_signal"] = {"txn_count_24h": velocity, "weight": 1.0}
        if float(row.get("risk_uplift", 0.0) or 0.0) > 0:
            evidence["hybrid_uplift_signal"] = {"risk_uplift": float(row.get("risk_uplift", 0.0) or 0.0), "weight": 1.0}
        return evidence

    @staticmethod
    def _is_demo_source(source: str | None) -> bool:
        normalized = str(source or "").strip().lower()
        return normalized in {"synthetic", "bankcsv", "demo", "generated_demo"}

    @staticmethod
    def _stabilize_model_score_distribution(scores: pd.Series) -> pd.Series:
        series = pd.to_numeric(scores, errors="coerce").fillna(0.0).clip(lower=0.0, upper=100.0)
        if len(series) <= 1:
            return series
        pct_rank = series.rank(method="average", pct=True).fillna(0.0)
        spread_bonus = pct_rank * 10.0
        # Mild deterministic expansion for demo runs:
        # compressed models (e.g. max ~50) get a +0..10 spread while preserving score order.
        stabilized = np.clip(series.to_numpy() + spread_bonus.to_numpy(), 0.0, 100.0)
        return pd.Series(stabilized, index=series.index, dtype=float)

    def apply_governance(self, alerts_df: pd.DataFrame, stabilize_for_demo: bool = False) -> pd.DataFrame:
        if alerts_df is None or alerts_df.empty:
            return pd.DataFrame()

        out = alerts_df.copy()
        out["risk_score"] = pd.to_numeric(out.get("risk_score", 0.0), errors="coerce").fillna(0.0)
        out["risk_score_model"] = out["risk_score"]
        if stabilize_for_demo:
            out["risk_score_model"] = self._stabilize_model_score_distribution(out["risk_score_model"])

        # Prevent score saturation in demo/bank modes by scaling heuristic uplift by remaining headroom.
        # This preserves ordering while avoiding large score plateaus at the max cap.
        out["risk_uplift_raw"] = self._heuristic_uplift(out)
        headroom = np.clip((100.0 - out["risk_score_model"]) / 100.0, 0.15, 1.0)
        base_multiplier = 0.55 if stabilize_for_demo else 0.75
        uplift_cap = 22.0 if stabilize_for_demo else 35.0
        out["risk_uplift"] = np.clip(out["risk_uplift_raw"] * headroom * base_multiplier, 0.0, uplift_cap)
        score_cap = 98.0 if stabilize_for_demo else 100.0
        out["risk_score"] = np.clip(out["risk_score_model"] + out["risk_uplift"], 0.0, score_cap)

        if stabilize_for_demo and len(out) > 1:
            near_cap_mask = out["risk_score"] >= (score_cap - 0.25)
            near_cap_ratio = float(near_cap_mask.mean())
            if near_cap_ratio > 0.2:
                cap_rows = out.loc[near_cap_mask, ["risk_score_model", "risk_uplift_raw"]].copy()
                rank_pct = cap_rows["risk_score_model"].rank(method="first", pct=True).fillna(0.5)
                uplift_rank = cap_rows["risk_uplift_raw"].rank(method="first", pct=True).fillna(0.5)
                spread = (rank_pct * 0.7 + uplift_rank * 0.3).clip(0.0, 1.0)
                out.loc[near_cap_mask, "risk_score"] = np.clip(score_cap - (1.0 - spread) * 3.0, 0.0, score_cap)

        out["risk_prob"] = np.clip(pd.to_numeric(out.get("risk_prob", out["risk_score"] / 100.0), errors="coerce").fillna(0.0), 0.0, 1.0)
        out["risk_prob"] = np.maximum(out["risk_prob"], out["risk_score"] / 100.0)
        out["risk_band"] = out["risk_score"].apply(self._risk_band)
        out["alert_priority"] = out["risk_score"].apply(self._priority)
        out["priority"] = out.get("priority", out["risk_band"].str.lower())

        def _status(score: float) -> str:
            if score < self._suppress_threshold:
                return "suppressed"
            if score >= self._mandatory_review_threshold:
                return "mandatory_review"
            return "eligible"

        out["governance_status"] = out["risk_score"].apply(_status)
        out["in_queue"] = (out["governance_status"] != "suppressed").map(lambda v: bool(v)).astype(object)
        out["suppression_code"] = out["governance_status"].apply(lambda s: "LOW_RISK" if s == "suppressed" else "")
        out["suppression_reason"] = out["governance_status"].apply(lambda s: "Risk below queue threshold" if s == "suppressed" else "")
        out["policy_version"] = out.get("policy_version", "2.0")

        rule_evidence_rows: list[str] = []
        rules_rows: list[str] = []
        for row in out.to_dict("records"):
            existing_evidence = self._parse_json(row.get("rule_evidence_json"), {})
            if not isinstance(existing_evidence, dict) or not existing_evidence:
                existing_evidence = self._build_rule_evidence_row(row)
            existing_rules = self._parse_json(row.get("rules_json"), [])
            if not isinstance(existing_rules, list) or not existing_rules:
                existing_rules = [
                    {"rule_id": key, "source": "hybrid_governance", "weight": value.get("weight", 1.0)}
                    for key, value in existing_evidence.items()
                    if isinstance(value, dict)
                ]
            rule_evidence_rows.append(json.dumps(existing_evidence, ensure_ascii=True))
            rules_rows.append(json.dumps(existing_rules, ensure_ascii=True))
        out["rule_evidence_json"] = rule_evidence_rows
        out["rules_json"] = rules_rows
        return out

    def metadata(self) -> dict[str, Any]:
        return {
            "suppress_threshold": self._suppress_threshold,
            "mandatory_review_threshold": self._mandatory_review_threshold,
            "p0_threshold": self._p0_threshold,
            "policy_version": "2.0",
        }

    @staticmethod
    def _parse_json(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            try:
                return json.loads(text)
            except Exception:
                return default
        return default

    def prioritize_alerts(self, tenant_id: str, run_id: str, alert_ids: list[str], model_version: str) -> list[dict[str, Any]]:
        if self._repository is None:
            return []
        if not alert_ids:
            return []

        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        selected = [dict(row) for row in payloads if str(row.get("alert_id")) in set(alert_ids)]
        if not selected:
            return []

        stabilize_for_demo = False
        runs = self._repository.list_pipeline_runs(tenant_id=tenant_id, limit=500)
        run_meta = next((item for item in runs if str(item.get("run_id")) == str(run_id)), None)
        if run_meta:
            stabilize_for_demo = self._is_demo_source(run_meta.get("source"))

        frame = pd.DataFrame(selected)
        governed = self.apply_governance(frame, stabilize_for_demo=stabilize_for_demo)
        updates: list[dict[str, Any]] = []
        for base_row in governed.to_dict("records"):
            row = dict(base_row)
            existing_top = self._parse_json(row.get("top_feature_contributions_json"), [])
            explanation: dict[str, Any] = {}
            if self._explainability_service is not None:
                explanation = self._explainability_service.generate_explanation(
                    model=None,
                    feature_frame=pd.DataFrame([row]),
                    model_version=model_version,
                    fallback_contributions=existing_top if isinstance(existing_top, list) else [],
                    tenant_id=tenant_id,
                    alert_id=str(row.get("alert_id") or ""),
                )
                row = self._explainability_service.merge_into_alert_metadata(row, explanation)
                row["risk_reason_codes"] = explanation.get("risk_reason_codes", [])
                row["feature_attribution"] = explanation.get("feature_attribution", [])
            if not row.get("ml_signals_json"):
                signals = {
                        "model_version": model_version,
                        "risk_reason_codes": row.get("risk_reason_codes", []),
                        "top_feature_contributions": row.get("feature_attribution") or existing_top,
                        "explanation_method": explanation.get("explanation_method", "unknown")
                        if self._explainability_service is not None
                        else "unknown",
                        "explanation_status": explanation.get("explanation_status", "unknown")
                        if self._explainability_service is not None
                        else "unknown",
                        "explanation_warning": explanation.get("explanation_warning")
                        if self._explainability_service is not None
                        else None,
                        "explanation_warning_code": explanation.get("explanation_warning_code")
                        if self._explainability_service is not None
                        else None,
                    }
                row["ml_signals_json"] = json.dumps(signals, ensure_ascii=True)
            if not row.get("features_json"):
                feature_snapshot: dict[str, Any] = {}
                for key in (
                    "amount",
                    "time_gap",
                    "num_transactions",
                    "country_risk",
                    "segment",
                    "typology",
                    "user_id",
                ):
                    if key in row:
                        feature_snapshot[key] = row.get(key)
                if not feature_snapshot:
                    feature_snapshot = {"alert_id": row.get("alert_id")}
                row["features_json"] = feature_snapshot
            # Keep explainability metadata in alert payload for analyst UI and audit use.
            row["risk_explain_json"] = row.get("risk_explain_json") or json.dumps(
                {
                    "model_version": model_version,
                    "risk_reason_codes": row.get("risk_reason_codes", []),
                    "feature_attribution": row.get("feature_attribution", []),
                    "explanation_method": explanation.get("explanation_method", "unknown")
                    if self._explainability_service is not None
                    else "unknown",
                    "explanation_status": explanation.get("explanation_status", "unknown")
                    if self._explainability_service is not None
                    else "unknown",
                    "explanation_warning": explanation.get("explanation_warning")
                    if self._explainability_service is not None
                    else None,
                    "explanation_warning_code": explanation.get("explanation_warning_code")
                    if self._explainability_service is not None
                    else None,
                },
                ensure_ascii=True,
            )
            updates.append(row)

        self._repository.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=updates)
        if self._lifecycle_service is not None:
            self._lifecycle_service.record_monitoring(
                tenant_id=tenant_id,
                model_version=model_version,
                model_drift=0.0,
                score_distribution_shift=float(governed["risk_score"].std(ddof=0) if "risk_score" in governed.columns else 0.0),
                alert_outcome_feedback=0.0,
                metadata={
                    "run_id": run_id,
                    "event": "alerts_prioritized",
                    "alert_count": len(updates),
                },
            )
        return updates
