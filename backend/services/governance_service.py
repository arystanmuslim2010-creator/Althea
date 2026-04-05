"""Governance service — policy enforcement only.

This service is responsible for compliance constraints, suppression logic,
and mandatory review flagging. It is NOT a ranking engine.

Ranking is delegated to the decision layer (decision/priority_formula.py).
Governance status is enforced after ranking by decision/policy_engine.py.

Backward-compatible public methods are preserved so that existing callers
(alerts_router, pipeline_service, investigation_router) are not broken.
The heuristic uplift logic is retained as a regulatory urgency signal
that feeds into the priority formula rather than as a direct score modifier.
"""
from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from decision.action_router import ActionRouter
from decision.policy_engine import PolicyConfig, PolicyEngine
from decision.priority_formula import PriorityFormula, PriorityWeights


class GovernanceService:
    """Governance policy adapter.

    Produces queue eligibility, suppression metadata, and audit records
    from priority scores. All ranking decisions are delegated to
    PriorityFormula; this class enforces compliance constraints only.
    """

    def __init__(
        self,
        repository=None,
        explainability_service=None,
        lifecycle_service=None,
        suppress_threshold: float = 30.0,
        mandatory_review_threshold: float = 70.0,
        p0_threshold: float = 85.0,
    ) -> None:
        self._repository = repository
        self._explainability_service = explainability_service
        self._lifecycle_service = lifecycle_service

        config = PolicyConfig(
            suppress_threshold=float(suppress_threshold),
            mandatory_review_threshold=float(mandatory_review_threshold),
        )
        self._policy_engine = PolicyEngine(config=config)
        self._priority_formula = PriorityFormula()
        self._action_router = ActionRouter()

        # Keep thresholds on self for metadata() compatibility
        self._suppress_threshold = float(suppress_threshold)
        self._mandatory_review_threshold = float(mandatory_review_threshold)
        self._p0_threshold = float(p0_threshold)

    # ------------------------------------------------------------------
    # Primary pipeline entrypoint
    # ------------------------------------------------------------------

    def apply_governance(
        self,
        alerts_df: pd.DataFrame,
        stabilize_for_demo: bool = False,
    ) -> pd.DataFrame:
        """Apply governance policy to a scored alerts DataFrame.

        Expects alerts_df to contain:
            risk_score (from inference)       → maps to escalation_prob
            Optionally: graph_risk_score, similar_suspicious_strength,
                        p50_hours, typology, country, amount

        Returns alerts_df with governance columns added:
            governance_status, in_queue, suppression_code,
            suppression_reason, risk_band, alert_priority,
            priority_score, queue_action, compliance_flags_json
        """
        if alerts_df is None or alerts_df.empty:
            return pd.DataFrame()

        out = alerts_df.copy()

        # Base ML score (escalation probability)
        out["risk_score"] = pd.to_numeric(
            out.get("risk_score", 0.0), errors="coerce"
        ).fillna(0.0)

        # Demo stabilization (preserved for compatibility)
        if stabilize_for_demo:
            out["risk_score"] = self._stabilize_model_score_distribution(out["risk_score"])

        # Regulatory urgency signal (replaces direct heuristic uplift in score)
        out["regulatory_urgency"] = self._regulatory_urgency(out)

        def _num_col(name: str, default: float) -> pd.Series:
            raw = out.get(name, pd.Series(default, index=out.index))
            return pd.to_numeric(raw, errors="coerce").fillna(default)

        # Compute priority score via formula
        signals_df = pd.DataFrame({
            "escalation_prob": out["risk_score"] / 100.0,
            "graph_risk_score": _num_col("graph_risk_score", 0.0),
            "similar_suspicious_strength": _num_col("similar_suspicious_strength", 0.0),
            "p50_hours": _num_col("p50_hours", 24.0),
            "uncertainty": _num_col("uncertainty", 0.0),
            "regulatory_urgency": out["regulatory_urgency"],
        })
        has_extended_signals = any(
            col in out.columns
            for col in ("graph_risk_score", "similar_suspicious_strength", "p50_hours", "uncertainty")
        )
        if has_extended_signals:
            out["priority_score"] = self._priority_formula.compute_batch(signals_df)
        else:
            # Legacy fallback: preserve historical semantics where governance used risk_score directly.
            out["priority_score"] = pd.to_numeric(out["risk_score"], errors="coerce").fillna(0.0).clip(0.0, 100.0)

        # Apply policy rules
        out = self._policy_engine.apply(out)

        # Queue actions
        out["queue_action"] = self._action_router.route_batch(out)

        # Backward-compat: keep risk_prob column
        out["risk_prob"] = np.clip(out["risk_score"] / 100.0, 0.0, 1.0)

        # Rule evidence (preserved for existing API contracts)
        rule_evidence_rows = []
        rules_rows = []
        for row in out.to_dict("records"):
            evidence = self._build_rule_evidence_row(row)
            rules = [
                {"rule_id": key, "source": "governance_policy", "weight": value.get("weight", 1.0)}
                for key, value in evidence.items()
                if isinstance(value, dict)
            ]
            rule_evidence_rows.append(json.dumps(evidence, ensure_ascii=True))
            rules_rows.append(json.dumps(rules, ensure_ascii=True))
        out["rule_evidence_json"] = rule_evidence_rows
        out["rules_json"] = rules_rows

        return out

    # ------------------------------------------------------------------
    # Backward-compatible prioritize_alerts method
    # ------------------------------------------------------------------

    def prioritize_alerts(
        self,
        tenant_id: str,
        run_id: str,
        alert_ids: list[str],
        model_version: str,
    ) -> list[dict[str, Any]]:
        if self._repository is None or not alert_ids:
            return []

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id, limit=500000
        )
        selected = [dict(row) for row in payloads if str(row.get("alert_id")) in set(alert_ids)]
        if not selected:
            return []

        runs = self._repository.list_pipeline_runs(tenant_id=tenant_id, limit=500)
        run_meta = next((r for r in runs if str(r.get("run_id")) == str(run_id)), None)
        stabilize = self._is_demo_source(run_meta.get("source") if run_meta else None)

        frame = pd.DataFrame(selected)
        governed = self.apply_governance(frame, stabilize_for_demo=stabilize)
        updates: list[dict[str, Any]] = []

        for row in governed.to_dict("records"):
            row = dict(row)
            # Attach explainability if service available
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
                row["ml_signals_json"] = json.dumps({
                    "model_version": model_version,
                    "risk_reason_codes": row.get("risk_reason_codes", []),
                    "top_feature_contributions": row.get("feature_attribution") or existing_top,
                    "explanation_method": explanation.get("explanation_method", "unknown"),
                    "explanation_status": explanation.get("explanation_status", "unknown"),
                }, ensure_ascii=True)

            updates.append(row)

        self._repository.save_alert_payloads(
            tenant_id=tenant_id, run_id=run_id, records=updates
        )
        if self._lifecycle_service is not None:
            self._lifecycle_service.record_monitoring(
                tenant_id=tenant_id,
                model_version=model_version,
                model_drift=0.0,
                score_distribution_shift=float(governed["risk_score"].std(ddof=0) if "risk_score" in governed.columns else 0.0),
                alert_outcome_feedback=0.0,
                metadata={"run_id": run_id, "event": "alerts_prioritized", "alert_count": len(updates)},
            )
        return updates

    def metadata(self) -> dict[str, Any]:
        return {
            "suppress_threshold": self._suppress_threshold,
            "mandatory_review_threshold": self._mandatory_review_threshold,
            "p0_threshold": self._p0_threshold,
            "policy_version": self._policy_engine._config.policy_version,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _regulatory_urgency(frame: pd.DataFrame) -> pd.Series:
        """Compute a [0, 1] regulatory urgency score per row.

        Combines typology risk, jurisdiction risk, and amount thresholds
        into a normalized urgency signal used by the priority formula.
        """
        idx = frame.index
        typology = frame.get("typology", pd.Series("", index=idx)).astype(str).str.lower()
        country = frame.get("country", pd.Series("", index=idx)).astype(str).str.upper()
        amount_raw = frame.get("amount", pd.Series(0.0, index=idx))
        amount = pd.to_numeric(amount_raw, errors="coerce").fillna(0.0)

        typology_score = typology.map({
            "sanctions": 1.0, "terrorism_financing": 1.0,
            "structuring": 0.7, "high_amount_outlier": 0.5,
            "cross_border": 0.4, "flow_through": 0.3,
        }).fillna(0.0)
        country_score = country.map({
            "IR": 1.0, "KP": 1.0, "SY": 0.9,
            "RU": 0.6, "AE": 0.4,
        }).fillna(0.0)
        amount_score = pd.Series(np.where(amount >= 10_000, 0.5, np.where(amount >= 3_000, 0.3, 0.0)), index=idx)

        return np.clip((typology_score * 0.5 + country_score * 0.35 + amount_score * 0.15), 0.0, 1.0)

    @staticmethod
    def _stabilize_model_score_distribution(scores: pd.Series) -> pd.Series:
        series = pd.to_numeric(scores, errors="coerce").fillna(0.0).clip(0.0, 100.0)
        if len(series) <= 1:
            return series
        pct_rank = series.rank(method="average", pct=True).fillna(0.0)
        return np.clip(series.to_numpy() + pct_rank.to_numpy() * 10.0, 0.0, 100.0)

    @staticmethod
    def _build_rule_evidence_row(row: dict[str, Any]) -> dict[str, Any]:
        evidence: dict[str, Any] = {}
        typology = str(row.get("typology") or "").lower()
        country = str(row.get("country") or "").upper()
        try:
            amount = float(row.get("amount", 0.0) or 0.0)
        except Exception:
            amount = 0.0

        if typology in {"sanctions", "structuring", "high_amount_outlier", "cross_border", "flow_through", "terrorism_financing"}:
            evidence["typology_signal"] = {"typology": typology, "weight": 1.0}
        if country in {"IR", "KP", "RU", "AE", "SY"}:
            evidence["geo_risk_signal"] = {"country": country, "weight": 1.0}
        if amount >= 2000.0:
            evidence["high_amount_signal"] = {"amount": amount, "weight": 1.0}
        if float(row.get("regulatory_urgency", 0.0) or 0.0) > 0.3:
            evidence["regulatory_urgency_signal"] = {"urgency": float(row.get("regulatory_urgency", 0.0) or 0.0), "weight": 1.0}
        return evidence

    @staticmethod
    def _is_demo_source(source: str | None) -> bool:
        return str(source or "").strip().lower() in {"synthetic", "bankcsv", "demo", "generated_demo"}

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
