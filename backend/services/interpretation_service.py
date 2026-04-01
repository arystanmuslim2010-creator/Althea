from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class _Signal:
    key_reason: str
    aml_pattern: str
    focus_point: str
    weight: float = 0.08


class InterpretationService:
    """
    Translate technical model outputs into conservative AML analyst-facing language.
    """

    FEATURE_REASON_MAP: dict[str, str] = {
        "time_gap": "Transaction timing appears compressed versus expected cadence, which may indicate unusual velocity.",
        "amount_log1p": "Transaction value appears elevated and is consistent with higher-value movement.",
        "amount": "Transaction value appears elevated relative to expected activity.",
        "user_amount_mean": "Activity may deviate from the account's historical baseline.",
        "user_amount_std": "Amount variation appears irregular and may indicate structuring behavior.",
        "num_transactions": "Transaction count appears concentrated in a short window and warrants review.",
        "counterparty_concentration": "Counterparty concentration suggests funds may be moving through a narrow network.",
        "incoming_counterparty_count": "Multiple incoming counterparties may indicate fan-in behavior.",
        "outgoing_counterparty_count": "Distribution to multiple outgoing counterparties may indicate fan-out behavior.",
    }

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            if not math.isfinite(out):
                return default
            return out
        except Exception:
            return default

    def _normalize_contributions(self, raw_explain_payload: dict[str, Any]) -> list[dict[str, Any]]:
        raw = (
            raw_explain_payload.get("feature_attribution")
            or raw_explain_payload.get("contributions")
            or raw_explain_payload.get("top_feature_contributions")
            or []
        )
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            feature = str(item.get("feature") or item.get("name") or "").strip()
            if not feature:
                continue
            value = self._safe_float(item.get("value", item.get("contribution", item.get("shap_value"))))
            shap_value = item.get("shap_value", item.get("contribution"))
            shap = None if shap_value is None else self._safe_float(shap_value, default=0.0)
            magnitude = self._safe_float(item.get("magnitude"), default=0.0)
            if magnitude <= 0:
                base = shap if shap is not None else value
                magnitude = abs(self._safe_float(base))
            out.append(
                {
                    "feature": feature,
                    "value": value,
                    "shap_value": shap,
                    "magnitude": magnitude,
                }
            )
        out.sort(key=lambda row: float(row.get("magnitude") or 0.0), reverse=True)
        return out

    def _signal_from_velocity(self, feature_dict: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        time_gap = self._safe_float(feature_dict.get("time_gap"), default=-1)
        contrib_map = {str(item.get("feature")): item for item in contributions}
        time_gap_contrib = self._safe_float((contrib_map.get("time_gap") or {}).get("magnitude"), default=0.0)
        if (0 <= time_gap <= 900) or (time_gap_contrib >= 0.15):
            return _Signal(
                key_reason="Rapid transaction activity is consistent with an unusual velocity spike.",
                aml_pattern="Possible velocity spike",
                focus_point="Review transaction sequence timing to confirm whether activity accelerated abruptly.",
                weight=0.10,
            )
        return None

    def _signal_from_high_value(self, feature_dict: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        amount = self._safe_float(feature_dict.get("amount"), default=0.0)
        amount_log = self._safe_float(feature_dict.get("amount_log1p"), default=0.0)
        contrib_map = {str(item.get("feature")): item for item in contributions}
        amount_contrib = self._safe_float((contrib_map.get("amount_log1p") or contrib_map.get("amount") or {}).get("magnitude"))
        if amount >= 10000 or amount_log >= 9.0 or amount_contrib >= 0.2:
            return _Signal(
                key_reason="Transaction value appears elevated and may indicate high-value movement.",
                aml_pattern="High-value movement",
                focus_point="Validate source-of-funds and expected value range for this account profile.",
                weight=0.09,
            )
        return None

    def _signal_from_baseline_deviation(self, feature_dict: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        amount = self._safe_float(feature_dict.get("amount"), default=0.0)
        mean_amount = self._safe_float(feature_dict.get("user_amount_mean"), default=0.0)
        std_amount = max(self._safe_float(feature_dict.get("user_amount_std"), default=0.0), 1e-6)
        contrib_map = {str(item.get("feature")): item for item in contributions}
        mean_contrib = self._safe_float((contrib_map.get("user_amount_mean") or {}).get("magnitude"))

        ratio = (amount / mean_amount) if mean_amount > 0 else 0.0
        z_score = ((amount - mean_amount) / std_amount) if mean_amount > 0 else 0.0
        if ratio >= 1.8 or abs(z_score) >= 2.0 or mean_contrib >= 0.15:
            return _Signal(
                key_reason="Transaction values appear inconsistent with the account's historical baseline.",
                aml_pattern="Deviation from historical baseline",
                focus_point="Compare current amounts against historical account activity and expected customer behavior.",
                weight=0.10,
            )
        return None

    def _signal_from_structuring(self, feature_dict: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        tx_count = self._safe_float(feature_dict.get("num_transactions"), default=0.0)
        std_amount = self._safe_float(feature_dict.get("user_amount_std"), default=0.0)
        contrib_map = {str(item.get("feature")): item for item in contributions}
        std_contrib = self._safe_float((contrib_map.get("user_amount_std") or {}).get("magnitude"))
        if (tx_count >= 6 and std_amount > 0) or std_contrib >= 0.15:
            return _Signal(
                key_reason="Amount dispersion and transaction cadence may indicate structuring behavior.",
                aml_pattern="Possible structuring",
                focus_point="Check for repeated near-threshold amounts or fragmented transfers across short intervals.",
                weight=0.08,
            )
        return None

    def _signal_from_fan(self, feature_dict: dict[str, Any]) -> list[_Signal]:
        signals: list[_Signal] = []
        incoming = int(self._safe_float(feature_dict.get("incoming_counterparty_count"), default=0.0))
        outgoing = int(self._safe_float(feature_dict.get("outgoing_counterparty_count"), default=0.0))
        in_degree = int(self._safe_float(feature_dict.get("graph_in_degree"), default=0.0))
        out_degree = int(self._safe_float(feature_dict.get("graph_out_degree"), default=0.0))

        incoming = max(incoming, in_degree)
        outgoing = max(outgoing, out_degree)

        if incoming >= 3 and outgoing <= 1:
            signals.append(
                _Signal(
                    key_reason="Multiple incoming counterparties suggest potential fan-in concentration.",
                    aml_pattern="Possible fan-in",
                    focus_point="Review whether funds from several sources are converging into one destination account.",
                    weight=0.08,
                )
            )
        if outgoing >= 3 and incoming <= 1:
            signals.append(
                _Signal(
                    key_reason="Distribution to multiple outgoing counterparties suggests potential fan-out behavior.",
                    aml_pattern="Possible fan-out",
                    focus_point="Assess whether funds are being dispersed rapidly after receipt.",
                    weight=0.08,
                )
            )
        return signals

    def _signal_from_layering(self, feature_dict: dict[str, Any]) -> _Signal | None:
        in_out_delta = self._safe_float(feature_dict.get("incoming_outgoing_time_delta_seconds"), default=-1.0)
        has_flow_flag = bool(feature_dict.get("has_incoming_and_outgoing_sequence"))
        if (0 <= in_out_delta <= 3600) or has_flow_flag:
            return _Signal(
                key_reason="Short incoming-to-outgoing sequencing is consistent with potential layering behavior.",
                aml_pattern="Potential layering pattern",
                focus_point="Check whether incoming funds were redistributed quickly with limited business rationale.",
                weight=0.09,
            )
        return None

    def _fallback_signal(self, contributions: list[dict[str, Any]]) -> _Signal:
        if contributions:
            top = contributions[0]
            feature = str(top.get("feature") or "activity pattern")
            mapped = self.FEATURE_REASON_MAP.get(feature, f"Model highlighted {feature} as a notable risk driver.")
            return _Signal(
                key_reason=f"{mapped} This may indicate atypical behavior and warrants review.",
                aml_pattern="Atypical transactional behavior",
                focus_point="Review account activity context and compare with known customer profile.",
                weight=0.06,
            )
        return _Signal(
            key_reason="Model signals suggest atypical activity that warrants analyst review.",
            aml_pattern="Atypical transactional behavior",
            focus_point="Review transaction context and account history before concluding risk typology.",
            weight=0.05,
        )

    def _confidence_score(
        self,
        raw_explain_payload: dict[str, Any],
        signals: list[_Signal],
        contributions: list[dict[str, Any]],
    ) -> float:
        base_prob = raw_explain_payload.get("base_prob", raw_explain_payload.get("risk_prob"))
        risk_score = self._safe_float(raw_explain_payload.get("risk_score"), default=0.0)
        base = self._safe_float(base_prob, default=risk_score / 100.0 if risk_score > 0 else 0.55)
        base = min(max(base, 0.35), 0.95)

        contribution_strength = 0.0
        for row in contributions[:3]:
            contribution_strength += min(self._safe_float(row.get("magnitude"), default=0.0), 0.20)
        contribution_strength = min(contribution_strength * 0.15, 0.12)

        signal_boost = min(sum(signal.weight for signal in signals), 0.22)
        status = str(raw_explain_payload.get("explanation_status") or "").strip().lower()
        method = str(raw_explain_payload.get("explanation_method") or "").strip().lower()
        penalty = 0.0
        if status in {"fallback", "unknown", "unavailable"}:
            penalty += 0.06
        if method in {"numeric_fallback", "unknown", "unavailable"}:
            penalty += 0.04
        score = base + signal_boost + contribution_strength - penalty
        return round(min(max(score, 0.35), 0.99), 2)

    def build_human_explanation(self, raw_explain_payload: dict[str, Any], feature_dict: dict[str, Any]) -> dict[str, Any]:
        safe_payload = dict(raw_explain_payload or {})
        safe_features = dict(feature_dict or {})
        contributions = self._normalize_contributions(safe_payload)

        signals: list[_Signal] = []
        for detector in (
            self._signal_from_velocity,
            self._signal_from_high_value,
            self._signal_from_baseline_deviation,
            self._signal_from_structuring,
        ):
            signal = detector(safe_features, contributions)
            if signal is not None:
                signals.append(signal)

        signals.extend(self._signal_from_fan(safe_features))
        layering_signal = self._signal_from_layering(safe_features)
        if layering_signal is not None:
            signals.append(layering_signal)

        if not signals:
            signals.append(self._fallback_signal(contributions))

        # Keep distinct entries while preserving order.
        key_reasons: list[str] = []
        aml_patterns: list[str] = []
        focus_points: list[str] = []
        for signal in signals:
            if signal.key_reason not in key_reasons:
                key_reasons.append(signal.key_reason)
            if signal.aml_pattern not in aml_patterns:
                aml_patterns.append(signal.aml_pattern)
            if signal.focus_point not in focus_points:
                focus_points.append(signal.focus_point)

        summary_candidates = key_reasons[:2]
        if summary_candidates:
            joined = " and ".join(
                reason[0].lower() + reason[1:] if reason else reason for reason in summary_candidates
            )
            summary_text = (
                f"This alert is prioritized because it is consistent with {joined}. "
                "These indicators may indicate atypical activity and warrant analyst review."
            )
        else:
            summary_text = (
                "This alert is prioritized because model signals suggest atypical activity that warrants review."
            )

        return {
            "summary_text": summary_text,
            "key_reasons": key_reasons,
            "aml_patterns": aml_patterns,
            "analyst_focus_points": focus_points,
            "confidence_score": self._confidence_score(safe_payload, signals, contributions),
            "technical_details": safe_payload,
        }

