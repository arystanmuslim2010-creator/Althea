from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class _Signal:
    reason: str
    pattern: str | None
    focus_point: str
    score: float


class InterpretationService:
    """
    Additive interpretation layer for converting technical explainability payloads
    into analyst-facing AML language.
    """

    SUPPORTED_PATTERNS: set[str] = {
        "Structuring",
        "Layering",
        "Velocity spike",
        "Fan-in",
        "Fan-out",
        "Baseline deviation",
        "High-value anomaly",
    }

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            cast_value = float(value)
            return cast_value if math.isfinite(cast_value) else default
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

        merged: dict[str, dict[str, Any]] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            feature = str(item.get("feature") or item.get("name") or "").strip()
            if not feature:
                continue
            value = self._safe_float(item.get("value", item.get("contribution", item.get("shap_value"))))
            shap_raw = item.get("shap_value", item.get("contribution"))
            shap_value = None if shap_raw is None else self._safe_float(shap_raw, default=0.0)
            magnitude_raw = item.get("magnitude")
            magnitude = self._safe_float(magnitude_raw, default=0.0)
            if magnitude <= 0:
                magnitude = abs(shap_value if shap_value is not None else value)

            current = merged.get(feature)
            row = {
                "feature": feature,
                "value": value,
                "shap_value": shap_value,
                "magnitude": magnitude,
            }
            # Deduplicate duplicated feature rows by strongest magnitude.
            if current is None or float(row["magnitude"]) > float(current.get("magnitude") or 0.0):
                merged[feature] = row

        out = list(merged.values())
        out.sort(key=lambda row: float(row.get("magnitude") or 0.0), reverse=True)
        return out

    def _feature_magnitude(self, contributions: list[dict[str, Any]], feature_name: str) -> float:
        for row in contributions:
            if str(row.get("feature")) == feature_name:
                return self._safe_float(row.get("magnitude"), default=0.0)
        return 0.0

    def _signal_velocity(self, features: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        time_gap = self._safe_float(features.get("time_gap"), default=-1.0)
        magnitude = self._feature_magnitude(contributions, "time_gap")
        if (0 <= time_gap <= 900) or (magnitude >= 0.12):
            return _Signal(
                reason="Rapid transaction burst in a short period is consistent with unusual velocity and warrants review.",
                pattern="Velocity spike",
                focus_point="Check whether transactions accelerated abruptly compared with expected account cadence.",
                score=max(0.70, min(0.98, 0.70 + magnitude)),
            )
        return None

    def _signal_high_value(self, features: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        amount = self._safe_float(features.get("amount"), default=0.0)
        amount_log = self._safe_float(features.get("amount_log1p"), default=0.0)
        magnitude = max(
            self._feature_magnitude(contributions, "amount_log1p"),
            self._feature_magnitude(contributions, "amount"),
        )
        if amount >= 10000 or amount_log >= 9.0 or magnitude >= 0.14:
            return _Signal(
                reason="Transaction value is materially elevated and may indicate a high-value movement anomaly.",
                pattern="High-value anomaly",
                focus_point="Validate source-of-funds and whether the transaction size matches expected customer behavior.",
                score=max(0.66, min(0.95, 0.66 + magnitude)),
            )
        return None

    def _signal_baseline(self, features: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        amount = self._safe_float(features.get("amount"), default=0.0)
        historical_mean = self._safe_float(features.get("user_amount_mean"), default=0.0)
        historical_std = max(self._safe_float(features.get("user_amount_std"), default=0.0), 1e-6)
        magnitude = self._feature_magnitude(contributions, "user_amount_mean")
        ratio = (amount / historical_mean) if historical_mean > 0 else 0.0
        z_like = ((amount - historical_mean) / historical_std) if historical_mean > 0 else 0.0

        if ratio >= 1.8 or abs(z_like) >= 2.0 or magnitude >= 0.12:
            return _Signal(
                reason="Current transaction activity deviates from expected baseline behavior for this account.",
                pattern="Baseline deviation",
                focus_point="Compare recent activity against historical baseline and documented customer profile.",
                score=max(0.67, min(0.95, 0.67 + magnitude)),
            )
        return None

    def _signal_structuring(self, features: dict[str, Any], contributions: list[dict[str, Any]]) -> _Signal | None:
        tx_count = self._safe_float(features.get("num_transactions"), default=0.0)
        std_amount = self._safe_float(features.get("user_amount_std"), default=0.0)
        magnitude = self._feature_magnitude(contributions, "user_amount_std")
        if (tx_count >= 6 and std_amount > 0) or magnitude >= 0.14:
            return _Signal(
                reason="Irregular transaction amount distribution may indicate structuring and warrants review.",
                pattern="Structuring",
                focus_point="Assess whether amounts appear intentionally fragmented across multiple transfers.",
                score=max(0.68, min(0.96, 0.68 + magnitude)),
            )
        return None

    def _signal_counterparty(self, features: dict[str, Any], contributions: list[dict[str, Any]]) -> list[_Signal]:
        incoming = int(self._safe_float(features.get("incoming_counterparty_count"), default=0.0))
        outgoing = int(self._safe_float(features.get("outgoing_counterparty_count"), default=0.0))
        repeated_pairs = int(self._safe_float(features.get("repeated_pairs_count"), default=0.0))
        concentration = self._safe_float(features.get("counterparty_concentration"), default=0.0)
        concentration_mag = max(
            self._feature_magnitude(contributions, "counterparty_concentration"),
            self._feature_magnitude(contributions, "repeated_pairs_count"),
        )
        signals: list[_Signal] = []

        if repeated_pairs >= 3 or concentration >= 0.7 or concentration_mag >= 0.12:
            signals.append(
                _Signal(
                    reason=(
                        "Funds may be concentrated across a narrow set of counterparties, "
                        "which suggests network concentration risk."
                    ),
                    pattern="Fan-in" if incoming >= outgoing else "Fan-out",
                    focus_point=(
                        "Review whether multiple counterparties are funneling funds into one account."
                        if incoming >= outgoing
                        else "Review whether funds are dispersed quickly to multiple counterparties."
                    ),
                    score=max(0.64, min(0.92, 0.64 + concentration_mag)),
                )
            )
        elif incoming >= 3 and outgoing <= 1:
            signals.append(
                _Signal(
                    reason="Multiple incoming counterparties may indicate fan-in behavior and warrants review.",
                    pattern="Fan-in",
                    focus_point="Review whether several sources are routing funds into one destination account.",
                    score=0.69,
                )
            )
        elif outgoing >= 3 and incoming <= 1:
            signals.append(
                _Signal(
                    reason="Distribution to multiple outgoing counterparties may indicate fan-out behavior.",
                    pattern="Fan-out",
                    focus_point="Assess whether outgoing distribution happened rapidly after incoming activity.",
                    score=0.69,
                )
            )

        return signals

    def _signal_layering(self, features: dict[str, Any]) -> _Signal | None:
        in_out_delta = self._safe_float(features.get("incoming_outgoing_time_delta_seconds"), default=-1.0)
        sequence_flag = bool(features.get("has_incoming_and_outgoing_sequence"))
        if (0 <= in_out_delta <= 3600) or sequence_flag:
            return _Signal(
                reason="Funds appear to move onward quickly after receipt, which is consistent with potential layering.",
                pattern="Layering",
                focus_point="Check whether incoming funds were redistributed quickly without clear economic rationale.",
                score=0.74,
            )
        return None

    def _fallback_signal(self, contributions: list[dict[str, Any]]) -> _Signal:
        if contributions:
            top_feature = str(contributions[0].get("feature") or "model-selected driver")
            return _Signal(
                reason=(
                    f"Model output highlights {top_feature} as a material risk driver, "
                    "which suggests atypical activity and warrants review."
                ),
                pattern=None,
                focus_point="Review transaction context and customer profile before assigning typology.",
                score=0.58,
            )
        return _Signal(
            reason="Model signals suggest atypical activity that warrants additional analyst review.",
            pattern=None,
            focus_point="Review available account history, counterparties, and transaction context.",
            score=0.52,
        )

    def _confidence_score(
        self,
        raw_explain_payload: dict[str, Any],
        signals: list[_Signal],
        contributions: list[dict[str, Any]],
    ) -> float | None:
        base_prob = raw_explain_payload.get("base_prob", raw_explain_payload.get("risk_prob"))
        risk_score = self._safe_float(raw_explain_payload.get("risk_score"), default=0.0)
        if base_prob is None and risk_score <= 0 and not contributions:
            return None

        base = self._safe_float(base_prob, default=risk_score / 100.0 if risk_score > 0 else 0.55)
        base = min(max(base, 0.35), 0.95)
        signal_component = min(sum(max(0.0, signal.score - 0.50) for signal in signals[:3]) * 0.08, 0.20)
        contribution_component = min(
            sum(min(self._safe_float(row.get("magnitude"), default=0.0), 0.20) for row in contributions[:3]) * 0.15,
            0.10,
        )

        method = str(raw_explain_payload.get("explanation_method") or "").strip().lower()
        status = str(raw_explain_payload.get("explanation_status") or "").strip().lower()
        penalty = 0.0
        if method in {"numeric_fallback", "unknown", "unavailable"}:
            penalty += 0.04
        if status in {"fallback", "unknown", "unavailable"}:
            penalty += 0.05
        score = base + signal_component + contribution_component - penalty
        return round(min(max(score, 0.35), 0.99), 2)

    @staticmethod
    def _dedupe_signals(signals: list[_Signal]) -> list[_Signal]:
        deduped: list[_Signal] = []
        seen: set[tuple[str, str | None]] = set()
        for signal in sorted(signals, key=lambda item: item.score, reverse=True):
            key = (signal.reason, signal.pattern)
            if key in seen:
                continue
            deduped.append(signal)
            seen.add(key)
        return deduped

    def build_human_explanation(
        self,
        raw_explain_payload: dict[str, Any],
        feature_dict: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_payload = dict(raw_explain_payload or {})
        safe_features = dict(feature_dict or {})
        contributions = self._normalize_contributions(safe_payload)

        signals: list[_Signal] = []
        for detector in (
            self._signal_velocity,
            self._signal_high_value,
            self._signal_baseline,
            self._signal_structuring,
        ):
            maybe = detector(safe_features, contributions)
            if maybe is not None:
                signals.append(maybe)
        signals.extend(self._signal_counterparty(safe_features, contributions))
        layering = self._signal_layering(safe_features)
        if layering is not None:
            signals.append(layering)
        if not signals:
            signals.append(self._fallback_signal(contributions))

        ranked = self._dedupe_signals(signals)
        reason_cap = 5
        focus_cap = 5
        pattern_cap = 3

        key_reasons = [item.reason for item in ranked[:reason_cap]]
        analyst_focus_points = [item.focus_point for item in ranked[:focus_cap]]

        aml_patterns: list[str] = []
        for item in ranked:
            if not item.pattern or item.pattern not in self.SUPPORTED_PATTERNS:
                continue
            if item.pattern not in aml_patterns:
                aml_patterns.append(item.pattern)
            if len(aml_patterns) >= pattern_cap:
                break

        summary_reasons = key_reasons[:3]
        if summary_reasons:
            compact = []
            for text in summary_reasons:
                compact_text = text.rstrip(".")
                compact.append(compact_text[0].lower() + compact_text[1:] if compact_text else compact_text)
            summary_text = (
                "This alert is prioritized because it shows "
                + "; ".join(compact)
                + ". These indicators may indicate atypical activity and warrants review."
            )
        else:
            summary_text = (
                "This alert is prioritized because model signals suggest atypical activity that warrants review."
            )

        return {
            "summary_text": summary_text,
            "key_reasons": key_reasons[:5],
            "aml_patterns": aml_patterns[:3],
            "analyst_focus_points": analyst_focus_points[:5],
            "confidence_score": self._confidence_score(safe_payload, ranked, contributions),
            "technical_details": safe_payload,
        }
