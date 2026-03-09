"""Explainability driver generation for AML alerts."""
from __future__ import annotations

import json
from typing import Callable, Optional

import numpy as np
import pandas as pd

from . import config


def generate_explainability_drivers(
    df: pd.DataFrame,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> pd.DataFrame:
    """Compute numeric driver strings for alert explainability."""

    reasons = []
    top_drivers = []
    reason_codes_list = []
    driver_list_json = []
    evidence_json_list = []
    total_rows = len(df)
    rz_threshold = config.EXPLAIN_Z_THRESHOLD
    ratio_threshold = config.EXPLAIN_RATIO_THRESHOLD
    peer_threshold = 0.95

    for idx, row in enumerate(df.itertuples(index=False)):
        drivers = []
        reason_codes = []

        amount_dev = getattr(row, "amount_dev", 0.0)
        velocity_dev = getattr(row, "velocity_dev", 0.0)
        activity_dev = getattr(row, "activity_dev", 0.0)
        amount_z = getattr(row, "amount_z", 0.0)
        velocity_z = getattr(row, "velocity_z", 0.0)
        activity_z = getattr(row, "activity_z", 0.0)
        burst_score = getattr(row, "burst_score", 0.0)
        drift_amount = getattr(row, "drift_amount", 0.0)
        drift_velocity = getattr(row, "drift_velocity", 0.0)
        amount_seg_pct = getattr(row, "amount_seg_pct", 0.0)
        velocity_seg_pct = getattr(row, "velocity_seg_pct", 0.0)
        velocity_change_rate = getattr(row, "velocity_change_rate", 0.0)
        baseline_level = getattr(row, "baseline_level", "user")
        baseline_window_days = int(getattr(row, "baseline_window_days", 0))
        n_hist = int(getattr(row, "n_hist", 0))

        layer_scores = {
            "behavioral": float(getattr(row, "risk_behavioral", 0.0)),
            "structural": float(getattr(row, "risk_structural", 0.0)),
            "temporal": float(getattr(row, "risk_temporal", 0.0)),
            "meta": float(getattr(row, "risk_meta", 0.0)),
        }
        top_layer = max(layer_scores, key=layer_scores.get)
        if layer_scores[top_layer] > 0.6:
            if top_layer == "behavioral":
                reason_codes.append("BEHAVIORAL_DOMINANT")
            elif top_layer == "structural":
                reason_codes.append("STRUCTURAL_DOMINANT")
            elif top_layer == "temporal":
                reason_codes.append("TEMPORAL_DOMINANT")
            else:
                reason_codes.append("META_DOMINANT")

        if amount_dev >= ratio_threshold:
            reason_codes.append("AMT_DEV_HIGH")
        if velocity_dev >= ratio_threshold:
            reason_codes.append("VEL_DEV_HIGH")
        if activity_dev >= ratio_threshold:
            reason_codes.append("ACT_DEV_HIGH")
        if abs(amount_z) >= rz_threshold:
            reason_codes.append("AMT_Z_HIGH")
        if abs(velocity_z) >= rz_threshold:
            reason_codes.append("VEL_Z_HIGH")
        if abs(activity_z) >= rz_threshold:
            reason_codes.append("ACT_Z_HIGH")

        if burst_score >= ratio_threshold:
            drivers.append(f"burst_score={burst_score:.1f}x")
        if drift_amount >= ratio_threshold:
            drivers.append(f"drift_amount={drift_amount:.1f}x")
        if drift_velocity >= ratio_threshold:
            drivers.append(f"drift_velocity={drift_velocity:.1f}x")
        if velocity_change_rate >= ratio_threshold:
            drivers.append(f"velocity_change_rate={velocity_change_rate:.1f}x")

        if amount_seg_pct >= peer_threshold:
            drivers.append(f"peer_amount_pct={amount_seg_pct:.2f}")
        if velocity_seg_pct >= peer_threshold:
            drivers.append(f"peer_velocity_pct={velocity_seg_pct:.2f}")

        if baseline_level == "user":
            baseline_conf = min(1.0, n_hist / 50.0)
        elif baseline_level == "segment":
            baseline_conf = min(0.8, n_hist / 200.0)
        else:
            baseline_conf = 0.3
        if baseline_level != "user" or baseline_conf < 0.5:
            reason_codes.append("BASELINE_WEAK")

        typology = getattr(row, "typology", "none")
        if isinstance(typology, str):
            typ = typology.lower()
            if typ in ("smurfing", "structuring"):
                reason_codes.append("TYP_STRUCTURING")
            elif typ == "burst_activity":
                reason_codes.append("TYP_BURST")
            elif typ == "rapid_velocity":
                reason_codes.append("TYP_RAPID")
            elif typ == "high_amount_outlier":
                reason_codes.append("TYP_AMOUNT")

        z_drivers = []
        if abs(amount_z) >= rz_threshold:
            z_drivers.append(("amount_z", abs(amount_z), f"amount_z={amount_z:.1f}"))
        if abs(velocity_z) >= rz_threshold:
            z_drivers.append(("velocity_z", abs(velocity_z), f"velocity_z={velocity_z:.1f}"))
        if abs(activity_z) >= rz_threshold:
            z_drivers.append(("activity_z", abs(activity_z), f"activity_z={activity_z:.1f}"))
        z_drivers.sort(key=lambda x: x[1], reverse=True)

        dev_drivers = []
        if amount_dev >= ratio_threshold:
            dev_drivers.append(("amount_dev", amount_dev, f"amount_dev={amount_dev:.1f}x"))
        if velocity_dev >= ratio_threshold:
            dev_drivers.append(("velocity_dev", velocity_dev, f"velocity_dev={velocity_dev:.1f}x"))
        if activity_dev >= ratio_threshold:
            dev_drivers.append(("activity_dev", activity_dev, f"activity_dev={activity_dev:.1f}x"))
        dev_drivers.sort(key=lambda x: x[1], reverse=True)

        ordered_drivers = [d[2] for d in z_drivers] + [d[2] for d in dev_drivers]
        if top_layer and layer_scores[top_layer] > 0.6:
            ordered_drivers.append(f"{top_layer}_dominant")
        if baseline_level != "user":
            ordered_drivers.append(f"baseline={baseline_level}")

        if not ordered_drivers:
            ordered_drivers.append("within_baseline")

        # PART 5: Add temporal behavior drivers
        drift_amount = getattr(row, "drift_amount", 1.0)
        burst_ratio = getattr(row, "burst_ratio", 1.0)
        velocity_acceleration = getattr(row, "velocity_acceleration", 1.0)
        volatility_ratio = getattr(row, "volatility_ratio", 1.0)
        risk_regime_shift = getattr(row, "risk_regime_shift", 1.0)
        dormancy_flag = getattr(row, "dormancy_flag", 0)
        
        if drift_amount > 2.0:
            ordered_drivers.append("behavior_drift")
        if burst_ratio > 2.0:
            ordered_drivers.append("activity_burst")
        if velocity_acceleration > 2.0:
            ordered_drivers.append("velocity_spike")
        if volatility_ratio > 2.0:
            ordered_drivers.append("volatility_shift")
        if risk_regime_shift > 2.0:
            ordered_drivers.append("risk_regime_change")
        if dormancy_flag == 1:
            ordered_drivers.append("dormant_account_activation")

        # PART 6: Check for rule triggers (R001-R005)
        rule_triggers = []
        rule_ids = ["R001", "R002", "R003", "R004", "R005"]
        for rule_id in rule_ids:
            hit_col = f"rule_{rule_id}_hit"
            if hasattr(row, hit_col):
                hit_val = getattr(row, hit_col, 0)
                if int(hit_val) == 1:
                    rule_triggers.append(rule_id)
        
        # Append rule triggers to drivers if they exist
        if rule_triggers:
            rule_str = f"Rule triggers: {', '.join(rule_triggers)}"
            ordered_drivers.insert(0, rule_str)
        
        # PART D: If rule_score > 0.7 → Top_Driver = typology
        rule_score = float(getattr(row, "rule_score", 0.0))
        if rule_score > 0.7 and typology and typology.lower() != "none" and typology.lower() != "behavioral_anomaly":
            top_driver = typology.upper()
        elif rule_triggers:
            # If rule triggers exist, use first triggered rule as top driver
            top_driver = f"RULE_{rule_triggers[0]}"
        else:
            top_driver = ordered_drivers[0]

        reason = "; ".join(ordered_drivers[:3])
        reasons.append(reason)
        top_drivers.append(top_driver)

        # Collect rule evidence if rules fired
        rule_evidence = {}
        rule_ids = ["R001", "R002", "R003", "R004", "R005"]
        for rule_id in rule_ids:
            hit_col = f"rule_{rule_id}_hit"
            evidence_col = f"rule_{rule_id}_evidence"
            if hasattr(row, hit_col) and getattr(row, hit_col, 0) == 1:
                try:
                    evidence_json = getattr(row, evidence_col, "{}")
                    if isinstance(evidence_json, str) and evidence_json != "{}" and evidence_json != "none":
                        rule_evidence[rule_id] = json.loads(evidence_json)
                except Exception:
                    pass
        
        evidence = {
            "alert_id": getattr(row, "alert_id", None),
            "user_id": getattr(row, "user_id", None),
            "segment": getattr(row, "segment", None),
            "typology": getattr(row, "typology", None),
            "risk_score": float(getattr(row, "risk_score", 0.0)),
            "risk_components": {
                "behavioral": layer_scores["behavioral"],
                "structural": layer_scores["structural"],
                "temporal": layer_scores["temporal"],
                "meta": layer_scores["meta"],
            },
            "baseline": {
                "level": baseline_level,
                "window_days": baseline_window_days,
                "n_hist": n_hist,
                "confidence": baseline_conf,
            },
            "deviations": {
                "amount_dev": float(amount_dev),
                "velocity_dev": float(velocity_dev),
                "activity_dev": float(activity_dev),
                "amount_z": float(amount_z),
                "velocity_z": float(velocity_z),
                "activity_z": float(activity_z),
            },
            "drivers": ordered_drivers,
            "reason_codes": reason_codes,
        }
        
        # Add rule triggers and evidence if present
        if rule_triggers:
            evidence["rule_triggers"] = rule_triggers
        if rule_evidence:
            evidence["rule_evidence"] = rule_evidence

        driver_list_json.append(json.dumps(ordered_drivers, default=str))
        evidence_json_list.append(json.dumps(evidence, default=str))
        reason_codes_list.append(json.dumps(reason_codes, default=str))

        if progress_cb and idx % max(1, total_rows // 20) == 0:
            progress_cb(min(100, int((idx + 1) / total_rows * 100)))

    baseline_confidence = []
    baseline_level = df["baseline_level"] if "baseline_level" in df.columns else pd.Series("global", index=df.index)
    n_hist = df["n_hist"] if "n_hist" in df.columns else pd.Series(0.0, index=df.index)
    n_hist = pd.to_numeric(n_hist, errors="coerce").fillna(0)
    for lvl, n in zip(baseline_level, n_hist):
        if lvl == "user":
            baseline_confidence.append(min(1.0, float(n) / 50.0))
        elif lvl == "segment":
            baseline_confidence.append(min(0.8, float(n) / 200.0))
        else:
            baseline_confidence.append(0.3)

    df["baseline_confidence"] = pd.Series(baseline_confidence, index=df.index)
    df["Reason"] = reasons
    df["Top_Driver"] = top_drivers
    df["reason_codes"] = reason_codes_list
    df["driver_list"] = driver_list_json
    df["evidence_json"] = evidence_json_list

    if progress_cb:
        progress_cb(100)

    return df
