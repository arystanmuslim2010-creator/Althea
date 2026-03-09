from __future__ import annotations

import numpy as np
import pandas as pd

from .base import RuleResult

RULE_ID = "dormant"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "MEDIUM"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    out = df.copy()
    hit_col = "rule_dormant_hit"
    score_col = "rule_dormant_score"
    evidence_col = "rule_dormant_evidence"
    result_col = "rule_dormant_result"

    required = {"user_id", "ts"}
    if not required.issubset(out.columns):
        out[hit_col] = 0
        out[score_col] = 0.0
        out[evidence_col] = "none"
        out[result_col] = None
        return out

    inactive_days = float(getattr(cfg, "DORMANT_INACTIVE_DAYS", 30))
    burst_min = int(getattr(cfg, "DORMANT_BURST_MIN", 8))
    thresholds = {"inactive_days_thr": inactive_days, "burst_min": burst_min}
    window = {"days": 1, "inactive_days": inactive_days}

    out = out.sort_values(["user_id", "ts"], kind="mergesort").copy()
    out[hit_col] = 0
    out[score_col] = 0.0
    out[evidence_col] = "none"
    out[result_col] = None

    # Set DatetimeIndex for time-based rolling
    out_ts = out.set_index("ts")

    # Inactivity gap per user via groupby diff
    inactivity = (
        out_ts.groupby("user_id", sort=False)["user_id"]
        .transform(lambda s: s.index.to_series().diff().dt.total_seconds().div(86400.0).fillna(0.0))
    )

    # 24h burst count per user via groupby rolling
    out_ts["_one"] = 1.0
    burst_24h = (
        out_ts.groupby("user_id", sort=False)["_one"]
        .rolling("1D")
        .count()
        .fillna(1)
        .reset_index(level=0, drop=True)
    )

    # Forward-propagate dormancy signal using backward rolling max over 1D:
    # A row at time R is "dormancy_within" if any trigger (inactivity >= threshold)
    # occurred in the window [R-1D, R] — equivalent to forward propagation of trigger for 24h.
    dormancy_trigger = (inactivity >= inactive_days).astype(float)
    out_ts["_trigger"] = dormancy_trigger.values
    dormancy_rolling = (
        out_ts.groupby("user_id", sort=False)["_trigger"]
        .rolling("1D")
        .max()
        .fillna(0)
        .reset_index(level=0, drop=True)
    )
    dormancy_within = dormancy_rolling > 0

    hits = dormancy_within & (burst_24h >= burst_min)
    score = np.minimum(
        1.0,
        0.5 * (inactivity.to_numpy() / max(inactive_days, 1))
        + 0.5 * (burst_24h.to_numpy() / max(burst_min, 1)),
    )

    out[hit_col] = hits.astype(int).to_numpy()
    out[score_col] = score

    evidence = []
    result_list = []
    for i, b, h, sc in zip(
        inactivity.to_numpy(), burst_24h.to_numpy(), hits.to_numpy(), score
    ):
        evidence.append(f"inactive_days={int(i)}; burst_24h={int(b)}" if h else "none")
        result_list.append(
            RuleResult(
                rule_id=RULE_ID,
                rule_version=RULE_VERSION,
                hit=bool(h),
                severity=DEFAULT_SEVERITY,
                score=float(sc),
                evidence={"inactive_days": float(i), "burst_24h": int(b)} if h else {},
                thresholds=thresholds,
                window=window,
            ).to_dict()
        )
    out[evidence_col] = evidence
    out[result_col] = result_list

    return out
