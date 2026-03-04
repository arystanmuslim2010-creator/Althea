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

    out[hit_col] = 0
    out[score_col] = 0.0
    out[evidence_col] = "none"
    out[result_col] = None

    out = out.sort_values(["user_id", "ts"])
    for user, group in out.groupby("user_id", sort=False):
        grp = group.copy()
        grp = grp.set_index("ts")
        inactivity = grp.index.to_series().diff().dt.total_seconds().div(86400.0).fillna(0.0)
        burst_24h = pd.Series(1, index=grp.index).rolling("1D").count().fillna(1)
        hits = (inactivity >= inactive_days) & (burst_24h >= burst_min)
        score = np.minimum(
            1.0,
            0.5 * (inactivity / max(inactive_days, 1)) + 0.5 * (burst_24h / max(burst_min, 1)),
        )
        out.loc[group.index, hit_col] = hits.astype(int).to_numpy()
        out.loc[group.index, score_col] = score.to_numpy()
        evidence = []
        result_list = []
        for i, b, h, sc in zip(inactivity, burst_24h, hits, score):
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
        out.loc[group.index, evidence_col] = evidence
        out.loc[group.index, result_col] = result_list

    return out
