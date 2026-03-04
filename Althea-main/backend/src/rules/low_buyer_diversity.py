from __future__ import annotations

import numpy as np
import pandas as pd

from .base import RuleResult

RULE_ID = "low_buyer_diversity"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "LOW"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    out = df.copy()
    hit_col = "rule_low_buyer_diversity_hit"
    score_col = "rule_low_buyer_diversity_score"
    evidence_col = "rule_low_buyer_diversity_evidence"
    result_col = "rule_low_buyer_diversity_result"

    required = {"user_id", "ts", "segment", "counterparty_id"}
    if not required.issubset(out.columns):
        out[hit_col] = 0
        out[score_col] = 0.0
        out[evidence_col] = "none"
        out[result_col] = None
        return out

    window_days = int(getattr(cfg, "LOW_BUYERS_WINDOW_DAYS", 7))
    tx_min = int(getattr(cfg, "LOW_BUYERS_TX_MIN", 20))
    uniq_max = int(getattr(cfg, "LOW_BUYERS_UNIQUE_MAX", 3))
    thresholds = {"tx_min": tx_min, "uniq_max": uniq_max}
    window = {"days": window_days}

    no_hit = RuleResult(RULE_ID, RULE_VERSION, False, DEFAULT_SEVERITY, 0.0, {}, thresholds, window).to_dict()
    out[hit_col] = 0
    out[score_col] = 0.0
    out[evidence_col] = "none"
    out[result_col] = out.index.map(lambda _: no_hit)

    out = out.sort_values(["user_id", "ts"])
    smb_mask = out["segment"].astype(str) == "smb"
    for user, group in out[smb_mask].groupby("user_id", sort=False):
        grp = group.copy().set_index("ts")
        times = grp.index.to_numpy().astype("datetime64[ns]").astype("int64")
        window_ns = np.int64(window_days * 24 * 60 * 60 * 1e9)
        left_idx = np.searchsorted(times, times - window_ns, side="left")
        cp = grp["counterparty_id"].astype(str).to_numpy()
        tx_count = np.zeros(len(grp), dtype=int)
        uniq = np.zeros(len(grp), dtype=int)
        for i in range(len(grp)):
            left = left_idx[i]
            tx_count[i] = i - left + 1
            uniq[i] = len(np.unique(cp[left : i + 1]))
        hits = (tx_count >= tx_min) & (uniq <= uniq_max)
        score = np.clip(
            (tx_count / max(tx_min, 1)) * ((uniq_max - uniq + 1) / max(uniq_max, 1)),
            0.0,
            1.0,
        )
        out.loc[group.index, hit_col] = hits.astype(int)
        out.loc[group.index, score_col] = score
        evidence = []
        result_list = []
        for u, t, h, sc in zip(uniq, tx_count, hits, score):
            evidence.append(f"buyers_7d={int(u)}; tx_7d={int(t)}" if h else "none")
            result_list.append(
                RuleResult(
                    rule_id=RULE_ID,
                    rule_version=RULE_VERSION,
                    hit=bool(h),
                    severity=DEFAULT_SEVERITY,
                    score=float(sc),
                    evidence={"buyers_7d": int(u), "tx_7d": int(t)} if h else {},
                    thresholds=thresholds,
                    window=window,
                ).to_dict()
            )
        out.loc[group.index, evidence_col] = evidence
        out.loc[group.index, result_col] = result_list

    return out
