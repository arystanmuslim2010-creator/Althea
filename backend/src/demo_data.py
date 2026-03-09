"""Demo data generation for AML alert prioritization."""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from . import config


def generate_demo_data(
    n_users: int = config.DEMO_DEFAULT_USERS,
    tx_per_user: int = config.DEMO_DEFAULT_TX_PER_USER,
    seed: int = config.DEMO_SEED,
    suspicious_rate: float = config.DEMO_DEFAULT_SUSPICIOUS_RATE,
) -> pd.DataFrame:
    """Generate synthetic transaction data with injected suspicious patterns."""

    rng = np.random.default_rng(seed)

    segments = config.DEMO_SEGMENTS
    seg_probs = np.array(config.DEMO_SEGMENT_PROBS)

    rows: List[list] = []
    for user_id in range(n_users):
        seg_idx = rng.choice(len(segments), p=seg_probs)
        segment, p = segments[seg_idx]

        # user baseline
        base_amount = max(config.DEMO_BASE_AMOUNT_MIN, rng.normal(p["amount_mu"], p["amount_mu"] * 0.15))
        base_gap = max(config.DEMO_BASE_GAP_MIN, rng.normal(p["gap_mu"], p["gap_mu"] * 0.20))

        for _ in range(tx_per_user):
            # normal behavior
            amount = rng.lognormal(mean=np.log(base_amount), sigma=p["amount_sigma"])
            time_gap = abs(rng.normal(base_gap, base_gap * p["gap_sigma"])) + config.DEMO_TIME_GAP_EPS
            num_tx = rng.poisson(lam=p["lam"]) + config.DEMO_NUM_TX_ADD

            rows.append([user_id, segment, amount, time_gap, num_tx, "none", config.RISK_LABEL_NO])

    df = pd.DataFrame(
        rows,
        columns=[
            "user_id",
            "segment",
            "amount",
            "time_gap",
            "num_transactions",
            "typology",
            "synthetic_true_suspicious",
        ],
    )

    # --- Inject suspicious patterns ---
    n_total = len(df)
    n_susp = max(10, int(suspicious_rate * n_total))
    susp_idx = rng.choice(df.index, size=n_susp, replace=False)

    # assign typologies to suspicious
    typologies = config.DEMO_TYPOLOGIES
    typ_probs = np.array(config.DEMO_TYPOLOGY_PROBS)
    chosen_typs = rng.choice(typologies, size=n_susp, p=typ_probs)

    df.loc[susp_idx, "synthetic_true_suspicious"] = config.RISK_LABEL_YES
    df.loc[susp_idx, "typology"] = chosen_typs

    # apply effects
    for idx, typ in zip(susp_idx, chosen_typs):
        if typ == "high_amount_outlier":
            df.at[idx, "amount"] *= rng.integers(
                config.HIGH_AMOUNT_OUTLIER_MULTIPLIER_MIN, config.HIGH_AMOUNT_OUTLIER_MULTIPLIER_MAX
            )
        elif typ == "burst_activity":
            df.at[idx, "num_transactions"] += rng.integers(
                config.BURST_ACTIVITY_TX_ADD_MIN, config.BURST_ACTIVITY_TX_ADD_MAX
            )
            df.at[idx, "time_gap"] *= rng.uniform(
                config.BURST_ACTIVITY_GAP_MULT_MIN, config.BURST_ACTIVITY_GAP_MULT_MAX
            )
        elif typ == "smurfing":
            # many small operations (increase activity but total amount not very large)
            df.at[idx, "num_transactions"] += rng.integers(
                config.SMURFING_TX_ADD_MIN, config.SMURFING_TX_ADD_MAX
            )
            df.at[idx, "amount"] *= rng.uniform(
                config.SMURFING_AMOUNT_MULT_MIN, config.SMURFING_AMOUNT_MULT_MAX
            )
            df.at[idx, "time_gap"] *= rng.uniform(
                config.SMURFING_GAP_MULT_MIN, config.SMURFING_GAP_MULT_MAX
            )
        elif typ == "rapid_velocity":
            df.at[idx, "time_gap"] *= rng.uniform(
                config.RAPID_VELOCITY_GAP_MULT_MIN, config.RAPID_VELOCITY_GAP_MULT_MAX
            )
            df.at[idx, "num_transactions"] += rng.integers(
                config.RAPID_VELOCITY_TX_ADD_MIN, config.RAPID_VELOCITY_TX_ADD_MAX
            )

    # careful rounding
    df["amount"] = df["amount"].clip(lower=config.DEMO_AMOUNT_CLIP_MIN).round(config.DEMO_AMOUNT_ROUND)
    df["time_gap"] = df["time_gap"].clip(lower=config.DEMO_TIME_GAP_CLIP_MIN).round(config.DEMO_TIME_GAP_ROUND)

    return df
