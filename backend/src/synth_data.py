"""
Synthetic alert data generator for AML overlay demonstration.

⚠️  SYNTHETIC LABELS — NOT PRODUCTION GROUND TRUTH ⚠️
=======================================================
The column ``synthetic_true_suspicious`` is generated **programmatically** from
rule-based heuristics (amount outliers, burst activity, smurfing patterns, etc.).
It is **NOT** derived from real SAR filings, regulatory outcomes, or analyst
dispositions.

Implications for reported metrics:
- All precision, recall, and AUC values reflect *synthetic label consistency*,
  meaning how well the ML model recovers the same rule-generated labels it was
  indirectly trained to replicate.
- These numbers do **not** represent real-world detection performance against
  actual money-laundering activity.

Production deployment path (label evolution):
1. **Synthetic labels** (current — demo/pilot only): rule-generated flags used
   for initial model training and UI validation.
2. **Analyst disposition labels**: analysts mark each alert as TP / FP after
   investigation.  These become the retraining labels once a sufficient number
   of closed cases accumulates (MIN_CLOSED_CASES_FOR_METRICS in config.py).
3. **SAR outcome labels**: alerts that resulted in a filed SAR provide the
   strongest signal.  Feed these back via the OUTCOME_COLUMN pipeline.

Do not use metrics computed from synthetic labels for regulatory reporting,
capacity planning, or production model validation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class _SegmentProfile:
    amount_mu: float
    amount_sigma: float
    gap_mu: float
    gap_sigma: float
    lam: int


def _rng_from_cfg(cfg) -> np.random.Generator:
    seed = getattr(cfg, "DEMO_SEED", 42)
    return np.random.default_rng(int(seed))


def _segment_profiles(cfg) -> List[Tuple[str, _SegmentProfile]]:
    segments = getattr(cfg, "DEMO_SEGMENTS", [])
    if segments:
        return [
            (name, _SegmentProfile(**params))  # type: ignore[arg-type]
            for name, params in segments
        ]
    return [
        ("retail_low", _SegmentProfile(40, 0.25, 3.5, 0.35, 2)),
        ("retail_high", _SegmentProfile(120, 0.30, 2.2, 0.35, 3)),
        ("smb", _SegmentProfile(350, 0.35, 1.8, 0.40, 4)),
        ("corporate", _SegmentProfile(900, 0.30, 1.5, 0.35, 5)),
    ]


def _segment_probs(cfg, n: int) -> np.ndarray:
    probs = getattr(cfg, "DEMO_SEGMENT_PROBS", None)
    if probs and len(probs) == n:
        return np.array(probs, dtype=float)
    return np.full(n, 1.0 / n)


def _choose(items: Iterable[str], rng: np.random.Generator, size: int, p: np.ndarray | None = None) -> List[str]:
    items = list(items)
    return rng.choice(items, size=size, replace=True, p=p).tolist()


def _generate_timestamps(
    rng: np.random.Generator,
    tx_count: int,
    gap_mu: float,
    gap_sigma: float,
    window_days: int = 90,
) -> np.ndarray:
    now = pd.Timestamp.utcnow()
    window_minutes = window_days * 24 * 60
    gaps_hours = rng.lognormal(mean=np.log(max(gap_mu, 0.1)), sigma=gap_sigma, size=tx_count)
    gaps_minutes = gaps_hours * 60.0
    total_minutes = float(gaps_minutes.sum())
    if total_minutes <= 0:
        gaps_minutes = np.full(tx_count, 60.0)
        total_minutes = float(gaps_minutes.sum())
    if total_minutes > window_minutes:
        scale = (window_minutes * 0.90) / total_minutes
        gaps_minutes = gaps_minutes * scale
    end_time = now - pd.Timedelta(minutes=float(rng.uniform(0, 60 * 24 * 2)))
    start_time = end_time - pd.Timedelta(minutes=float(gaps_minutes.sum()))
    times = start_time + pd.to_timedelta(np.cumsum(gaps_minutes), unit="m")
    return times.to_numpy()


def _rolling_count_24h(times: np.ndarray) -> np.ndarray:
    ns_times = times.astype("datetime64[ns]").astype("int64")
    window = np.int64(24 * 60 * 60 * 1e9)
    left_idx = np.searchsorted(ns_times, ns_times - window, side="left")
    return (np.arange(len(ns_times)) - left_idx + 1).astype(int)


def _apply_structuring(df: pd.DataFrame, cfg, rng: np.random.Generator, users: List[str]) -> None:
    if not users:
        return
    low = getattr(cfg, "STRUCTURING_LOW", 9500)
    high = getattr(cfg, "STRUCTURING_THRESHOLD", 10000)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=60)
    mask_users = df["user_id"].isin(users)
    mask_recent = df["ts"] >= cutoff
    target_idx = df.index[mask_users & mask_recent]
    if target_idx.empty:
        return
    pick = rng.choice(target_idx, size=max(1, int(len(target_idx) * 0.45)), replace=False)
    df.loc[pick, "amount"] = rng.uniform(low, high - 1, size=len(pick))
    df.loc[pick, "direction"] = "out"
    df.loc[pick, "channel"] = rng.choice(["bank_transfer", "cash"], size=len(pick), replace=True)
    df.loc[pick, "typology"] = "structuring"
    df.loc[pick, "synthetic_true_suspicious"] = "Yes"


def _apply_dormant(df: pd.DataFrame, cfg, rng: np.random.Generator, users: List[str]) -> None:
    if not users:
        return
    now = pd.Timestamp.utcnow()
    for user in users:
        idx = df.index[df["user_id"] == user].tolist()
        if len(idx) < 6:
            continue
        rng.shuffle(idx)
        burst_n = max(5, min(12, int(len(idx) * 0.3)))
        burst_idx = idx[:burst_n]
        old_idx = idx[burst_n:]
        burst_times = now - pd.to_timedelta(rng.uniform(5, 48 * 60, size=burst_n), unit="m")
        old_times = now - pd.to_timedelta(rng.uniform(35 * 24 * 60, 90 * 24 * 60, size=len(old_idx)), unit="m")
        df.loc[burst_idx, "ts"] = burst_times
        df.loc[old_idx, "ts"] = old_times
        df.loc[burst_idx, "typology"] = "dormant"
        df.loc[burst_idx, "synthetic_true_suspicious"] = "Yes"


def _apply_rapid_withdraw(df: pd.DataFrame, cfg, rng: np.random.Generator, users: List[str]) -> None:
    if not users:
        return
    for user in users:
        user_df = df[df["user_id"] == user].sort_values("ts")
        if len(user_df) < 4:
            continue
        idx = user_df.index.to_list()
        pos = rng.integers(0, len(idx) - 1)
        inbound_idx = idx[pos]
        outbound_idx = idx[pos + 1]
        inbound_amt = float(max(df.loc[inbound_idx, "amount"], 25.0))
        df.loc[inbound_idx, "direction"] = "in"
        df.loc[outbound_idx, "direction"] = "out"
        df.loc[outbound_idx, "amount"] = inbound_amt * rng.uniform(0.95, 1.05)
        df.loc[outbound_idx, "ts"] = df.loc[inbound_idx, "ts"] + pd.Timedelta(
            minutes=float(rng.uniform(5, 30))
        )
        df.loc[[inbound_idx, outbound_idx], "typology"] = "rapid_withdraw"
        df.loc[[inbound_idx, outbound_idx], "synthetic_true_suspicious"] = "Yes"


def _apply_flow_through(df: pd.DataFrame, cfg, rng: np.random.Generator, users: List[str]) -> None:
    if not users:
        return
    for user in users:
        user_df = df[df["user_id"] == user].sort_values("ts")
        if len(user_df) < 6:
            continue
        window_days = int(rng.integers(3, 8))
        end_time = user_df["ts"].max()
        start_time = end_time - pd.Timedelta(days=window_days)
        window_idx = user_df.index[user_df["ts"] >= start_time].tolist()
        if len(window_idx) < 4:
            continue
        split = len(window_idx) // 2
        in_idx = window_idx[:split]
        out_idx = window_idx[split:]
        df.loc[in_idx, "direction"] = "in"
        df.loc[out_idx, "direction"] = "out"
        total_in = df.loc[in_idx, "amount"].sum()
        total_out = df.loc[out_idx, "amount"].sum()
        if total_out > 0:
            scale = total_in / total_out
            df.loc[out_idx, "amount"] = df.loc[out_idx, "amount"] * scale
        df.loc[window_idx, "typology"] = "flow_through"
        df.loc[window_idx, "synthetic_true_suspicious"] = "Yes"


def _apply_low_buyer_diversity(
    df: pd.DataFrame, cfg, rng: np.random.Generator, users: List[str]
) -> None:
    if not users:
        return
    for user in users:
        idx = df.index[df["user_id"] == user]
        if idx.empty:
            continue
        counterparties = [f"CP_{user}_{i:03d}" for i in range(rng.integers(2, 5))]
        df.loc[idx, "counterparty_id"] = rng.choice(counterparties, size=len(idx), replace=True)
        df.loc[idx, "typology"] = "low_buyer_diversity"
        df.loc[idx, "synthetic_true_suspicious"] = "Yes"


def _apply_high_risk_country(
    df: pd.DataFrame, cfg, rng: np.random.Generator, ratio: float
) -> None:
    if ratio <= 0:
        return
    n = len(df)
    count = max(1, int(n * ratio))
    idx = rng.choice(df.index, size=count, replace=False)
    countries = getattr(cfg, "HIGH_RISK_COUNTRIES", ["AE", "TR", "PA", "CY", "RU"])
    df.loc[idx, "country"] = rng.choice(countries, size=len(idx), replace=True)
    df.loc[idx, "typology"] = np.where(
        df.loc[idx, "typology"] == "none", "high_risk_country", df.loc[idx, "typology"]
    )
    df.loc[idx, "synthetic_true_suspicious"] = "Yes"


def _compute_time_features(df: pd.DataFrame, cfg) -> pd.DataFrame:
    df = df.sort_values(["user_id", "ts"]).reset_index(drop=True)
    gaps = []
    counts = []
    for user, group in df.groupby("user_id", sort=False):
        group = group.sort_values("ts")
        diffs = group["ts"].diff().dt.total_seconds().div(60.0)
        fill = diffs[diffs > 0].median()
        if not np.isfinite(fill):
            fill = 60.0
        diffs = diffs.fillna(fill).clip(lower=getattr(cfg, "DEMO_TIME_GAP_CLIP_MIN", 0.01))
        gaps.append(diffs.to_numpy())
        counts.append(_rolling_count_24h(group["ts"].to_numpy()))
    df["time_gap"] = np.concatenate(gaps)
    df["num_transactions"] = np.concatenate(counts)
    return df


# AML upstream systems (simulated alert sources)
_SOURCE_SYSTEMS = ["Actimize", "SAS", "FICO", "NICE", "Oracle FCC"]
# Typologies typical of AML monitoring outputs
_ALERT_TYPOLOGIES = [
    "structuring", "cross_border", "rapid_velocity", "flow_through",
    "sanctions", "high_amount_outlier", "burst_activity", "smurfing",
    "dormant", "bank_alert", "low_buyer_diversity", "high_risk_country",
]
# Rule names that upstream systems might attach
_RULE_NAMES = [
    "VelocityThreshold", "StructPattern", "CrossBorderFlow", "AmountSpike",
    "DormantReactivate", "PeerOutlier", "SanctionsScreening", "PEPMatch",
]
_DETECTION_CHANNELS = ["transaction_monitor", "screening", "surveillance", "network_analytics"]

# High-risk typologies and signals for pilot injection (HIGH/CRITICAL)
_HIGH_RISK_TYPOLOGIES = ["sanctions", "high_risk_country", "cross_border", "high_amount_outlier", "structuring", "flow_through"]
_HIGH_RISK_COUNTRIES = ["RU", "AE", "TR", "PA", "CY"]


def _apply_pilot_risk_injection(
    df: pd.DataFrame,
    rng: np.random.Generator,
    risk_distribution: Dict[str, float],
) -> pd.DataFrame:
    """
    After base generation: assign risk bands to hit target distribution, then inject
    contextual risk signals for HIGH/CRITICAL and MEDIUM. Seeded by the same rng for determinism.
    Does not modify scoring logic or governance thresholds.
    """
    n = len(df)
    if n == 0:
        return df

    # 1) Target counts from distribution (deterministic given n and distribution)
    low_p = risk_distribution.get("low", 0.6)
    med_p = risk_distribution.get("medium", 0.25)
    high_p = risk_distribution.get("high", 0.12)
    crit_p = risk_distribution.get("critical", 0.03)
    probs = np.array([low_p, med_p, high_p, crit_p])
    probs = probs / probs.sum()
    counts = rng.multinomial(n, probs)
    n_low, n_med, n_high, n_crit = counts[0], counts[1], counts[2], counts[3]

    # 2) Assign bands: shuffle row indices then assign in order (deterministic)
    indices = np.arange(n, dtype=int)
    rng.shuffle(indices)
    bands: List[str] = (
        ["LOW"] * n_low + ["MEDIUM"] * n_med + ["HIGH"] * n_high + ["CRITICAL"] * n_crit
    )
    # Map index position -> band (indices[i] gets bands[i])
    band_by_idx = {indices[i]: bands[i] for i in range(n)}
    df = df.copy()
    df["alert_risk_band"] = [band_by_idx[i] for i in range(n)]

    # 3) Initialize optional columns so downstream can use them (rules need country + direction)
    df["sanctions_hit"] = False
    df["high_risk_country"] = False
    df["amount"] = 0.0
    df["repeated_typology"] = False
    df["behavioral_baseline_deviation"] = 0.0
    df["cross_border"] = False
    df["n_hist"] = 0
    df["peer_segment_percentile"] = 0.5
    # direction/country already set in base; overwrite country for high_risk_country HIGH/CRITICAL rows below
    # Amount baseline for 95th percentile (deterministic)
    base_amounts = rng.lognormal(mean=7, sigma=1.5, size=n).round(2)
    df["amount"] = base_amounts
    p95 = float(np.percentile(base_amounts, 95))

    high_crit_mask = df["alert_risk_band"].isin(["HIGH", "CRITICAL"])
    high_crit_idx = df.index[high_crit_mask].tolist()

    for idx in high_crit_idx:
        # Probabilistically inject HIGH-risk signals (at least one strong signal)
        u = rng.random()
        if u < 0.22:
            df.at[idx, "sanctions_hit"] = True
            df.at[idx, "typology"] = "sanctions"
        elif u < 0.44:
            df.at[idx, "high_risk_country"] = True
            df.at[idx, "typology"] = rng.choice(["high_risk_country", "cross_border"])
            df.at[idx, "country"] = rng.choice(_HIGH_RISK_COUNTRIES)
        elif u < 0.62:
            df.at[idx, "amount"] = float(rng.uniform(p95 * 1.01, p95 * 3.0))
            df.at[idx, "typology"] = rng.choice(["high_amount_outlier", "structuring"])
        elif u < 0.78:
            df.at[idx, "repeated_typology"] = True
            df.at[idx, "typology"] = rng.choice(_HIGH_RISK_TYPOLOGIES)
        elif u < 0.90:
            df.at[idx, "behavioral_baseline_deviation"] = float(rng.uniform(1.8, 4.0))
            df.at[idx, "typology"] = rng.choice(["rapid_velocity", "burst_activity", "dormant"])
        else:
            df.at[idx, "cross_border"] = True
            df.at[idx, "typology"] = "cross_border"
        # Ensure amount > 95th for a subset of high/critical
        if rng.random() < 0.4:
            df.at[idx, "amount"] = float(rng.uniform(p95 * 1.01, p95 * 2.5))

    med_idx = df.index[df["alert_risk_band"] == "MEDIUM"].tolist()
    for idx in med_idx:
        if rng.random() < 0.5:
            df.at[idx, "typology_confidence"] = min(100.0, float(df.at[idx, "typology_confidence"]) + rng.uniform(5, 25))
        if rng.random() < 0.5:
            df.at[idx, "peer_segment_percentile"] = float(rng.uniform(0.80, 0.98))
        if rng.random() < 0.5:
            df.at[idx, "n_hist"] = int(rng.integers(2, 12))

    return df


def generate_synthetic_alerts(
    n_rows: int = 400,
    cfg: Optional[Any] = None,
    seed: int = 42,
    n_entities: Optional[int] = None,
) -> pd.DataFrame:
    """
    Generate synthetic AML alert-level data conforming to NormalizedAlert schema.
    Each row simulates an alert already raised by an upstream monitoring engine.
    No transaction-level fields (transaction_id, merchant, counterparty, etc.).
    """
    rng = np.random.default_rng(seed)
    n = max(1, int(n_rows))
    n_entities = n_entities or max(50, n // 8)

    entity_ids = [f"USR{i:04d}" for i in rng.integers(0, 99999, size=n_entities).tolist()]
    # Ensure enough unique entities for n_rows
    entity_list = (entity_ids * (1 + n // n_entities))[:n]
    rng.shuffle(entity_list)

    base_ts = pd.Timestamp.utcnow() - pd.Timedelta(days=30)
    timestamps = [
        (base_ts + pd.Timedelta(minutes=float(rng.integers(0, 30 * 24 * 60)))).strftime("%Y-%m-%dT%H:%M:%SZ")
        for _ in range(n)
    ]

    alert_ids = [f"ALT{rng.integers(1000, 99999)}" for _ in range(n)]
    source_systems = rng.choice(_SOURCE_SYSTEMS, size=n, replace=True).tolist()
    typologies = rng.choice(_ALERT_TYPOLOGIES, size=n, replace=True).tolist()
    risk_scores_source = rng.integers(20, 95, size=n).tolist()
    rule_names = rng.choice(_RULE_NAMES, size=n, replace=True).tolist()
    detection_channels = rng.choice(_DETECTION_CHANNELS, size=n, replace=True).tolist()
    typology_confidence = (rng.uniform(0.3, 0.98, size=n) * 100).round(1).tolist()
    alert_risk_bands = rng.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"], size=n, replace=True, p=[0.35, 0.35, 0.2, 0.1]).tolist()

    vendor_metadata_list: List[Dict[str, Any]] = []
    for i in range(n):
        vm: Dict[str, Any] = {
            "rule_name": rule_names[i],
            "alert_age_days": int(rng.integers(0, 14)),
        }
        if rng.random() > 0.5:
            vm["detection_channel"] = detection_channels[i]
        vendor_metadata_list.append(vm)

    df = pd.DataFrame({
        "alert_id": alert_ids,
        "entity_id": entity_list,
        "source_system": source_systems,
        "timestamp": timestamps,
        "typology": typologies,
        "risk_score_source": risk_scores_source,
        "vendor_metadata": vendor_metadata_list,
    })
    df["alert_risk_band"] = alert_risk_bands
    df["rule_name"] = rule_names
    df["detection_channel"] = detection_channels
    df["typology_confidence"] = typology_confidence
    # Pipeline expects user_id for some stages; map entity_id
    df["user_id"] = df["entity_id"]
    # Optional: segment for enrichment (alert-level context)
    df["segment"] = rng.choice(["retail", "smb", "corporate", "private_banking"], size=n, replace=True).tolist()
    # Columns required by ingest and rules (overlay): country, direction, amount (pilot may overwrite)
    df["country"] = "US"
    df["direction"] = "out"
    df["amount"] = (rng.lognormal(mean=6, sigma=1.2, size=n).round(2)).tolist()

    # Pilot test mode: risk-stratified distribution + contextual signal injection (deterministic, seeded)
    pilot_test_mode = False
    risk_distribution: Dict[str, float] = {"low": 0.6, "medium": 0.25, "high": 0.12, "critical": 0.03}
    if cfg is not None:
        pilot_test_mode = getattr(cfg, "PILOT_TEST_MODE", False)
        risk_distribution = getattr(cfg, "PILOT_RISK_DISTRIBUTION", risk_distribution)
    else:
        try:
            from . import config as app_config
            pilot_test_mode = getattr(app_config, "PILOT_TEST_MODE", False)
            risk_distribution = getattr(app_config, "PILOT_RISK_DISTRIBUTION", risk_distribution)
        except Exception:
            pass
    if pilot_test_mode and isinstance(risk_distribution, dict):
        df = _apply_pilot_risk_injection(df, rng, risk_distribution)

    return df


def generate_synthetic_transactions(
    n_users: int,
    tx_per_user: int,
    cfg,
    seed: int = 42,
    suspicious_rate: float = 0.06
) -> pd.DataFrame:
    """
    Thin wrapper over demo_data.generate_demo_data().
    Produces raw transaction-level data (not for overlay mode).
    """
    from . import demo_data

    df = demo_data.generate_demo_data(
        n_users=n_users,
        tx_per_user=tx_per_user,
        seed=seed,
        suspicious_rate=suspicious_rate
    )
    return df
