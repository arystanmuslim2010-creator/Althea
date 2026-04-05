"""Alert-native feature builder.

Produces features derived exclusively from the alert record itself —
no external enrichment required. These features are available immediately
on alert ingestion with zero latency.

Features produced:
    amount                 raw transaction amount
    amount_log1p           log(1 + amount)
    hour_of_day            hour extracted from alert timestamp
    day_of_week            day of week (0=Monday)
    is_weekend             1 if Saturday or Sunday
    typology_code          factorized typology category
    segment_code           factorized customer segment
    source_system_code     factorized source system
    country_risk           jurisdiction risk score [0, 1]
    threshold_breach       1 if amount exceeds regulatory threshold (10k / 3k)
    is_round_amount        1 if amount is exactly divisible by 1000
    is_cross_border        1 if typology or country flags suggest cross-border
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from features.builders.base import BaseFeatureBuilder, BuilderContext

# Jurisdiction risk scores derived from FATF high-risk / grey-list status
# and country-specific AML maturity assessments.
# Scale: 0.0 (low risk) → 1.0 (very high risk / sanctioned)
_COUNTRY_RISK: dict[str, float] = {
    "US": 0.15, "GB": 0.15, "DE": 0.15, "FR": 0.15, "CH": 0.20,
    "SG": 0.25, "HK": 0.30, "AE": 0.55, "CN": 0.40, "IN": 0.35,
    "MX": 0.50, "BR": 0.45, "ZA": 0.50, "NG": 0.60, "PK": 0.65,
    "AF": 0.85, "MM": 0.80, "KP": 1.00, "IR": 1.00, "SY": 0.90,
    "RU": 0.70, "BY": 0.65, "CU": 0.75, "VE": 0.70,
    "UNKNOWN": 0.30,
}

# Regulatory reporting thresholds (USD equivalent)
_CTR_THRESHOLD = 10_000.0   # Currency Transaction Report
_SAR_THRESHOLD = 3_000.0    # Structuring concern threshold


class AlertFeatureBuilder(BaseFeatureBuilder):
    """Build alert-native features from the normalized alerts DataFrame."""

    @property
    def feature_names(self) -> list[str]:
        return [
            "amount",
            "amount_log1p",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "typology_code",
            "segment_code",
            "source_system_code",
            "country_risk",
            "threshold_breach",
            "is_round_amount",
            "is_cross_border",
        ]

    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            out = pd.DataFrame(columns=["alert_id"] + self.feature_names)
            return out

        out = pd.DataFrame()
        out["alert_id"] = df["alert_id"].astype(str) if "alert_id" in df.columns else pd.Series(range(len(df))).astype(str)

        # ------ amount -------------------------------------------------------
        amount = pd.to_numeric(df.get("amount", 0.0), errors="coerce").fillna(0.0)
        out["amount"] = amount
        out["amount_log1p"] = np.log1p(np.clip(amount, 0.0, None))

        # ------ temporal -----------------------------------------------------
        fallback_ts = pd.Timestamp.now(tz="UTC")
        ts = pd.to_datetime(df.get("timestamp", fallback_ts), errors="coerce", utc=True)
        ts = ts.fillna(fallback_ts)
        out["hour_of_day"] = ts.dt.hour.astype(float)
        out["day_of_week"] = ts.dt.dayofweek.astype(float)
        out["is_weekend"] = (out["day_of_week"] >= 5.0).astype(float)

        # ------ categoricals -------------------------------------------------
        out["typology_code"] = self._factorize(df.get("typology", pd.Series(["anomaly"] * len(df))))
        out["segment_code"] = self._factorize(df.get("segment", pd.Series(["retail"] * len(df))))
        out["source_system_code"] = self._factorize(df.get("source_system", pd.Series(["core_bank"] * len(df))))

        # ------ jurisdiction risk -------------------------------------------
        country = df.get("country", pd.Series(["UNKNOWN"] * len(df))).astype(str).str.upper().str.strip()
        out["country_risk"] = country.map(_COUNTRY_RISK).fillna(0.30).astype(float)

        # ------ regulatory thresholds ----------------------------------------
        out["threshold_breach"] = np.where(
            amount >= _CTR_THRESHOLD, 2.0,
            np.where(amount >= _SAR_THRESHOLD, 1.0, 0.0)
        ).astype(float)

        # ------ round amount (structuring signal) ----------------------------
        out["is_round_amount"] = ((amount % 1000.0) < 1e-2).astype(float)

        # ------ cross-border proxy -------------------------------------------
        typology_str = df.get("typology", pd.Series([""] * len(df))).astype(str).str.lower()
        cross_border_typology = typology_str.str.contains("cross_border|international|swift|wire", na=False).astype(float)
        high_risk_country = (out["country_risk"] >= 0.55).astype(float)
        out["is_cross_border"] = np.clip(cross_border_typology + high_risk_country, 0.0, 1.0)

        return out.reset_index(drop=True)

    @staticmethod
    def _factorize(series: pd.Series) -> pd.Series:
        codes, _ = pd.factorize(series.astype(str).fillna("UNKNOWN"), sort=True)
        return pd.Series(codes, index=series.index, dtype=float)
