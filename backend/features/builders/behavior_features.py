"""Behavioral window feature builder.

Computes rolling behavioural aggregates over 1d / 7d / 30d / 90d windows
from a transaction history DataFrame. These features are the backbone of
AML detection — they capture volume, velocity, pattern changes, and novelty
relative to the entity's own historical baseline.

Required context:
    context.transaction_history : DataFrame with columns
        [entity_id, timestamp, amount, country, counterparty_id, is_cross_border]
        Pre-filtered to cover at least 90 days before the alert timestamp.

Features produced per window (prefix = window tag):
    txn_count_{w}d           transaction count in window
    amount_sum_{w}d          total amount
    amount_avg_{w}d          average amount per transaction
    velocity_delta_{w}d      change in daily txn rate vs prior period
    new_counterparty_ratio   fraction of new (first-time) counterparties in 7d
    cross_border_ratio       fraction of cross-border transactions in 7d
    dormant_reactivation     1 if entity had no activity in prior 60d
    round_amount_ratio       fraction of round-amount transactions
    burstiness               coefficient of variation of inter-transaction gaps
    pass_through_proxy       fraction of transactions where in ≈ out in 30d
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features.builders.base import BaseFeatureBuilder, BuilderContext

_WINDOWS = [1, 7, 30, 90]
_DORMANCY_DAYS = 60
_ROUND_MOD = 1000.0


class BehaviorFeatureBuilder(BaseFeatureBuilder):
    """Build behavioral window features from transaction history."""

    @property
    def feature_names(self) -> list[str]:
        names = []
        for w in _WINDOWS:
            names += [f"txn_count_{w}d", f"amount_sum_{w}d", f"amount_avg_{w}d"]
        names += [
            "velocity_delta_7d",
            "new_counterparty_ratio",
            "cross_border_ratio",
            "dormant_reactivation",
            "round_amount_ratio",
            "burstiness",
            "pass_through_proxy",
        ]
        return names

    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
        zeros = self._zeros(alert_ids)

        if context is None or context.transaction_history is None or context.transaction_history.empty:
            return zeros

        txn = context.transaction_history.copy()
        txn = self._normalize_txn(txn)
        if txn.empty:
            return zeros

        # Map alert → entity_id
        entity_col = "user_id" if "user_id" in df.columns else ("customer_id" if "customer_id" in df.columns else None)
        if entity_col is None:
            return zeros

        alerts = df[["alert_id", entity_col, "timestamp"]].copy()
        alerts["alert_id"] = alerts["alert_id"].astype(str)
        alerts["_alert_ts"] = pd.to_datetime(alerts["timestamp"], utc=True, errors="coerce")
        alerts["_entity"] = alerts[entity_col].astype(str)

        rows = []
        for _, alert_row in alerts.iterrows():
            entity = str(alert_row["_entity"])
            ts = alert_row["_alert_ts"]
            entity_txn = txn[txn["entity_id"] == entity].copy()

            row: dict[str, object] = {"alert_id": str(alert_row["alert_id"])}

            for w in _WINDOWS:
                cutoff_start = ts - pd.Timedelta(days=w)
                window_txn = entity_txn[(entity_txn["ts"] >= cutoff_start) & (entity_txn["ts"] < ts)]
                row[f"txn_count_{w}d"] = float(len(window_txn))
                row[f"amount_sum_{w}d"] = float(window_txn["amount"].sum())
                row[f"amount_avg_{w}d"] = float(window_txn["amount"].mean()) if len(window_txn) > 0 else 0.0

            # Velocity delta: 7d rate vs 8-14d rate
            prev7_start = ts - pd.Timedelta(days=14)
            prev7_end = ts - pd.Timedelta(days=7)
            prev7 = entity_txn[(entity_txn["ts"] >= prev7_start) & (entity_txn["ts"] < prev7_end)]
            cur7_rate = row["txn_count_7d"] / 7.0
            prev7_rate = len(prev7) / 7.0
            row["velocity_delta_7d"] = float(cur7_rate - prev7_rate)

            # New counterparty ratio in 7d
            seven_day_start = ts - pd.Timedelta(days=7)
            historical_start = ts - pd.Timedelta(days=90)
            w7_txn = entity_txn[(entity_txn["ts"] >= seven_day_start) & (entity_txn["ts"] < ts)]
            prior_txn = entity_txn[(entity_txn["ts"] >= historical_start) & (entity_txn["ts"] < seven_day_start)]
            if "counterparty_id" in entity_txn.columns and len(w7_txn) > 0:
                prior_cps = set(prior_txn["counterparty_id"].dropna().astype(str))
                new_cps = w7_txn["counterparty_id"].dropna().astype(str)
                row["new_counterparty_ratio"] = float(new_cps.isin(prior_cps.__class__(new_cps) - prior_cps).mean()) if len(new_cps) > 0 else 0.0
                # Simpler: count new counterparties / total in window
                new_flag = ~new_cps.isin(prior_cps)
                row["new_counterparty_ratio"] = float(new_flag.mean()) if len(new_cps) > 0 else 0.0
            else:
                row["new_counterparty_ratio"] = 0.0

            # Cross-border ratio in 7d
            if "is_cross_border" in entity_txn.columns and len(w7_txn) > 0:
                row["cross_border_ratio"] = float(w7_txn["is_cross_border"].astype(float).mean())
            else:
                row["cross_border_ratio"] = 0.0

            # Dormant reactivation
            prior60_start = ts - pd.Timedelta(days=60)
            prior_60d = entity_txn[(entity_txn["ts"] >= prior60_start) & (entity_txn["ts"] < ts)]
            row["dormant_reactivation"] = 1.0 if len(prior_60d) == 0 and len(entity_txn) > 0 else 0.0

            # Round-amount ratio in 7d
            if len(w7_txn) > 0:
                round_mask = (w7_txn["amount"] % _ROUND_MOD) < 1e-2
                row["round_amount_ratio"] = float(round_mask.mean())
            else:
                row["round_amount_ratio"] = 0.0

            # Burstiness: coefficient of variation of inter-transaction gaps
            if len(w7_txn) >= 3:
                sorted_ts = w7_txn["ts"].sort_values()
                gaps = sorted_ts.diff().dt.total_seconds().dropna()
                mean_gap = float(gaps.mean())
                std_gap = float(gaps.std())
                row["burstiness"] = float(std_gap / mean_gap) if mean_gap > 0 else 0.0
            else:
                row["burstiness"] = 0.0

            # Pass-through proxy in 30d: high volume in AND out for same entity
            thirty_day_start = ts - pd.Timedelta(days=30)
            w30_txn = entity_txn[(entity_txn["ts"] >= thirty_day_start) & (entity_txn["ts"] < ts)]
            if "direction" in entity_txn.columns and len(w30_txn) > 0:
                credits = float(w30_txn[w30_txn["direction"] == "credit"]["amount"].sum())
                debits = float(w30_txn[w30_txn["direction"] == "debit"]["amount"].sum())
                total = credits + debits
                row["pass_through_proxy"] = float(min(credits, debits) / total) if total > 0 else 0.0
            else:
                # Proxy: high txn count relative to 90d baseline suggests pass-through
                row["pass_through_proxy"] = min(float(row["txn_count_30d"]) / max(float(row["txn_count_90d"]), 1.0), 1.0)

            rows.append(row)

        if not rows:
            return zeros

        result = pd.DataFrame(rows)
        for col in self.feature_names:
            if col not in result.columns:
                result[col] = 0.0
        return result[["alert_id"] + self.feature_names].reset_index(drop=True)

    # ------------------------------------------------------------------

    def _zeros(self, alert_ids: list[str]) -> pd.DataFrame:
        out = pd.DataFrame({"alert_id": alert_ids})
        for name in self.feature_names:
            out[name] = 0.0
        return out

    @staticmethod
    def _normalize_txn(txn: pd.DataFrame) -> pd.DataFrame:
        out = txn.copy()
        # Standardize entity identifier
        if "entity_id" not in out.columns:
            for col in ("user_id", "customer_id", "account_id"):
                if col in out.columns:
                    out["entity_id"] = out[col].astype(str)
                    break
        if "entity_id" not in out.columns:
            return pd.DataFrame()

        # Standardize timestamp
        for ts_col in ("timestamp", "ts", "timestamp_utc", "created_at"):
            if ts_col in out.columns:
                out["ts"] = pd.to_datetime(out[ts_col], utc=True, errors="coerce")
                break
        if "ts" not in out.columns:
            return pd.DataFrame()

        out = out.dropna(subset=["entity_id", "ts"])
        out["amount"] = pd.to_numeric(out.get("amount", 0.0), errors="coerce").fillna(0.0)
        return out
