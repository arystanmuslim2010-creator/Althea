"""Historical outcome feature builder.

Computes features derived from prior AML alert outcomes for the same entity.
These encode the entity's investigation track record and are extremely
predictive for repeat-offender cases and recurrence patterns.

Required context:
    context.outcome_history : DataFrame with columns
        [entity_id, alert_id, analyst_decision, timestamp, typology]

Features produced:
    prior_alert_count        total prior alerts for this entity
    prior_escalation_rate    fraction of prior alerts that were escalated/TP
    prior_sar_rate           fraction of prior alerts that led to SAR filing
    prior_fp_rate            fraction that were false positives
    prior_typology_freq      frequency of current typology in prior alerts
    days_since_last_alert    calendar days since most recent prior alert
    recurrence_flag          1 if entity has any prior escalated alert
    sar_history_flag         1 if entity has any prior SAR filing
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features.builders.base import BaseFeatureBuilder, BuilderContext

_ESCALATED_DECISIONS = frozenset({"true_positive", "escalated", "sar_filed", "confirmed_suspicious"})
_SAR_DECISIONS = frozenset({"sar_filed"})
_FP_DECISIONS = frozenset({"false_positive", "benign_activity"})


class HistoryFeatureBuilder(BaseFeatureBuilder):

    @property
    def feature_names(self) -> list[str]:
        return [
            "prior_alert_count",
            "prior_escalation_rate",
            "prior_sar_rate",
            "prior_fp_rate",
            "prior_typology_freq",
            "days_since_last_alert",
            "recurrence_flag",
            "sar_history_flag",
        ]

    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
        zeros = self._zeros(alert_ids)

        if context is None or context.outcome_history is None or context.outcome_history.empty:
            return zeros

        history = context.outcome_history.copy()
        history = self._normalize_history(history)
        if history.empty:
            return zeros

        entity_col = self._entity_col(df)
        if entity_col is None:
            return zeros

        alerts = df[["alert_id", entity_col]].copy()
        alerts["_entity"] = alerts[entity_col].astype(str)
        alert_ts = self._parse_alert_timestamps(df)

        rows = []
        for idx, alert_row in alerts.iterrows():
            entity = str(alert_row["_entity"])
            alert_id = str(alert_row["alert_id"])
            ts = alert_ts.iloc[idx] if idx < len(alert_ts) else None
            typology = str(df.get("typology", pd.Series([""] * len(df))).iloc[idx] if "typology" in df.columns else "")

            # Strict point-in-time: only outcomes BEFORE this alert's timestamp
            entity_hist = history[history["entity_id"] == entity]
            if ts is not None and pd.notna(ts):
                entity_hist = entity_hist[entity_hist["ts"] < ts]

            # Exclude the current alert itself
            entity_hist = entity_hist[entity_hist["alert_id"] != alert_id]

            n = len(entity_hist)
            row: dict[str, object] = {"alert_id": alert_id}
            row["prior_alert_count"] = float(n)

            if n > 0:
                decisions = entity_hist["analyst_decision"].astype(str).str.lower()
                row["prior_escalation_rate"] = float(decisions.isin(_ESCALATED_DECISIONS).mean())
                row["prior_sar_rate"] = float(decisions.isin(_SAR_DECISIONS).mean())
                row["prior_fp_rate"] = float(decisions.isin(_FP_DECISIONS).mean())

                if typology and "typology" in entity_hist.columns:
                    typo_matches = (entity_hist["typology"].astype(str).str.lower() == typology.lower()).sum()
                    row["prior_typology_freq"] = float(typo_matches / n)
                else:
                    row["prior_typology_freq"] = 0.0

                last_ts = entity_hist["ts"].max()
                if ts is not None and pd.notna(last_ts):
                    row["days_since_last_alert"] = float((ts - last_ts).total_seconds() / 86400.0)
                else:
                    row["days_since_last_alert"] = 999.0

                row["recurrence_flag"] = 1.0 if decisions.isin(_ESCALATED_DECISIONS).any() else 0.0
                row["sar_history_flag"] = 1.0 if decisions.isin(_SAR_DECISIONS).any() else 0.0
            else:
                row["prior_escalation_rate"] = 0.0
                row["prior_sar_rate"] = 0.0
                row["prior_fp_rate"] = 0.0
                row["prior_typology_freq"] = 0.0
                row["days_since_last_alert"] = 999.0
                row["recurrence_flag"] = 0.0
                row["sar_history_flag"] = 0.0

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
    def _normalize_history(history: pd.DataFrame) -> pd.DataFrame:
        out = history.copy()
        if "entity_id" not in out.columns:
            for col in ("user_id", "customer_id", "account_id"):
                if col in out.columns:
                    out["entity_id"] = out[col].astype(str)
                    break
        if "entity_id" not in out.columns:
            return pd.DataFrame()

        for ts_col in ("timestamp", "ts", "decision_timestamp", "created_at"):
            if ts_col in out.columns:
                out["ts"] = pd.to_datetime(out[ts_col], utc=True, errors="coerce")
                break
        if "ts" not in out.columns:
            out["ts"] = pd.NaT

        if "alert_id" not in out.columns:
            out["alert_id"] = ""
        if "analyst_decision" not in out.columns:
            out["analyst_decision"] = "unknown"

        return out.dropna(subset=["entity_id"])

    @staticmethod
    def _entity_col(df: pd.DataFrame) -> str | None:
        for col in ("user_id", "customer_id", "account_id"):
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _parse_alert_timestamps(df: pd.DataFrame) -> pd.Series:
        for col in ("timestamp", "alert_created_at", "created_at", "timestamp_utc"):
            if col in df.columns:
                return pd.to_datetime(df[col], utc=True, errors="coerce")
        return pd.Series([None] * len(df))
