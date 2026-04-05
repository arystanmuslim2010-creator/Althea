"""Investigation cost / complexity feature builder.

Estimates the expected investigation effort for an alert based on
structural complexity: entity linkage, graph depth, historical case
resolution times, and analyst touch count proxies.

These features feed directly into the investigation time model.

Required context:
    context.graph_features : DataFrame with alert_id, linked_entity_count,
                              graph_degree, graph_complexity_proxy
    context.case_history   : DataFrame with entity_id, resolution_hours,
                              touch_count

Features produced:
    linked_entity_count         number of entities linked to this alert
    graph_complexity_proxy      normalized graph density proxy [0, 1]
    prior_similar_case_time_mean mean resolution hours of similar past cases
    prior_similar_case_time_p90  p90 resolution hours of similar past cases
    prior_touch_count            mean analyst touch count in similar cases
    investigation_complexity     composite complexity score [0, 1]
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features.builders.base import BaseFeatureBuilder, BuilderContext


class CostFeatureBuilder(BaseFeatureBuilder):

    @property
    def feature_names(self) -> list[str]:
        return [
            "linked_entity_count",
            "graph_complexity_proxy",
            "prior_similar_case_time_mean",
            "prior_similar_case_time_p90",
            "prior_touch_count",
            "investigation_complexity",
        ]

    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
        out = self._zeros(alert_ids)

        # Graph-derived complexity
        if context is not None and context.graph_features is not None and not context.graph_features.empty:
            gf = context.graph_features.copy()
            gf["alert_id"] = gf["alert_id"].astype(str)
            merged = pd.DataFrame({"alert_id": alert_ids}).merge(gf, on="alert_id", how="left")
            out["linked_entity_count"] = pd.to_numeric(
                merged.get("linked_entity_count", 1), errors="coerce"
            ).fillna(1.0)
            out["graph_complexity_proxy"] = np.clip(
                pd.to_numeric(merged.get("graph_complexity_proxy", 0.0), errors="coerce").fillna(0.0),
                0.0, 1.0
            )
        else:
            # Derive a simple proxy from alert fields
            entity_cols = [c for c in df.columns if c in ("account_id", "counterparty_id", "device_id", "ip_address")]
            if entity_cols:
                non_null_entities = df[entity_cols].notna().sum(axis=1)
                out["linked_entity_count"] = non_null_entities.clip(lower=1).astype(float).values
                out["graph_complexity_proxy"] = np.clip(non_null_entities / len(entity_cols), 0.0, 1.0).values
            else:
                out["linked_entity_count"] = 1.0
                out["graph_complexity_proxy"] = 0.1

        # Case history — resolution time and touch count
        if context is not None and context.case_history is not None and not context.case_history.empty:
            case_hist = context.case_history.copy()
            case_hist = self._normalize_case_history(case_hist)

            entity_col = self._entity_col(df)
            if entity_col is not None:
                entities = df[entity_col].astype(str)
                time_means = []
                time_p90s = []
                touch_means = []
                for eid in entities:
                    entity_cases = case_hist[case_hist["entity_id"] == eid]
                    if entity_cases.empty:
                        # Fall back to typology-level stats
                        typology = str(df.get("typology", pd.Series([""] * len(df))).iloc[0] if "typology" in df.columns else "")
                        if typology and "typology" in case_hist.columns:
                            entity_cases = case_hist[case_hist["typology"].str.lower() == typology.lower()]

                    if not entity_cases.empty and "resolution_hours" in entity_cases.columns:
                        rh = pd.to_numeric(entity_cases["resolution_hours"], errors="coerce").dropna()
                        time_means.append(float(rh.mean()) if len(rh) > 0 else 24.0)
                        time_p90s.append(float(rh.quantile(0.90)) if len(rh) > 0 else 48.0)
                    else:
                        time_means.append(24.0)  # default 24h
                        time_p90s.append(48.0)

                    if not entity_cases.empty and "touch_count" in entity_cases.columns:
                        tc = pd.to_numeric(entity_cases["touch_count"], errors="coerce").dropna()
                        touch_means.append(float(tc.mean()) if len(tc) > 0 else 3.0)
                    else:
                        touch_means.append(3.0)

                out["prior_similar_case_time_mean"] = time_means
                out["prior_similar_case_time_p90"] = time_p90s
                out["prior_touch_count"] = touch_means
            # else: keep defaults

        # Composite investigation complexity [0, 1]
        out["investigation_complexity"] = np.clip(
            (
                out["graph_complexity_proxy"] * 0.35
                + np.clip(np.log1p(out["linked_entity_count"]) / np.log1p(20), 0.0, 1.0) * 0.35
                + np.clip(out["prior_similar_case_time_mean"] / 168.0, 0.0, 1.0) * 0.30
            ),
            0.0, 1.0
        )

        return out.reset_index(drop=True)

    def _zeros(self, alert_ids: list[str]) -> pd.DataFrame:
        out = pd.DataFrame({"alert_id": alert_ids})
        out["linked_entity_count"] = 1.0
        out["graph_complexity_proxy"] = 0.1
        out["prior_similar_case_time_mean"] = 24.0
        out["prior_similar_case_time_p90"] = 48.0
        out["prior_touch_count"] = 3.0
        out["investigation_complexity"] = 0.2
        return out

    @staticmethod
    def _normalize_case_history(case_hist: pd.DataFrame) -> pd.DataFrame:
        out = case_hist.copy()
        if "entity_id" not in out.columns:
            for col in ("user_id", "customer_id", "account_id"):
                if col in out.columns:
                    out["entity_id"] = out[col].astype(str)
                    break
        return out

    @staticmethod
    def _entity_col(df: pd.DataFrame) -> str | None:
        for col in ("user_id", "customer_id", "account_id"):
            if col in df.columns:
                return col
        return None
