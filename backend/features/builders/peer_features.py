"""Peer deviation feature builder.

Computes how much a given alert entity deviates from its peer group
(same segment, same typology, or same jurisdiction). High peer deviation
is a strong discriminator: legitimate customers cluster closely around
their peer distribution, while suspicious actors often stand out.

Required context:
    context.peer_stats : DataFrame with columns
        [segment, typology, country,
         peer_amount_p50, peer_amount_p90, peer_amount_mean,
         peer_velocity_p50, peer_velocity_p90,
         peer_count]

Features produced:
    amount_vs_peer_percentile    percentile of this alert's amount within peer
    velocity_vs_peer_percentile  percentile of velocity within peer
    amount_peer_z                standard scores vs peer mean/std
    typology_novelty             1 if typology is rare for this segment
    geo_deviation                1 if country is atypical for this segment
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from features.builders.base import BaseFeatureBuilder, BuilderContext


class PeerFeatureBuilder(BaseFeatureBuilder):

    @property
    def feature_names(self) -> list[str]:
        return [
            "amount_vs_peer_percentile",
            "velocity_vs_peer_percentile",
            "amount_peer_z",
            "typology_novelty",
            "geo_deviation",
        ]

    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
        zeros = self._zeros(alert_ids)

        if context is None or context.peer_stats is None or context.peer_stats.empty:
            # Fallback: infer peer stats from the batch itself (training-time only)
            return self._intra_batch_peers(df)

        peer = context.peer_stats.copy()
        rows = []
        for idx, alert_row in df.iterrows():
            alert_id = str(alert_row.get("alert_id", idx))
            amount = float(alert_row.get("amount", 0.0) or 0.0)
            segment = str(alert_row.get("segment", "retail") or "retail").lower()
            typology = str(alert_row.get("typology", "") or "").lower()
            country = str(alert_row.get("country", "UNKNOWN") or "UNKNOWN").upper()
            velocity = float(alert_row.get("txn_count_7d", 0.0) or 0.0)

            # Find matching peer group (segment match preferred; fall back to global)
            peer_match = peer[peer["segment"].str.lower() == segment] if "segment" in peer.columns else peer
            if peer_match.empty:
                peer_match = peer

            row: dict[str, object] = {"alert_id": alert_id}

            if not peer_match.empty:
                p50 = float(peer_match["peer_amount_p50"].iloc[0] or 0.0)
                p90 = float(peer_match["peer_amount_p90"].iloc[0] or 0.0)
                mean = float(peer_match["peer_amount_mean"].iloc[0] or 0.0)
                std = float(peer_match.get("peer_amount_std", pd.Series([1.0])).iloc[0] or 1.0)

                # Percentile via linear interpolation between p50 and p90
                if p90 > p50:
                    row["amount_vs_peer_percentile"] = float(
                        min(0.5 + 0.4 * (amount - p50) / (p90 - p50 + 1e-6), 1.0)
                    )
                elif amount >= p50:
                    row["amount_vs_peer_percentile"] = 1.0
                else:
                    row["amount_vs_peer_percentile"] = float(min(amount / (p50 + 1e-6) * 0.5, 0.5))

                row["amount_peer_z"] = float((amount - mean) / (std + 1e-6))

                # Velocity peer comparison
                v_p50 = float(peer_match.get("peer_velocity_p50", pd.Series([0.0])).iloc[0] or 0.0)
                v_p90 = float(peer_match.get("peer_velocity_p90", pd.Series([0.0])).iloc[0] or 0.0)
                if v_p90 > v_p50:
                    row["velocity_vs_peer_percentile"] = float(
                        min(0.5 + 0.4 * (velocity - v_p50) / (v_p90 - v_p50 + 1e-6), 1.0)
                    )
                else:
                    row["velocity_vs_peer_percentile"] = 0.5
            else:
                row["amount_vs_peer_percentile"] = 0.5
                row["velocity_vs_peer_percentile"] = 0.5
                row["amount_peer_z"] = 0.0

            # Typology novelty: flag if typology is unusual for this segment
            if "segment_typology_freq" in peer_match.columns:
                seg_typo_freq = peer_match[peer_match.get("typology", pd.Series([""] * len(peer_match))) == typology]
                row["typology_novelty"] = 1.0 if seg_typo_freq.empty else 0.0
            else:
                row["typology_novelty"] = 0.0

            # Geographic deviation: flag unusual countries for this segment
            if "segment_common_countries" in peer_match.columns:
                common_countries_raw = peer_match["segment_common_countries"].iloc[0]
                common_countries = set(str(common_countries_raw).split(",")) if common_countries_raw else set()
                row["geo_deviation"] = 0.0 if country in common_countries else 1.0
            else:
                row["geo_deviation"] = 0.0

            rows.append(row)

        if not rows:
            return zeros

        result = pd.DataFrame(rows)
        for col in self.feature_names:
            if col not in result.columns:
                result[col] = 0.0
        return result[["alert_id"] + self.feature_names].reset_index(drop=True)

    def _intra_batch_peers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute peer features using the batch itself as the peer group.

        Used during training when external peer stats are unavailable.
        Each entity is compared against all other entities with the
        same segment label in the batch.
        """
        alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
        out = pd.DataFrame({"alert_id": alert_ids})
        for col in self.feature_names:
            out[col] = 0.0

        if df.empty or "amount" not in df.columns:
            return out

        amount = pd.to_numeric(df.get("amount", 0.0), errors="coerce").fillna(0.0)
        segment = df.get("segment", pd.Series(["retail"] * len(df))).astype(str)

        amount_vs_peer_pct = pd.Series(0.5, index=df.index)
        amount_z = pd.Series(0.0, index=df.index)

        for seg_val, seg_idx in df.groupby(segment).groups.items():
            seg_amounts = amount.loc[seg_idx]
            if len(seg_amounts) < 2:
                continue
            pct_rank = seg_amounts.rank(pct=True, method="average")
            mean = seg_amounts.mean()
            std = seg_amounts.std() + 1e-6
            amount_vs_peer_pct.loc[seg_idx] = pct_rank
            amount_z.loc[seg_idx] = (seg_amounts - mean) / std

        out["amount_vs_peer_percentile"] = amount_vs_peer_pct.values
        out["amount_peer_z"] = amount_z.values

        velocity = pd.to_numeric(df.get("txn_count_7d", df.get("num_transactions", 0.0)), errors="coerce").fillna(0.0)
        vel_pct = velocity.rank(pct=True, method="average")
        out["velocity_vs_peer_percentile"] = vel_pct.values

        return out

    def _zeros(self, alert_ids: list[str]) -> pd.DataFrame:
        out = pd.DataFrame({"alert_id": alert_ids})
        for name in self.feature_names:
            out[name] = 0.0
        return out
