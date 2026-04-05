"""Case vectorizer — convert investigation cases to fixed-length vectors.

Uses a deterministic feature-based vectorization approach rather than
deep learning embeddings so the system works without GPU infrastructure
and remains fully auditable (each dimension is a named feature).

A case vector is derived from the alert's stored feature snapshot plus
metadata signals (typology, amount, country, outcome).
"""
from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd

# Feature fields extracted from the alert payload for vectorization.
# Order matters — must be consistent between index-time and query-time.
_VECTOR_FIELDS = [
    ("amount_log1p", float, 0.0),
    ("typology_code", float, 0.0),
    ("country_risk", float, 0.3),
    ("segment_code", float, 0.0),
    ("txn_count_7d", float, 0.0),
    ("amount_sum_7d", float, 0.0),
    ("cross_border_ratio", float, 0.0),
    ("round_amount_ratio", float, 0.0),
    ("dormant_reactivation", float, 0.0),
    ("prior_alert_count", float, 0.0),
    ("prior_escalation_rate", float, 0.0),
    ("prior_sar_rate", float, 0.0),
    ("graph_degree", float, 0.0),
    ("suspicious_neighbor_ratio", float, 0.0),
    ("fan_out_score", float, 0.0),
    ("community_risk_score", float, 0.0),
    ("risk_score", float, 0.0),
]

VECTOR_DIM = len(_VECTOR_FIELDS)


class CaseVectorizer:
    """Convert alert/case payloads to fixed-dimension float32 vectors."""

    def vectorize(self, payload: dict[str, Any]) -> np.ndarray:
        """Convert a single alert payload dict to a feature vector."""
        vec = np.zeros(VECTOR_DIM, dtype=np.float32)

        features: dict[str, Any] = {}
        # Features may be stored nested under 'features_json' or flat
        raw_features = payload.get("features_json") or {}
        if isinstance(raw_features, str):
            import json
            try:
                raw_features = json.loads(raw_features)
            except Exception:
                raw_features = {}
        features.update(raw_features if isinstance(raw_features, dict) else {})
        # Also check top-level payload fields
        features.update({k: v for k, v in payload.items() if k != "features_json"})

        for i, (field, dtype, default) in enumerate(_VECTOR_FIELDS):
            raw = features.get(field, default)
            if field == "typology_code" and (raw is None or raw == default):
                raw = self._typology_code(features.get("typology"))
            try:
                val = float(raw) if raw is not None else default
                if not np.isfinite(val):
                    val = default
            except (TypeError, ValueError):
                val = default
            val = self._normalize_feature(field, val)
            vec[i] = val

        # L2-normalize for cosine similarity computation via dot product
        norm = np.linalg.norm(vec)
        if norm > 1e-9:
            vec = vec / norm

        return vec

    def vectorize_batch(self, payloads: list[dict[str, Any]]) -> np.ndarray:
        """Vectorize a list of payloads. Returns shape (n, VECTOR_DIM)."""
        if not payloads:
            return np.zeros((0, VECTOR_DIM), dtype=np.float32)
        return np.vstack([self.vectorize(p) for p in payloads]).astype(np.float32)

    @staticmethod
    def feature_names() -> list[str]:
        return [f for f, _, _ in _VECTOR_FIELDS]

    @staticmethod
    def _typology_code(typology: Any) -> float:
        text = str(typology or "").strip().lower()
        if not text:
            return 0.0
        digest = hashlib.sha1(text.encode("utf-8")).digest()
        # Deterministic [0, 1] scalar.
        return int.from_bytes(digest[:4], "big") / float(2**32 - 1)

    @staticmethod
    def _normalize_feature(field: str, value: float) -> float:
        if field in {"amount_log1p", "amount_sum_7d"}:
            return float(np.log1p(max(value, 0.0)) / 10.0)
        if field == "typology_code":
            return float(np.clip(value, 0.0, 1.0) * 3.0)
        if field in {"risk_score", "community_risk_score"}:
            return float(np.clip(value / 100.0, 0.0, 1.0))
        if field in {"prior_alert_count", "txn_count_7d", "graph_degree"}:
            return float(np.clip(value / 20.0, 0.0, 1.0))
        if field in {"prior_escalation_rate", "prior_sar_rate", "country_risk", "cross_border_ratio", "round_amount_ratio", "suspicious_neighbor_ratio", "fan_out_score"}:
            return float(np.clip(value, 0.0, 1.0))
        return float(value)
