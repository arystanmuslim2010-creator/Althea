"""Case index — in-memory nearest-neighbor index for historical cases.

Implements a simple but effective L2/cosine nearest-neighbor search
using numpy, partitioned by outcome label so we can retrieve
'similar suspicious' and 'similar false positive' cases separately.

For production deployments with > 500k indexed cases, this can be
replaced with a FAISS or annoy backend by swapping CaseIndex for
a FAISS-backed implementation implementing the same interface.

The index is serializable (via joblib) and stored in object storage
so it can be loaded at inference time without re-building.
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from joblib import dump as joblib_dump
from joblib import load as joblib_load

from retrieval.case_vectorizer import CaseVectorizer

logger = logging.getLogger("althea.retrieval.case_index")


@dataclass
class IndexedCase:
    alert_id: str
    outcome_label: str   # "suspicious" | "false_positive" | "escalated" | "sar"
    typology: str
    amount: float
    risk_score: float
    resolution_hours: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchHit:
    case: IndexedCase
    similarity: float

    def __iter__(self):
        # Backward-compatible tuple unpacking: `case, sim = hit`.
        yield self.case
        yield self.similarity


class CaseIndex:
    """Partitioned nearest-neighbor index for historical AML cases.

    Vectors are stored per outcome partition so retrieval can target
    'similar suspicious cases' or 'similar FP cases' independently.

    Partitions:
        suspicious     — true_positive, escalated, confirmed_suspicious
        sar            — sar_filed
        false_positive — false_positive, benign_activity
    """

    PARTITIONS = ("suspicious", "sar", "false_positive")

    def __init__(self, vectorizer: CaseVectorizer | None = None) -> None:
        self._vectorizer = vectorizer or CaseVectorizer()
        # partition → (matrix [n, dim], list[IndexedCase])
        self._vectors: dict[str, np.ndarray] = {p: np.zeros((0, 0), dtype=np.float32) for p in self.PARTITIONS}
        self._cases: dict[str, list[IndexedCase]] = {p: [] for p in self.PARTITIONS}

    # ------------------------------------------------------------------
    # Index building
    # ------------------------------------------------------------------

    def build(self, payloads: list[dict[str, Any]], outcomes: list[dict[str, Any]]) -> None:
        """Build the index from alert payloads and their finalized outcomes.

        Parameters
        ----------
        payloads : list of alert payload dicts (alert_id, features_json, etc.)
        outcomes : list of outcome dicts (alert_id, analyst_decision, ...)
        """
        outcome_map = {str(o["alert_id"]): o for o in outcomes if o.get("alert_id")}
        payload_map = {str(p.get("alert_id", "")): p for p in payloads if p.get("alert_id")}

        partition_payloads: dict[str, list[dict[str, Any]]] = {p: [] for p in self.PARTITIONS}
        partition_cases: dict[str, list[IndexedCase]] = {p: [] for p in self.PARTITIONS}

        for alert_id, outcome in outcome_map.items():
            payload = payload_map.get(alert_id)
            if payload is None:
                continue

            partition = self._outcome_to_partition(str(outcome.get("analyst_decision", "")))
            if partition is None:
                continue

            case = IndexedCase(
                alert_id=alert_id,
                outcome_label=partition,
                typology=str(payload.get("typology") or ""),
                amount=float(payload.get("amount", 0.0) or 0.0),
                risk_score=float(payload.get("risk_score", 0.0) or 0.0),
                resolution_hours=None,
                metadata={
                    "analyst_decision": outcome.get("analyst_decision"),
                    "model_version": outcome.get("model_version"),
                    "risk_score_at_decision": outcome.get("risk_score_at_decision"),
                    "decision_reason": outcome.get("decision_reason"),
                },
            )
            partition_payloads[partition].append(payload)
            partition_cases[partition].append(case)

        for partition in self.PARTITIONS:
            if not partition_payloads[partition]:
                continue
            vecs = self._vectorizer.vectorize_batch(partition_payloads[partition])
            self._vectors[partition] = vecs
            self._cases[partition] = partition_cases[partition]

        total = sum(len(self._cases[p]) for p in self.PARTITIONS)
        logger.info(
            "CaseIndex built: total=%d suspicious=%d sar=%d false_positive=%d",
            total,
            len(self._cases["suspicious"]),
            len(self._cases["sar"]),
            len(self._cases["false_positive"]),
        )

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def search(
        self,
        query_vector: np.ndarray,
        partition: str = "suspicious",
        top_k: int = 5,
    ) -> list[SearchHit]:
        """Find the top-k nearest cases in the specified partition.

        Returns list of (IndexedCase, similarity_score) sorted by
        descending similarity.
        """
        if partition not in self.PARTITIONS:
            raise ValueError(f"Unknown partition '{partition}'. Choose from {self.PARTITIONS}")

        matrix = self._vectors.get(partition)
        cases = self._cases.get(partition, [])

        if matrix is None or len(matrix) == 0 or not cases:
            return []

        qv = query_vector.astype(np.float32).ravel()
        norm = np.linalg.norm(qv)
        if norm > 1e-9:
            qv = qv / norm

        # Cosine similarity via dot product (vectors are pre-normalized)
        similarities = matrix.dot(qv)
        top_k_actual = min(top_k, len(cases))
        top_indices = np.argsort(-similarities)[:top_k_actual]

        return [
            SearchHit(case=cases[i], similarity=float(similarities[i]))
            for i in top_indices
        ]

    def size(self, partition: str | None = None) -> int:
        if partition:
            return len(self._cases.get(partition, []))
        return sum(len(self._cases[p]) for p in self.PARTITIONS)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def serialize(self) -> bytes:
        buf = io.BytesIO()
        joblib_dump({"vectors": self._vectors, "cases": self._cases}, buf)
        return buf.getvalue()

    def load_from_bytes(self, data: bytes) -> None:
        state = joblib_load(io.BytesIO(data))
        self._vectors = state.get("vectors", {p: np.zeros((0, 0)) for p in self.PARTITIONS})
        self._cases = state.get("cases", {p: [] for p in self.PARTITIONS})

    @classmethod
    def deserialize(cls, data: bytes, vectorizer: CaseVectorizer | None = None) -> "CaseIndex":
        index = cls(vectorizer=vectorizer)
        index.load_from_bytes(data)
        return index

    # ------------------------------------------------------------------

    @staticmethod
    def _outcome_to_partition(decision: str) -> str | None:
        decision = decision.lower().strip()
        if decision == "sar_filed":
            return "sar"
        if decision in {"true_positive", "escalated", "confirmed_suspicious"}:
            return "suspicious"
        if decision in {"false_positive", "benign_activity"}:
            return "false_positive"
        return None
