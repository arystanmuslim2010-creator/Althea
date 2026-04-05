"""Case retriever — execute nearest-neighbor queries and format results.

Combines CaseVectorizer + CaseIndex to produce structured retrieval
results suitable for:
- Explanation layer (similar suspicious / FP evidence)
- Analyst UI (display prior similar cases)
- Decision layer (similar suspicious case strength signal)
"""
from __future__ import annotations

from typing import Any

import numpy as np

from retrieval.case_index import CaseIndex
from retrieval.case_vectorizer import CaseVectorizer


class CaseRetriever:
    """Execute similarity queries against the case index."""

    def __init__(
        self,
        case_index: CaseIndex,
        vectorizer: CaseVectorizer | None = None,
        default_top_k: int = 5,
        min_similarity: float = 0.3,
    ) -> None:
        self._index = case_index
        self._vectorizer = vectorizer or CaseVectorizer()
        self._default_top_k = default_top_k
        self._min_similarity = min_similarity

    def retrieve_similar(
        self,
        alert_payload: dict[str, Any],
        top_k: int | None = None,
        partitions: tuple[str, ...] = ("suspicious", "sar", "false_positive"),
    ) -> dict[str, Any]:
        """Retrieve similar cases across specified outcome partitions.

        Parameters
        ----------
        alert_payload : alert payload dict (same format as alert record)
        top_k         : maximum cases to return per partition
        partitions    : which outcome partitions to query

        Returns
        -------
        dict with keys:
            similar_suspicious : list of similar escalated/TP cases
            similar_sar        : list of similar SAR-filed cases
            similar_fp         : list of similar false-positive cases
            query_alert_id     : str
            retrieval_summary  : dict with counts and mean similarity
        """
        k = top_k or self._default_top_k
        query_vec = self._vectorizer.vectorize(alert_payload)
        alert_id = str(alert_payload.get("alert_id", ""))

        results: dict[str, Any] = {
            "query_alert_id": alert_id,
            "similar_suspicious": [],
            "similar_sar": [],
            "similar_fp": [],
        }

        partition_key_map = {
            "suspicious": "similar_suspicious",
            "sar": "similar_sar",
            "false_positive": "similar_fp",
        }

        for partition in partitions:
            if partition not in partition_key_map:
                continue
            matches = self._index.search(query_vec, partition=partition, top_k=k)
            formatted = [
                self._format_match(case, sim, partition)
                for case, sim in (self._coerce_match(match) for match in matches)
                if case is not None and sim >= self._min_similarity and case.alert_id != alert_id
            ]
            results[partition_key_map[partition]] = formatted

        # Retrieval summary
        all_suspicious = results["similar_suspicious"] + results["similar_sar"]
        mean_sim = (
            float(np.mean([r["similarity"] for r in all_suspicious]))
            if all_suspicious else 0.0
        )
        results["retrieval_summary"] = {
            "suspicious_count": len(results["similar_suspicious"]),
            "sar_count": len(results["similar_sar"]),
            "fp_count": len(results["similar_fp"]),
            "mean_suspicious_similarity": round(mean_sim, 4),
            "has_similar_suspicious": len(all_suspicious) > 0,
            "index_size": self._index.size(),
        }

        return results

    def get_suspicious_strength(self, alert_payload: dict[str, Any]) -> float:
        """Return a scalar [0, 1] representing strength of similar suspicious cases.

        Used as a single signal in the decision policy ranking formula.
        """
        k = min(self._default_top_k, 3)
        query_vec = self._vectorizer.vectorize(alert_payload)

        # Combine suspicious + SAR partitions
        sus_matches = self._index.search(query_vec, partition="suspicious", top_k=k)
        sar_matches = self._index.search(query_vec, partition="sar", top_k=k)

        all_sims = [
            sim
            for case, sim in (self._coerce_match(match) for match in (sus_matches + sar_matches))
            if case is not None and sim >= self._min_similarity
        ]
        if not all_sims:
            return 0.0

        # Weight SAR matches more heavily than regular suspicious matches
        sar_sims = [
            sim * 1.3
            for case, sim in (self._coerce_match(match) for match in sar_matches)
            if case is not None and sim >= self._min_similarity
        ]
        sus_sims = [
            sim
            for case, sim in (self._coerce_match(match) for match in sus_matches)
            if case is not None and sim >= self._min_similarity
        ]
        combined = sar_sims + sus_sims
        return float(min(np.mean(combined) if combined else 0.0, 1.0))

    @staticmethod
    def _format_match(case, similarity: float, partition: str) -> dict[str, Any]:
        return {
            "alert_id": case.alert_id,
            "outcome_label": case.outcome_label,
            "typology": case.typology,
            "amount": case.amount,
            "risk_score": case.risk_score,
            "resolution_hours": case.resolution_hours,
            "similarity": round(float(similarity), 4),
            "analyst_decision": case.metadata.get("analyst_decision"),
            "decision_reason": case.metadata.get("decision_reason"),
            "risk_score_at_decision": case.metadata.get("risk_score_at_decision"),
        }

    @staticmethod
    def _coerce_match(match: Any) -> tuple[Any | None, float]:
        if match is None:
            return None, 0.0
        case = getattr(match, "case", None)
        sim = getattr(match, "similarity", None)
        if case is not None and sim is not None:
            return case, float(sim)
        if isinstance(match, (tuple, list)) and len(match) >= 2:
            return match[0], float(match[1])
        return None, 0.0
