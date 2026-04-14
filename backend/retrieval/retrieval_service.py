"""Retrieval service — manages index lifecycle and exposes retrieval API.

Responsibilities:
- Build / rebuild the case index from historical outcomes
- Persist index to object storage for fast loading at inference time
- Serve similar-case queries for explanation and decision signals
- Keep the index fresh via incremental updates

The index is stored at ``retrieval/{tenant_id}/case_index.bin`` in
object storage and loaded on first use per tenant.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from retrieval.case_index import CaseIndex
from retrieval.case_retriever import CaseRetriever
from retrieval.case_vectorizer import CaseVectorizer

logger = logging.getLogger("althea.retrieval.service")

_INDEX_URI_TEMPLATE = "retrieval/{tenant_id}/case_index.bin"
_INDEX_META_TEMPLATE = "retrieval/{tenant_id}/case_index_meta.json"


class RetrievalService:
    """Manage case index lifecycle and provide retrieval interface."""

    def __init__(
        self,
        repository,
        object_storage,
        vectorizer: CaseVectorizer | None = None,
        min_similarity: float = 0.3,
        top_k: int = 5,
    ) -> None:
        self._repository = repository
        self._storage = object_storage
        self._vectorizer = vectorizer or CaseVectorizer()
        self._min_similarity = min_similarity
        self._top_k = top_k
        # per-tenant index cache (loaded lazily)
        self._index_cache: dict[str, CaseIndex] = {}

    # ------------------------------------------------------------------
    # Index lifecycle
    # ------------------------------------------------------------------

    def build_index(
        self,
        tenant_id: str,
        limit: int = 50_000,
    ) -> dict[str, Any]:
        """Build the case index for a tenant from stored alerts and outcomes.

        Fetches all finalized outcomes and their corresponding alert payloads,
        vectorizes them, and saves the serialized index to object storage.
        """
        from sqlalchemy import text

        outcomes: list[dict[str, Any]] = []
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT alert_id, analyst_decision, decision_reason,
                           model_version, risk_score_at_decision
                    FROM alert_outcomes
                    WHERE tenant_id = :tid
                    ORDER BY timestamp DESC
                    LIMIT :limit
                    """
                ),
                {"tid": tenant_id, "limit": limit},
            ).fetchall()
            outcomes = [
                {
                    "alert_id": str(r[0]),
                    "analyst_decision": str(r[1]),
                    "decision_reason": r[2],
                    "model_version": r[3],
                    "risk_score_at_decision": r[4],
                }
                for r in rows
            ]

        if not outcomes:
            return {"status": "no_outcomes", "indexed": 0}

        alert_ids = [o["alert_id"] for o in outcomes]
        payloads_filtered = self._repository.list_latest_alert_payloads_for_alert_ids(
            tenant_id=tenant_id,
            alert_ids=alert_ids,
            limit=limit,
        )
        if not payloads_filtered:
            logger.info(
                json.dumps(
                    {
                        "event": "case_index_build_skipped",
                        "tenant_id": tenant_id,
                        "reason": "no_alert_payloads_for_outcomes",
                        "outcome_count": len(outcomes),
                    },
                    ensure_ascii=True,
                )
            )
            return {"status": "no_payloads", "indexed": 0, "outcomes": len(outcomes)}

        index = CaseIndex(vectorizer=self._vectorizer)
        index.build(payloads=payloads_filtered, outcomes=outcomes)

        # Persist index
        index_uri = _INDEX_URI_TEMPLATE.format(tenant_id=tenant_id)
        self._storage.put_bytes(index_uri, index.serialize())

        meta = {
            "tenant_id": tenant_id,
            "indexed_cases": index.size(),
            "suspicious": index.size("suspicious"),
            "sar": index.size("sar"),
            "false_positive": index.size("false_positive"),
        }
        meta_uri = _INDEX_META_TEMPLATE.format(tenant_id=tenant_id)
        self._storage.put_json(meta_uri, meta)

        # Update in-memory cache
        self._index_cache[tenant_id] = index

        logger.info(
            json.dumps(
                {"event": "case_index_built", "tenant_id": tenant_id, **meta},
                ensure_ascii=True,
            )
        )
        return {"status": "ok", **meta}

    def get_retriever(self, tenant_id: str) -> CaseRetriever:
        """Return a CaseRetriever backed by the tenant's case index."""
        index = self._get_or_load_index(tenant_id)
        return CaseRetriever(
            case_index=index,
            vectorizer=self._vectorizer,
            default_top_k=self._top_k,
            min_similarity=self._min_similarity,
        )

    def retrieve_similar_cases(
        self,
        tenant_id: str,
        alert_payload: dict[str, Any],
        top_k: int | None = None,
    ) -> dict[str, Any]:
        """Convenience method — build retriever and execute query."""
        retriever = self.get_retriever(tenant_id)
        return retriever.retrieve_similar(alert_payload, top_k=top_k)

    def get_suspicious_strength(
        self,
        tenant_id: str,
        alert_payload: dict[str, Any],
    ) -> float:
        """Return similar-suspicious strength score [0, 1] for a single alert."""
        retriever = self.get_retriever(tenant_id)
        return retriever.get_suspicious_strength(alert_payload)

    def index_meta(self, tenant_id: str) -> dict[str, Any]:
        """Return metadata about the current index for this tenant."""
        meta_uri = _INDEX_META_TEMPLATE.format(tenant_id=tenant_id)
        try:
            return self._storage.get_json(meta_uri) or {}
        except Exception:
            return {}

    # ------------------------------------------------------------------

    def _get_or_load_index(self, tenant_id: str) -> CaseIndex:
        if tenant_id in self._index_cache:
            return self._index_cache[tenant_id]

        index_uri = _INDEX_URI_TEMPLATE.format(tenant_id=tenant_id)
        try:
            data = self._storage.get_bytes(index_uri)
            index = CaseIndex.deserialize(data, vectorizer=self._vectorizer)
            self._index_cache[tenant_id] = index
            logger.info("Loaded case index for tenant %s: size=%d", tenant_id, index.size())
            return index
        except Exception:
            logger.info("No case index found for tenant %s — returning empty index", tenant_id)
            empty = CaseIndex(vectorizer=self._vectorizer)
            self._index_cache[tenant_id] = empty
            return empty
