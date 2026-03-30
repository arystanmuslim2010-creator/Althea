from __future__ import annotations

from typing import Any

import pandas as pd

from models.explainability_service import ExplainabilityService, get_explainability_service


class GovernanceExplainabilityService:
    """
    Governance adapter around the shared runtime explainability engine.

    This keeps existing governance imports/call-sites stable while ensuring
    governance and runtime paths use one explainability implementation.
    """

    def __init__(self, explainability_service: ExplainabilityService | None = None) -> None:
        self._service = explainability_service or get_explainability_service()

    def generate_explanation(
        self,
        model: Any,
        feature_frame: pd.DataFrame,
        fallback_contributions: list[dict[str, Any]] | None = None,
        model_version: str = "unknown",
        tenant_id: str | None = None,
        alert_id: str | None = None,
        feature_schema_version: str | None = None,
    ) -> dict[str, Any]:
        return self._service.generate_explanation(
            model=model,
            feature_frame=feature_frame,
            model_version=model_version,
            fallback_contributions=fallback_contributions,
            tenant_id=tenant_id,
            alert_id=alert_id,
            feature_schema_version=feature_schema_version,
        )

    def merge_into_alert_metadata(self, alert_payload: dict[str, Any], explanation: dict[str, Any]) -> dict[str, Any]:
        return self._service.merge_into_alert_metadata(alert_payload=alert_payload, explanation=explanation)
