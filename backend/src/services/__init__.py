"""Service layer facade exports for the AML app.

Lazy imports keep package import side effects minimal and avoid loading
unrelated legacy modules unless explicitly requested.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "FeatureService",
    "ScoringService",
    "CaseService",
    "OpsService",
    "MissingColumnsError",
    "IngestionService",
    "IngestionError",
]


def __getattr__(name: str) -> Any:
    if name in {"FeatureService", "MissingColumnsError"}:
        from .feature_service import FeatureService, MissingColumnsError

        return {"FeatureService": FeatureService, "MissingColumnsError": MissingColumnsError}[name]
    if name == "ScoringService":
        from .scoring_service import ScoringService

        return ScoringService
    if name == "CaseService":
        from .case_service import CaseService

        return CaseService
    if name == "OpsService":
        from .ops_service import OpsService

        return OpsService
    if name in {"IngestionService", "IngestionError"}:
        from .ingestion_service import IngestionError, IngestionService

        return {"IngestionService": IngestionService, "IngestionError": IngestionError}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
