"""Versioned external risk data layer (bank-grade, hash-verified, reproducible)."""
from __future__ import annotations

from .loader import load_external_source, load_all_configured_sources
from .constraints import (
    compute_external_flags_and_versions,
    external_versions_snapshot,
    high_risk_countries_for_rule,
)

__all__ = [
    "load_external_source",
    "load_all_configured_sources",
    "compute_external_flags_and_versions",
    "external_versions_snapshot",
    "high_risk_countries_for_rule",
]
