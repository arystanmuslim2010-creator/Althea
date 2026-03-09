"""Shim: re-export from legacy. Canonical rule engine is src.rule_engine."""
from __future__ import annotations

from .legacy.rules_engine import run_rule_engine  # noqa: F401

__all__ = ["run_rule_engine"]
