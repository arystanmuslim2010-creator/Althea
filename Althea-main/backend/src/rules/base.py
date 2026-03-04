"""Base types for AML modular rules: RuleResult schema and RuleOutput (legacy)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class RuleResult:
    """
    Canonical rule output: JSON-serializable, with severity and version.
    score is in [0, 1] across all rules.
    """
    rule_id: str
    rule_version: str
    hit: bool
    severity: str  # INFO | LOW | MEDIUM | HIGH | CRITICAL
    score: float
    evidence: Dict[str, Any]
    thresholds: Dict[str, Any]
    window: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """JSON-serializable dict; all values must be JSON-serializable."""
        return {
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "hit": self.hit,
            "severity": self.severity,
            "score": round(float(self.score), 6),
            "evidence": {k: _json_safe(v) for k, v in self.evidence.items()},
            "thresholds": {k: _json_safe(v) for k, v in self.thresholds.items()},
            "window": {k: _json_safe(v) for k, v in self.window.items()},
        }


def _json_safe(val: Any) -> Any:
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, (list, tuple)):
        return [_json_safe(x) for x in val]
    if isinstance(val, dict):
        return {str(k): _json_safe(v) for k, v in val.items()}
    try:
        return float(val)
    except (TypeError, ValueError):
        return str(val)


@dataclass(frozen=True)
class RuleOutput:
    """Legacy: column names for rule hit/score/evidence."""
    hit_col: str
    score_col: str
    evidence_col: str
    name: str
