"""Domain dataclasses for AML overlay pipeline (auditability, reproducibility)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RawAlert:
    """Single raw alert as ingested (before normalization)."""
    data: Dict[str, Any]
    row_index: int = 0
    source_file: str = ""


@dataclass
class NormalizedAlert:
    """Alert conforming to normalized schema (required fields present)."""
    alert_id: str
    entity_id: str  # user_id
    source_system: str
    timestamp: str
    risk_score_source: str  # e.g. "ml_calibrated"
    typology: str
    vendor_metadata: Dict[str, Any]
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Context:
    """Context built before scoring (behavioral baseline, history, peer, external)."""
    alert_id: str
    behavioral_baseline: Dict[str, Any] = field(default_factory=dict)
    historical_alerts: Dict[str, Any] = field(default_factory=dict)  # count, prior_dispositions
    peer_comparison: Dict[str, Any] = field(default_factory=dict)  # segment_percentile, etc.
    external_signals: Dict[str, Any] = field(default_factory=dict)  # risk_country, sanctions_flags

    def to_json(self) -> Dict[str, Any]:
        return {
            "behavioral_baseline": self.behavioral_baseline,
            "historical_alerts": self.historical_alerts,
            "peer_comparison": self.peer_comparison,
            "external_signals": self.external_signals,
        }


@dataclass
class MLSignals:
    """ML model output signals (raw prob, contributions)."""
    risk_prob: float = 0.0
    risk_score_raw: float = 0.0
    risk_score: float = 0.0
    risk_band: str = ""
    contributions: Dict[str, float] = field(default_factory=dict)
    base_prob: float = 0.0
    external_priors: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RuleEvidence:
    """Standardized rule evidence (source + version when from external)."""
    rule_id: str
    name: str
    severity: str
    triggered: bool
    evidence: Dict[str, Any]
    external_source: str = ""
    external_version: str = ""


@dataclass
class RuleHit:
    """Single rule hit with evidence."""
    rule_id: str
    name: str
    severity: str
    triggered: bool
    evidence: RuleEvidence


@dataclass
class GovernanceDecision:
    """Governance outcome per alert."""
    governance_status: str  # eligible | suppressed | MANDATORY_REVIEW
    suppression_code: str = ""
    suppression_reason: str = ""
    in_queue: bool = False
    policy_version: str = ""
    hard_constraint_hit: bool = False
    hard_code: str = ""


@dataclass
class DecisionTrace:
    """Full decision trace: input -> features -> model -> rules -> governance -> outcome."""
    alert_id: str
    run_id: str
    input_summary: Dict[str, Any] = field(default_factory=dict)
    features_summary: Dict[str, Any] = field(default_factory=dict)
    model_output: Dict[str, Any] = field(default_factory=dict)
    rules: List[Dict[str, Any]] = field(default_factory=list)
    governance: Dict[str, Any] = field(default_factory=dict)
    outcome: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "run_id": self.run_id,
            "input_summary": self.input_summary,
            "features_summary": self.features_summary,
            "model_output": self.model_output,
            "rules": self.rules,
            "governance": self.governance,
            "outcome": self.outcome,
            "schema_version": self.schema_version,
        }


@dataclass
class RunRecord:
    """Run registry record."""
    run_id: str
    source: str
    dataset_hash: str
    row_count: int
    created_at: str
    notes: str = ""
    schema_version: str = "1.0"
