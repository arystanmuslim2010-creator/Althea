from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class FeatureDependency:
    feature_name: str
    depends_on: str


@dataclass(slots=True)
class FeatureDefinition:
    name: str
    dtype: str
    description: str
    source: str = "alerts"
    owner: str = "feature-engineering"
    default_value: Any = None
    tags: list[str] = field(default_factory=list)
