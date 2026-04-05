"""Feature builder sub-package.

Each builder produces a distinct slice of the feature space:
    AlertFeatureBuilder     — alert-native signals
    BehaviorFeatureBuilder  — rolling window behavioral features
    HistoryFeatureBuilder   — historical outcome features
    PeerFeatureBuilder      — peer deviation features
    CostFeatureBuilder      — investigation cost / complexity features
"""
from features.builders.alert_features import AlertFeatureBuilder
from features.builders.behavior_features import BehaviorFeatureBuilder
from features.builders.cost_features import CostFeatureBuilder
from features.builders.history_features import HistoryFeatureBuilder
from features.builders.peer_features import PeerFeatureBuilder

__all__ = [
    "AlertFeatureBuilder",
    "BehaviorFeatureBuilder",
    "HistoryFeatureBuilder",
    "PeerFeatureBuilder",
    "CostFeatureBuilder",
]
