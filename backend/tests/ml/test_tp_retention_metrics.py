"""Test TP retention and suppression metrics."""
import pytest
import numpy as np
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from src.ml.metrics import (
    tp_retention_at_suppression,
    suppression_at_tp_retention,
    precision_at_k_percent,
    pr_auc,
)


def test_tp_retention_at_suppression():
    # Perfect score: all TP ranked first -> suppressing bottom 50% keeps all TP
    y = np.array([1, 1, 0, 0])
    score = np.array([0.9, 0.8, 0.3, 0.2])  # TPs top
    ret = tp_retention_at_suppression(y, score, 0.5)
    assert ret == 1.0

    # Suppress 50%: keep top 2; both are TP -> 100% retention
    ret2 = tp_retention_at_suppression(y, score, 0.5)
    assert ret2 == 1.0

    # If we suppress 75%, we keep only 1 alert; 1 TP in top 1 -> 0.5 retention (1 of 2 TPs)
    ret3 = tp_retention_at_suppression(y, score, 0.75)
    assert ret3 == 0.5


def test_suppression_at_tp_retention():
    y = np.array([1, 1, 0, 0, 0])
    score = np.array([0.9, 0.8, 0.4, 0.3, 0.2])
    # To keep 100% TP we can suppress bottom 3 (60%)
    s = suppression_at_tp_retention(y, score, retention_target=1.0)
    assert s >= 0.4 and s <= 0.7  # can suppress at least 40%


def test_precision_at_k_percent():
    y = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0])  # 2 TP in 10
    score = np.array([0.9, 0.8, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])  # top 2 = TP
    p10 = precision_at_k_percent(y, score, k=0.1)
    assert p10 == 1.0
    p20 = precision_at_k_percent(y, score, k=0.2)
    assert p20 == 1.0
