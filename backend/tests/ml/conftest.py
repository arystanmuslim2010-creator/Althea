"""Shared fixtures for the ML test suite."""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the backend root is on sys.path
_backend = Path(__file__).resolve().parent.parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))
