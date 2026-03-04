"""SQLite storage adapter (default). Wraps src.storage.Storage."""
from __future__ import annotations

from typing import Optional

from ..storage import Storage, get_storage

__all__ = ["get_storage", "Storage"]
