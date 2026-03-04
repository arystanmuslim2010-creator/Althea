"""Postgres storage adapter (skeleton). Not required to run locally."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from .base import StorageBase


class PostgresStorage(StorageBase):
    """Skeleton for Postgres. Implement when deploying to Postgres."""

    def __init__(self, dsn: str = ""):
        self.dsn = dsn

    def save_run(self, run_id: str, source: str, dataset_hash: str, row_count: int, notes: str = "") -> None:
        raise NotImplementedError("Postgres adapter not implemented; use SQLite for local run.")

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("Postgres adapter not implemented.")

    def upsert_alerts(self, alerts: List[Dict[str, Any]], run_id: Optional[str] = None) -> None:
        raise NotImplementedError("Postgres adapter not implemented.")

    def load_alerts_by_run(self, run_id: str) -> pd.DataFrame:
        raise NotImplementedError("Postgres adapter not implemented.")
