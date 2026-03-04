"""Abstract storage interface (Postgres-ready). Implementations: sqlite_adapter, postgres (skeleton)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd


class StorageBase(ABC):
    """Interface for alert/run persistence. SQLite and Postgres adapters implement this."""

    @abstractmethod
    def save_run(self, run_id: str, source: str, dataset_hash: str, row_count: int, notes: str = "") -> None:
        pass

    @abstractmethod
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def upsert_alerts(self, alerts: List[Dict[str, Any]], run_id: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def load_alerts_by_run(self, run_id: str) -> pd.DataFrame:
        pass

    def load_queue_by_run(self, run_id: str) -> pd.DataFrame:
        """Optional: load in_queue alerts for run. Default uses load_alerts_by_run + filter."""
        df = self.load_alerts_by_run(run_id)
        if df.empty or "in_queue" not in df.columns:
            return df
        return df[df["in_queue"] == True].sort_values("risk_score", ascending=False)
