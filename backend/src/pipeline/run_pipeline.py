"""
Single entry point for running the AML overlay pipeline.
run_pipeline(source, input_obj, config) -> run_id.
Delegates to orchestrator.run_pipeline with storage, data_dir, reports_dir, dead_letter_dir.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from .orchestrator import run_pipeline as _run_pipeline
from ..storage import get_storage
from .. import config as app_config


def run_pipeline(
    source: str,
    input_obj: Union[str, Path, bytes, pd.DataFrame],
    config: Dict[str, Any],
) -> str:
    """
    Run the full pipeline. Returns run_id.

    Args:
        source: One of "csv", "json", "dataframe", "bank_alerts_csv".
        input_obj: Path (str or Path), raw bytes, or DataFrame.
        config: Config overrides (e.g. policy_version, governance.daily_budget).

    Returns:
        run_id: Unique run identifier.
    """
    data_dir = Path(getattr(app_config, "DATA_DIR", "data"))
    reports_dir = data_dir / "reports"
    dead_letter_dir = data_dir / "dead_letter"
    db_path = str(data_dir / "app.db")
    storage = get_storage(db_path)

    input_path: Optional[Path] = None
    input_df: Optional[pd.DataFrame] = None
    input_bytes: Optional[bytes] = None

    if isinstance(input_obj, pd.DataFrame):
        input_df = input_obj
        if source not in ("dataframe", "bank_alerts_csv"):
            source = "dataframe"
    elif isinstance(input_obj, (str, Path)):
        input_path = Path(input_obj)
        if source == "bank_alerts_csv":
            source = "csv"  # orchestrator uses csv path; ingest uses bank_alerts via IngestionService when cols match
    elif isinstance(input_obj, bytes):
        input_bytes = input_obj
        if source == "bank_alerts_csv":
            source = "csv"
    else:
        raise TypeError("input_obj must be str, Path, bytes, or DataFrame")

    return _run_pipeline(
        source=source,
        config_overrides=config,
        input_path=input_path,
        input_df=input_df,
        input_bytes=input_bytes,
        storage=storage,
        data_dir=data_dir,
        reports_dir=reports_dir,
        dead_letter_dir=dead_letter_dir,
    )
