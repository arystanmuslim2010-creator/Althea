"""
Integration test: run overlay pipeline on a small CSV and print counts by governance_status and in_queue.

Run from backend folder:
  set PYTHONPATH=.
  python scripts/test_pipeline_csv.py

Or double-click run_test_pipeline.bat in the backend folder.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure backend is on path (parent of scripts/)
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

# Use project root data dir (parent of backend)
_root = _backend.parent
_data = _root / "data"
_data.mkdir(parents=True, exist_ok=True)


def main():
    import pandas as pd
    from src.storage import get_storage
    from src.pipeline import run_pipeline

    # Create minimal CSV in memory
    csv_content = b"""user_id,amount,segment,country,channel,typology,time_gap,num_transactions
U1,100,retail_low,US,web,bank_alert,3600,5
U2,500,retail_high,GB,web,bank_alert,7200,3
U3,200,smb,DE,web,bank_alert,1800,10
"""
    storage = get_storage(str(_data / "app.db"))

    print("Running overlay pipeline (source=csv, in-memory bytes)...")
    run_id = run_pipeline(
        source="csv",
        input_bytes=csv_content,
        storage=storage,
        data_dir=_data,
        reports_dir=_data / "reports",
        dead_letter_dir=_data / "dead_letter",
    )
    print(f"run_id: {run_id}")

    df = storage.load_alerts_by_run(run_id)
    if df.empty:
        print("No alerts loaded for run.")
        return 1

    print("\nCounts by governance_status:")
    if "governance_status" in df.columns:
        print(df["governance_status"].value_counts().to_string())
    print("\nCounts by in_queue:")
    if "in_queue" in df.columns:
        print(df["in_queue"].value_counts().to_string())
    print(f"\nTotal alerts: {len(df)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
