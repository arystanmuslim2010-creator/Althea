"""
Integration test: run overlay pipeline in Bank Alerts CSV (overlay) mode.
Ingest data/bank_alerts_template.csv (or sample), run pipeline, print counts by in_queue/governance_status,
confirm decision_trace_json is not null.

Run from backend folder:
  set PYTHONPATH=.
  python scripts/test_pipeline_bank_alerts.py

Or from project root:
  python -m backend.scripts.test_pipeline_bank_alerts  (if backend is a package)
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))
_root = _backend.parent
_data = _root / "data"
_data.mkdir(parents=True, exist_ok=True)


def main():
    from src.pipeline.run_pipeline import run_pipeline
    from src.storage import get_storage

    csv_path = _data / "bank_alerts_template.csv"
    if not csv_path.is_file():
        # Create minimal bank-alerts CSV inline
        csv_path.write_text(
            "alert_id,entity_id,user_id,created_at,source_system,typology,vendor_risk_score,amount,country,segment,channel,vendor_metadata\n"
            "BA-001,ENT-001,U001,2024-01-15T10:00:00Z,CORE_AML,structuring,72,15000,US,retail,wire,\"{}\"\n"
            "BA-002,ENT-002,U002,2024-01-15T11:30:00Z,CORE_AML,rapid_withdraw,,8500,GB,corporate,atm,\"{}\"\n"
            "BA-003,ENT-003,U003,2024-01-16T09:00:00Z,VENDOR_X,dormant,45,2000,DE,retail,pos,\"{}\"\n",
            encoding="utf-8",
        )

    config = {"policy_version": "1.0", "governance": {"daily_budget": None}}
    print("Running overlay pipeline (Bank Alerts CSV mode)...")
    run_id = run_pipeline(source="csv", input_obj=str(csv_path), config=config)
    print(f"run_id: {run_id}")

    storage = get_storage(str(_data / "app.db"))
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

    # Confirm decision_trace_json not null
    if "decision_trace_json" in df.columns:
        null_traces = df["decision_trace_json"].isna() | (df["decision_trace_json"].astype(str).str.strip() == "")
        if null_traces.all():
            print("\nFAIL: decision_trace_json is null/empty for all alerts.")
            return 1
        print(f"\nAlerts with decision_trace_json: {(~null_traces).sum()} / {len(df)}")
    else:
        print("\nWARN: decision_trace_json column missing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
