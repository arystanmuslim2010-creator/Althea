"""
Determinism test: run same input twice, assert run_id identical, dataset_hash, config_hash,
and per-alert governance_status and risk_score identical.

Run from backend folder:
  set PYTHONPATH=.
  python scripts/test_determinism.py
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

    # Same CSV content twice
    csv_content = b"""user_id,amount,segment,country,channel,typology,time_gap,num_transactions
U1,100,retail_low,US,web,bank_alert,3600,5
U2,500,retail_high,GB,web,bank_alert,7200,3
U3,200,smb,DE,web,bank_alert,1800,10
"""
    config = {"policy_version": "1.0", "governance": {"daily_budget": 10}}

    run_id_1 = run_pipeline(source="csv", input_obj=csv_content, config=config)
    run_id_2 = run_pipeline(source="csv", input_obj=csv_content, config=config)

    storage = get_storage(str(_data / "app.db"))
    r1 = storage.get_run(run_id_1)
    r2 = storage.get_run(run_id_2)
    a1 = storage.get_run_artifacts(run_id_1)
    a2 = storage.get_run_artifacts(run_id_2)

    df1 = storage.load_alerts_by_run(run_id_1)
    df2 = storage.load_alerts_by_run(run_id_2)

    errors = []
    # Deterministic run_id: same input + config => same run_id
    if run_id_1 != run_id_2:
        errors.append(f"run_id must be identical for same input+config: {run_id_1!r} vs {run_id_2!r}")
    if r1["dataset_hash"] != r2["dataset_hash"]:
        errors.append(f"dataset_hash mismatch: {r1['dataset_hash']} vs {r2['dataset_hash']}")
    if a1 and a2 and a1.get("config_hash") != a2.get("config_hash"):
        errors.append(f"config_hash mismatch: {a1.get('config_hash')} vs {a2.get('config_hash')}")
    if len(df1) != len(df2):
        errors.append(f"alert count mismatch: {len(df1)} vs {len(df2)}")

    # Per-alert: same alert_id set, same governance_status and risk_score per alert_id
    set1 = set(df1["alert_id"].astype(str).tolist())
    set2 = set(df2["alert_id"].astype(str).tolist())
    if set1 != set2:
        errors.append(f"alert_id set mismatch: {set1 ^ set2}")
    else:
        for aid in set1:
            row1 = df1[df1["alert_id"].astype(str) == aid].iloc[0]
            row2 = df2[df2["alert_id"].astype(str) == aid].iloc[0]
            g1 = str(row1.get("governance_status", ""))
            g2 = str(row2.get("governance_status", ""))
            if g1 != g2:
                errors.append(f"alert_id {aid} governance_status mismatch: {g1!r} vs {g2!r}")
            rs1 = float(row1.get("risk_score", 0))
            rs2 = float(row2.get("risk_score", 0))
            if abs(rs1 - rs2) > 1e-9:
                errors.append(f"alert_id {aid} risk_score mismatch: {rs1} vs {rs2}")

    if errors:
        print("Determinism check FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print("Determinism check passed: run_id identical, dataset_hash/config_hash identical, "
          "per-alert governance_status and risk_score identical across two runs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
