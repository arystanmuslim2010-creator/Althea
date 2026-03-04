"""
Test that hard constraints (e.g. sanctions hit) override governance and are never suppressed.
Run from backend folder: python scripts/test_hard_constraints_override.py
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


def main() -> int:
    from src.pipeline.run_pipeline import run_pipeline
    from src.storage import get_storage

    # CSV: one alert with country KP (sanctions hit per sanctions_v1.json), one with US (normal).
    # Use strict daily_budget=1 so that without hard-constraint override, both could be suppressed by budget.
    csv_content = b"""user_id,amount,segment,country,channel,typology,time_gap,num_transactions
U_SANCTIONS,5000,retail,KP,web,bank_alert,3600,5
U_NORMAL,100,retail,US,web,bank_alert,7200,3
"""
    config = {
        "policy_version": "1.0",
        "governance": {"daily_budget": 1},  # only 1 slot: normal alert would get it; sanctions must still be in_queue
    }

    run_id = run_pipeline(source="csv", input_obj=csv_content, config=config)
    storage = get_storage(str(_data / "app.db"))
    df = storage.load_alerts_by_run(run_id)
    if df.empty:
        print("FAIL: No alerts returned for run")
        return 1

    # Find hard-constraint alert(s) (sanctions hit = country KP in our CSV; persisted as hard_constraint=1)
    if "hard_constraint" not in df.columns:
        print("FAIL: No hard_constraint column in alerts")
        return 1
    hard_df = df[df["hard_constraint"].fillna(0).astype(int) == 1]
    if hard_df.empty:
        print("FAIL: No alert with hard_constraint=1 (expected one sanctions-hit alert for country KP)")
        return 1

    row = hard_df.iloc[0]
    gov_status = str(row.get("governance_status", "")).strip()
    in_queue_val = row.get("in_queue")
    in_queue = bool(in_queue_val) if in_queue_val is not None else (int(in_queue_val) == 1 if isinstance(in_queue_val, (int, float)) else False)
    suppressed = gov_status.lower() == "suppressed"

    errors = []
    if gov_status.upper() != "MANDATORY_REVIEW":
        errors.append(f"Hard-constraint alert must have governance_status MANDATORY_REVIEW (or mandatory_review), got: {gov_status!r}")
    if not in_queue:
        errors.append(f"Hard-constraint alert must have in_queue=1 (True), got: {in_queue_val!r}")
    if suppressed:
        errors.append("Hard-constraint alert must NOT be suppressed")

    if errors:
        print("Hard constraints override test FAILED:")
        for e in errors:
            print("  -", e)
        print("  alert_id:", row.get("alert_id"), "governance_status:", gov_status, "in_queue:", in_queue_val)
        return 1

    print("Hard constraints override test passed: hard-constraint (sanctions-hit) alert has MANDATORY_REVIEW and in_queue=1, not suppressed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
