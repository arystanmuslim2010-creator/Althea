"""
Generate a bank-alerts CSV with N rows for ingestion testing.
Uses the synthetic alert generator (with PILOT_TEST_MODE from config) and maps
columns to the bank CSV schema expected by IngestionService.

Usage (from backend folder):
  set PYTHONPATH=.
  python scripts/generate_bank_alerts_csv.py [--rows 1000]

Output: data/bank_alerts_1000.csv (or data/bank_alerts_<rows>.csv)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from types import SimpleNamespace

_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))
_root = _backend.parent
_data = _root / "data"
_data.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Generate bank alerts CSV for ingestion testing")
    parser.add_argument("--rows", type=int, default=1000, help="Number of alerts to generate (default 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output path (default: data/bank_alerts_<rows>.csv)")
    args = parser.parse_args()

    from src.synth_data import generate_synthetic_alerts
    from src import config

    n_rows = max(1, args.rows)
    cfg = SimpleNamespace(**{n: getattr(config, n) for n in dir(config) if n.isupper()})
    df = generate_synthetic_alerts(n_rows=n_rows, cfg=cfg, seed=args.seed)

    # Bank CSV schema: mandatory user_id, amount, segment, country, channel; time = timestamp_utc or time_gap
    # Include alert_risk_band so score stage can apply pilot override for balanced risk (LOW/MEDIUM/HIGH/CRITICAL)
    out = df[["alert_id", "user_id", "amount", "segment", "country", "typology", "source_system"]].copy()
    out["timestamp_utc"] = df["timestamp"]
    out["channel"] = "bank_transfer"
    out["time_gap"] = 86400
    out["num_transactions"] = 1
    if "alert_risk_band" in df.columns:
        out["alert_risk_band"] = df["alert_risk_band"]

    cols = [
        "alert_id", "user_id", "amount", "segment", "country", "channel",
        "timestamp_utc", "time_gap", "num_transactions", "typology", "source_system",
    ]
    if "alert_risk_band" in out.columns:
        cols.append("alert_risk_band")
    out = out[cols]

    out_path = Path(args.output) if args.output else _data / f"bank_alerts_{n_rows}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote {len(out)} alerts to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
