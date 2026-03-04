"""Regenerate data/bank_alerts_1000.csv with alert_risk_band for balanced risk. Run from backend: python scripts/regenerate_bank_alerts.py"""
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend))
_root = _backend.parent
from types import SimpleNamespace
from src import config
from src.synth_data import generate_synthetic_alerts

cfg = SimpleNamespace(**{n: getattr(config, n) for n in dir(config) if n.isupper()})
df = generate_synthetic_alerts(n_rows=1000, cfg=cfg, seed=42)
out = df[["alert_id", "user_id", "amount", "segment", "country", "typology", "source_system"]].copy()
out["timestamp_utc"] = df["timestamp"]
out["channel"] = "bank_transfer"
out["time_gap"] = 86400
out["num_transactions"] = 1
if "alert_risk_band" in df.columns:
    out["alert_risk_band"] = df["alert_risk_band"]
cols = ["alert_id", "user_id", "amount", "segment", "country", "channel",
        "timestamp_utc", "time_gap", "num_transactions", "typology", "source_system"]
if "alert_risk_band" in out.columns:
    cols.append("alert_risk_band")
out = out[cols]
path = _root / "data" / "bank_alerts_1000.csv"
path.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(path, index=False, encoding="utf-8")
print("Wrote", len(out), "alerts to", path)
print("alert_risk_band:", out["alert_risk_band"].value_counts().to_dict())
