"""Check governance and constraints in the latest run. Run from project root: py backend.scripts.check_constraints"""
import sqlite3
from pathlib import Path

# Project root = parent of backend/
_ROOT = Path(__file__).resolve().parent.parent.parent
_DB = _ROOT / "data" / "app.db"

con = sqlite3.connect(str(_DB))
cur = con.cursor()

rid = cur.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()[0]
print("active_run:", rid)

# Check how many suppressed/mandatory/eligible
print("governance_status counts:",
      cur.execute("SELECT governance_status, COUNT(*) FROM alerts WHERE run_id=? GROUP BY governance_status", (rid,)).fetchall())

# Check suppression codes
print("top suppression codes:",
      cur.execute("""SELECT suppression_code, COUNT(*)
                     FROM alerts WHERE run_id=? AND suppression_code IS NOT NULL AND suppression_code!=''
                     GROUP BY suppression_code ORDER BY COUNT(*) DESC LIMIT 20""", (rid,)).fetchall())

# Check if there are rule hits for high risk / sanctions rules (if written)
print("rules present sample:",
      cur.execute("SELECT alert_id, rules_json FROM alerts WHERE run_id=? AND rules_json IS NOT NULL AND rules_json!='' LIMIT 3", (rid,)).fetchall())
