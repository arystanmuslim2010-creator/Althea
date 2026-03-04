"""Check sanctions-related alerts. Run from project root: py backend.scripts.check_sanctions"""
import sqlite3
from pathlib import Path

# Project root = parent of backend/
_ROOT = Path(__file__).resolve().parent.parent.parent
_DB = _ROOT / "data" / "app.db"

con = sqlite3.connect(str(_DB))
cur = con.cursor()

rows = cur.execute("""
SELECT governance_status, in_queue, suppression_code
FROM alerts
WHERE suppression_code LIKE '%SANCTION%'
   OR suppression_reason LIKE '%sanction%'
LIMIT 10
""").fetchall()

print(rows)
