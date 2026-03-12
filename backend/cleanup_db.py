#!/usr/bin/env python
"""Clean up stale runtime contexts and prepare database for fresh pipeline run."""
import sqlite3
from datetime import datetime, timezone

db_path = "data/althea.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("🧹 Cleaning database...\n")

# 1. Clear stale runtime contexts
print("1️⃣ Clearing stale runtime contexts...")
cursor.execute("DELETE FROM runtime_context")
print(f"   ✓ Cleared runtime_context table")

# 2. Clear stale pipeline runs (keep last 5)
print("2️⃣ Clearing old pipeline runs...")
cursor.execute("""
    DELETE FROM pipeline_runs
    WHERE id NOT IN (
        SELECT id FROM pipeline_runs
        ORDER BY created_at DESC
        LIMIT 5
    )
""")
print(f"   ✓ Kept last 5 pipeline runs")

# 3. Clear failed pipeline jobs
print("3️⃣ Clearing failed jobs...")
cursor.execute("DELETE FROM pipeline_runs WHERE status IN ('failed', 'error')")
print(f"   ✓ Removed failed jobs")

# 4. Reset runtime context for fresh start
print("4️⃣ Creating fresh runtime context...")
cursor.execute("""
    INSERT INTO runtime_context (
        id, tenant_id, user_scope, actor,
        active_run_id, updated_at
    ) VALUES (
        'fresh-start-001',
        'default-bank',
        'public',
        'system',
        NULL,
        ?
    )
""", (datetime.now(timezone.utc).isoformat(),))
print(f"   ✓ Created fresh runtime context")

conn.commit()
conn.close()

print("\n" + "=" * 60)
print("✅ DATABASE CLEANED!")
print("=" * 60)
print("\nNow ready for fresh pipeline run.\n")
