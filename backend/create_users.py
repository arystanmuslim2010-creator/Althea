#!/usr/bin/env python
"""Create test users in the database."""
import sqlite3
import uuid
from datetime import datetime, timezone
from core.security import hash_password

def create_test_users():
    """Create 4 test users in SQLite database."""

    # Connect to database
    conn = sqlite3.connect('data/althea.db')
    cursor = conn.cursor()

    # Test users
    users = [
        {
            "email": "analyst@bank.com",
            "password": "Password123!",
            "role": "analyst",
            "team": "analysis-team"
        },
        {
            "email": "investigator@bank.com",
            "password": "Password123!",
            "role": "investigator",
            "team": "investigation-team"
        },
        {
            "email": "manager@bank.com",
            "password": "Password123!",
            "role": "manager",
            "team": "management"
        },
        {
            "email": "admin@bank.com",
            "password": "Password123!",
            "role": "admin",
            "team": "admin"
        },
    ]

    print("Creating test users...\n")

    for user in users:
        user_id = uuid.uuid4().hex
        email = user["email"]
        password_hash = hash_password(user["password"])
        role = user["role"]
        team = user["team"]
        tenant_id = "default-bank"
        now = datetime.now(timezone.utc).isoformat()

        try:
            cursor.execute("""
                INSERT INTO users (id, tenant_id, email, password_hash, role, team, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, tenant_id, email, password_hash, role, team, 1, now))

            print(f"[OK] {email} (role: {role})")
            print(f"     Password: {user['password']}\n")

        except sqlite3.IntegrityError:
            print(f"[EXISTS] {email} already exists (skipping)\n")
        except Exception as e:
            print(f"[FAIL] Failed to create {email}: {e}\n")

    conn.commit()
    conn.close()

    print("=" * 50)
    print("Done! Users ready to login.")
    print("=" * 50)

if __name__ == "__main__":
    create_test_users()
