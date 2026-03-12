#!/usr/bin/env python
"""Simplified pipeline execution that handles everything correctly."""
import requests
import json
import time
import os
from pathlib import Path

BASE_URL = "http://localhost:8000"
TENANT = "default-bank"
CSV_FILE = "sample_data.csv"

print("=" * 70)
print("ALTHEA SIMPLE PIPELINE RUN")
print("=" * 70)

# Step 0: Verify CSV exists
if not os.path.exists(CSV_FILE):
    print(f"[FAIL] ERROR: {CSV_FILE} not found!")
    print(f"   Expected at: {Path(CSV_FILE).absolute()}")
    exit(1)

csv_size = os.path.getsize(CSV_FILE)
print(f"\n[DONE] Found {CSV_FILE} ({csv_size} bytes)")

# Step 1: Login
print("\n" + "-" * 70)
print("STEP 1: LOGIN")
print("-" * 70)

try:
    login_resp = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"email": "analyst@bank.com", "password": "Password123!"},
        headers={"X-Tenant-ID": TENANT},
        timeout=5
    )

    if login_resp.status_code != 200:
        print(f"[FAIL] Login failed: {login_resp.text}")
        exit(1)

    token = login_resp.json()["access_token"]
    print(f"[OK] Logged in successfully")
    print(f"\n[TOKEN] {token}\n")

    # Save token
    with open("TOKEN.txt", "w") as f:
        f.write(token)
    print(f"[SAVE] Saved to TOKEN.txt")

except Exception as e:
    print(f"[FAIL] Connection error: {e}")
    print("   Make sure backend is running on Terminal 1!")
    exit(1)

headers = {
    "Authorization": f"Bearer {token}",
    "X-Tenant-ID": TENANT
}

# Step 2: Upload CSV
print("\n" + "-" * 70)
print("STEP 2: UPLOAD CSV")
print("-" * 70)

try:
    with open(CSV_FILE, "rb") as f:
        files = {"file": f}
        upload_resp = requests.post(
            f"{BASE_URL}/api/data/upload-csv",
            headers=headers,
            files=files,
            timeout=10
        )

    if upload_resp.status_code != 200:
        print(f"[FAIL] Upload failed: {upload_resp.text}")
        exit(1)

    upload_data = upload_resp.json()
    dataset_hash = upload_data.get("dataset_hash")
    row_count = upload_data.get("row_count")

    print(f"[OK] CSV uploaded successfully")
    print(f"   Dataset Hash: {dataset_hash}")
    print(f"   Row Count: {row_count}")

except Exception as e:
    print(f"[FAIL] Upload error: {e}")
    exit(1)

# Step 3: Verify file exists in correct location
print("\n" + "-" * 70)
print("STEP 3: VERIFY FILE STORAGE")
print("-" * 70)

expected_file = f"../data/object_storage/datasets/{TENANT}/public/{dataset_hash}.csv"
if os.path.exists(expected_file):
    print(f"[OK] File found at correct location:")
    print(f"   {expected_file}")
else:
    print(f"[WARN] File not found at expected location:")
    print(f"   {expected_file}")
    # Check alternative location
    alt_file = f"data/data/models/datasets/{TENANT}/public/{dataset_hash}.csv"
    if os.path.exists(alt_file):
        print(f"   Found at alternate (old) location: {alt_file}")
        print(f"   This is outdated - use the object_storage location")

# Step 4: Run Pipeline
print("\n" + "-" * 70)
print("STEP 4: START PIPELINE")
print("-" * 70)

try:
    pipeline_resp = requests.post(
        f"{BASE_URL}/api/pipeline/run",
        headers=headers,
        json={},
        timeout=10
    )

    if pipeline_resp.status_code != 200:
        print(f"[FAIL] Pipeline start failed: {pipeline_resp.text}")
        exit(1)

    pipeline_data = pipeline_resp.json()
    run_id = pipeline_data.get("run_id")
    status = pipeline_data.get("status")

    print(f"[OK] Pipeline started")
    print(f"   Run ID: {run_id}")
    print(f"   Status: {status}")

except Exception as e:
    print(f"[FAIL] Pipeline error: {e}")
    exit(1)

# Step 5: Wait for completion
print("\n" + "-" * 70)
print("STEP 5: WAITING FOR PROCESSING")
print("-" * 70)

print(f"[WAIT] Waiting for worker to process...")
print(f"   (Check Terminal 2 for worker output)\n")

alerts_generated = 0
max_wait = 60
check_interval = 2

for elapsed in range(0, max_wait, check_interval):
    time.sleep(check_interval)

    try:
        check_resp = requests.get(
            f"{BASE_URL}/api/alerts",
            headers=headers,
            timeout=5
        )

        if check_resp.status_code == 200:
            alerts = check_resp.json().get("alerts", [])
            if alerts and len(alerts) > alerts_generated:
                alerts_generated = len(alerts)
                print(f"   Generated {alerts_generated} alerts...")

                if alerts_generated > 0:
                    print(f"\n[OK] Pipeline complete!")
                    print(f"   Generated {alerts_generated} alerts")
                    break
    except:
        pass

if alerts_generated == 0:
    print(f"[WARN] No alerts generated yet (pipeline may still be running)")
    print(f"   Check Terminal 2 (Worker) for detailed output")

# Step 6: Summary
print("\n" + "=" * 70)
print("[OK] SETUP COMPLETE!")
print("=" * 70)

print(f"\n[URL] Frontend: http://localhost:5173")
print(f"[DOCS] API Docs: http://localhost:8000/docs")
print(f"\n[LOGIN] Login:")
print(f"   Email: analyst@bank.com")
print(f"   Password: Password123!")

print(f"\n[KEY] Your Token:")
print(f"   {token}")

print(f"\n[STATS] Generated Alerts: {alerts_generated}")
print(f"[ID] Run ID: {run_id}")

# Save all info
info = {
    "token": token,
    "email": "analyst@bank.com",
    "password": "Password123!",
    "frontend_url": "http://localhost:5173",
    "api_url": "http://localhost:8000",
    "tenant_id": TENANT,
    "run_id": run_id,
    "dataset_hash": dataset_hash,
    "alerts_generated": alerts_generated,
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
}

with open("LOGIN_INFO.json", "w") as f:
    json.dump(info, f, indent=2)

print(f"\n[SAVE] All info saved to LOGIN_INFO.json")
print("\n" + "=" * 70)
