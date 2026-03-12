#!/usr/bin/env python
"""Complete setup: Login, upload CSV, run pipeline, save token."""
import requests
import time
import json

BASE_URL = "http://localhost:8000"
TENANT = "default-bank"

print("=" * 60)
print("ALTHEA SETUP & RUN")
print("=" * 60)

# Step 1: Login
print("\n🔐 Step 1: Logging in...")
try:
    login_resp = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"email": "analyst@bank.com", "password": "Password123!"},
        headers={"X-Tenant-ID": TENANT},
        timeout=5
    )
    if login_resp.status_code != 200:
        print(f"❌ Login failed: {login_resp.text}")
        exit(1)

    token = login_resp.json()["access_token"]
    print(f"✅ Logged in successfully!")
    print(f"\n📌 YOUR TOKEN:\n{token}\n")

    # Save token to file
    with open("TOKEN.txt", "w") as f:
        f.write(token)
    print(f"💾 Token saved to: TOKEN.txt\n")

except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("Make sure backend is running on Terminal 1")
    exit(1)

headers = {
    "Authorization": f"Bearer {token}",
    "X-Tenant-ID": TENANT
}

# Step 2: Upload CSV
print("📤 Step 2: Uploading sample_data.csv...")
try:
    with open("sample_data.csv", "rb") as f:
        files = {"file": f}
        upload_resp = requests.post(
            f"{BASE_URL}/api/data/upload-csv",
            headers=headers,
            files=files,
            timeout=10
        )
    if upload_resp.status_code != 200:
        print(f"❌ Upload failed: {upload_resp.text}")
        exit(1)

    dataset_info = upload_resp.json()
    print(f"✅ CSV uploaded!")
    print(f"   Dataset hash: {dataset_info.get('dataset_hash')}")
    print(f"   Rows: {dataset_info.get('row_count')}\n")

except Exception as e:
    print(f"❌ Upload failed: {e}")
    exit(1)

# Step 3: Run Pipeline
print("⚙️ Step 3: Starting pipeline...")
try:
    pipeline_resp = requests.post(
        f"{BASE_URL}/api/pipeline/run",
        headers=headers,
        json={},
        timeout=10
    )
    if pipeline_resp.status_code != 200:
        print(f"❌ Pipeline start failed: {pipeline_resp.text}")
        exit(1)

    pipeline_data = pipeline_resp.json()
    run_id = pipeline_data.get("run_id")
    print(f"✅ Pipeline started!")
    print(f"   Run ID: {run_id}\n")

except Exception as e:
    print(f"❌ Pipeline start failed: {e}")
    exit(1)

# Step 4: Wait for completion
print("⏳ Step 4: Waiting for worker to process alerts...")
print("   (This may take 10-30 seconds...)\n")

completed = False
for i in range(60):
    time.sleep(1)
    try:
        check_resp = requests.get(
            f"{BASE_URL}/api/alerts",
            headers=headers,
            timeout=5
        )
        if check_resp.status_code == 200:
            alerts = check_resp.json().get("alerts", [])
            if alerts:
                print(f"✅ Pipeline complete!")
                print(f"   Generated {len(alerts)} alerts\n")
                completed = True
                break
    except:
        pass

    # Show progress
    if (i + 1) % 5 == 0:
        print(f"   Still processing... ({i+1}s)")

if not completed:
    print(f"⚠️ Pipeline still running (or worker not responding)")
    print(f"   Check Terminal 3 (Worker) for status\n")

# Summary
print("=" * 60)
print("✅ SETUP COMPLETE!")
print("=" * 60)
print(f"\n🔑 YOUR LOGIN TOKEN:\n{token}\n")
print("📱 Frontend: http://localhost:5173")
print("📚 API Docs: http://localhost:8000/docs\n")
print("👤 Login Credentials:")
print("   Email: analyst@bank.com")
print("   Password: Password123!\n")
print("=" * 60)

# Save all info to file
info = {
    "token": token,
    "email": "analyst@bank.com",
    "password": "Password123!",
    "frontend_url": "http://localhost:5173",
    "api_url": "http://localhost:8000",
    "tenant_id": TENANT,
    "run_id": run_id
}

with open("LOGIN_INFO.json", "w") as f:
    json.dump(info, f, indent=2)

print(f"💾 All info saved to: LOGIN_INFO.json")
