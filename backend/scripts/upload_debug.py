#!/usr/bin/env python
"""Test where files are being saved during upload."""
import os
import requests
import json

BASE_URL = os.getenv("ALTHEA_API_URL", "http://localhost:8000")
TENANT = os.getenv("ALTHEA_DEFAULT_TENANT_ID", "default-bank")
EMAIL = os.getenv("ALTHEA_TEST_EMAIL", "analyst@bank.com")
PASSWORD = os.getenv("ALTHEA_TEST_PASSWORD", "")

print("=" * 70)
print("TEST UPLOAD - Debug file storage location")
print("=" * 70 + "\n")

# Step 1: Login
print("[1] Logging in...")
try:
    if not PASSWORD:
        raise RuntimeError("Set ALTHEA_TEST_PASSWORD before running this script.")
    login_resp = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"email": EMAIL, "password": PASSWORD},
        headers={"X-Tenant-ID": TENANT},
        timeout=5
    )

    if login_resp.status_code != 200:
        print(f"[FAIL] Login failed: {login_resp.text}")
        exit(1)

    token = login_resp.json()["access_token"]
    print("[OK] Logged in\n")
except Exception as e:
    print(f"[FAIL] Login error: {e}")
    exit(1)

headers = {
    "Authorization": f"Bearer {token}",
    "X-Tenant-ID": TENANT
}

# Step 2: Upload small test file
print("[2] Uploading test CSV...")
try:
    # Create a small test CSV
    test_csv = "id,name,value\n1,test,100\n2,test2,200\n"

    files = {"file": ("test.csv", test_csv.encode())}
    upload_resp = requests.post(
        f"{BASE_URL}/api/data/upload-csv",
        headers=headers,
        files=files,
        timeout=10
    )

    print(f"   Response status: {upload_resp.status_code}")
    print(f"   Response body: {upload_resp.text}\n")

    if upload_resp.status_code == 200:
        upload_data = upload_resp.json()
        dataset_hash = upload_data.get("dataset_hash")
        print(f"[OK] Upload returned hash: {dataset_hash}\n")

        # Now check where the file was saved
        print("[3] Checking file location...")
        locations_to_check = [
            f"../data/object_storage/datasets/{TENANT}/public/{dataset_hash}.csv",
            f"data/object_storage/datasets/{TENANT}/public/{dataset_hash}.csv",
            f"../data/datasets/{TENANT}/public/{dataset_hash}.csv",
            f"data/datasets/{TENANT}/public/{dataset_hash}.csv",
        ]

        found = False
        for loc in locations_to_check:
            abs_path = os.path.abspath(loc)
            if os.path.exists(loc):
                print(f"   [FOUND] {abs_path}")
                found = True
            else:
                print(f"   [NOT FOUND] {abs_path}")

        if not found:
            print("\n   [DEBUG] Checking what directories exist...")
            for base_dir in ["../data", "data"]:
                if os.path.exists(base_dir):
                    print(f"   {os.path.abspath(base_dir)} exists")
                    try:
                        for root, dirs, files in os.walk(base_dir):
                            level = root.replace(base_dir, '').count(os.sep)
                            indent = ' ' * 2 * level
                            print(f"   {indent}{os.path.basename(root)}/")
                            subindent = ' ' * 2 * (level + 1)
                            for file in files[:5]:  # Limit output
                                print(f"   {subindent}{file}")
                            if level > 3:  # Limit depth
                                break
                    except:
                        pass
    else:
        print(f"[FAIL] Upload failed\n")

except Exception as e:
    print(f"[FAIL] Upload error: {e}")
    import traceback
    traceback.print_exc()
