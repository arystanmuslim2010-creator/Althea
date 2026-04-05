# Final Migration Stabilization Checks

Use this checklist after hard-disabling legacy ingestion (`ALTHEA_ENABLE_LEGACY_INGESTION=false`) and before deleting any additional legacy code.

## 1. Configuration and Startup

- Confirm API and worker start successfully with:
  - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`
  - `ALTHEA_ENABLE_LEGACY_INGESTION=false`
  - `ALTHEA_PRIMARY_INGESTION_MODE=alert_jsonl`
- Confirm API and worker use identical values for the three flags above.
- Confirm startup logs show legacy ingestion disabled and alert JSONL active.

## 2. Legacy Hard-Disable Verification

- Call legacy endpoints (`/api/data/upload-csv`, `/api/data/upload-bank-csv`).
- Verify `503` structured response:
  - `error=legacy_ingestion_disabled`
  - message indicates post-migration finalization disable.
- Verify blocked attempts are counted in metrics:
  - `legacy_path_access_attempt_total`
  - `legacy_path_access_blocked_total`

## 3. Primary Ingestion Health

- Upload a known-good alert JSONL sample through:
  - `/api/data/upload-alert-jsonl`
  - `/api/data/upload` (default path)
- Verify ingestion status is `accepted` (or expected validation outcome).
- Verify summary contains counts (`total_rows`, `success_count`, `failed_count`, `warning_count`, `elapsed_ms`).

## 4. Core Backend Regression Checks

- Verify `/health` remains healthy/degraded without crashes.
- Verify queue endpoint behavior (`/api/work/queue`) remains valid.
- Verify alert detail/investigation endpoints remain valid.
- Verify explainability and workflow actions remain functional.
- Verify no 5xx spike after hard-disable window.

## 5. Operational Status Review

- Read `/internal/migration/finalization-status`.
- Confirm:
  - `legacy_disabled=true`
  - `blocked_legacy_attempts_recent` is low and explainable
  - `new_ingestion_healthy=true`
  - `recent_ingestion_failure_runs` not trending upward
  - no meaningful rise in `ingestion_data_quality_inconsistency_total`

## 6. Worker and Data Quality Stability

- Confirm workers continue processing without crash loops.
- Check ingestion metrics:
  - `ingestion_failure_total`
  - `ingestion_validation_failure_total`
  - `ingestion_data_quality_inconsistency_total`
- Confirm recent ingestion runs persist expected alert counts.

