# Phase 4 Cutover Plan

## What Changed
- Added primary ingestion selector:
  - `ALTHEA_PRIMARY_INGESTION_MODE=legacy|alert_jsonl`
- Default primary mode is now `alert_jsonl`.
- Added unified endpoint:
  - `POST /api/data/upload`
  - Routes to legacy or alert JSONL path based on primary mode.
- Added runtime admin switch for immediate cutover/rollback:
  - `POST /internal/ingestion/primary-mode`
- Added cutover monitoring metrics:
  - `primary_ingestion_mode`
  - `ingestion_path_used_total`
  - `alerts_ingested_per_mode`

## Primary Mode Behavior
- `ALTHEA_PRIMARY_INGESTION_MODE=alert_jsonl`
  - Unified upload route defaults to alert-centric ingestion.
  - Legacy endpoints remain available.
- `ALTHEA_PRIMARY_INGESTION_MODE=legacy`
  - Unified upload route defaults to legacy ingestion behavior.
- Optional per-request override (testing):
  - `POST /api/data/upload?ingestion_mode=legacy|alert_jsonl`

## Runtime Mode Switch
- Admin endpoint:
  - `POST /internal/ingestion/primary-mode`
  - Body: `{ "mode": "legacy" }` or `{ "mode": "alert_jsonl" }`
- Purpose:
  - Immediate application-level switch without schema change.
  - Fast rollback path during incidents.

## Monitoring Checklist During Cutover
1. Verify `primary_ingestion_mode` metric reflects expected mode.
2. Check `ingestion_path_used_total` trend by mode and status.
3. Check `alerts_ingested_per_mode` volume split.
4. Confirm no spike in:
   - `ingestion_failure_total`
   - `ingestion_validation_failure_total`
   - API 5xx errors
5. Confirm queue depth/worker health stays stable.
6. Confirm alert queue/detail/explain endpoints continue to load for both new and historical alerts.

## Validation Checklist
1. `POST /api/data/upload` works with primary mode set to `alert_jsonl`.
2. `POST /api/data/upload` works with primary mode set to `legacy`.
3. Existing endpoints still work:
   - `/api/data/upload-alert-jsonl`
   - `/api/data/upload-bank-csv`
   - `/api/data/upload-csv`
4. Alert detail and explainability endpoints remain functional.
5. No data corruption signs in recent ingestion runs.

## Legacy Path Status
- Legacy ingestion is now secondary, but still supported.
- When legacy ingestion is used while primary mode is `alert_jsonl`, the backend logs a warning for operator visibility.
# Historical note: this Phase 4 cutover plan is retained for audit trail.
# Post-finalization operation is documented in docs/final_migration_runbook.md.
