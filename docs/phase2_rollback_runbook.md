# Phase 2 Rollback Runbook: Alert-Centric Ingestion

## Immediate Rollback Actions

1. Disable alert-centric ingestion flags:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false`
2. Keep/restore conservative validation controls:
   - `ALTHEA_STRICT_INGESTION_VALIDATION=false`
   - `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS=1000` (or baseline value)
3. Stop initiating new JSONL uploads until verification is complete.

## Service Health Verification

1. Verify API startup and `/health` endpoint are stable.
2. Verify queue/status endpoints still respond:
   - `/api/pipeline/jobs/{job_id}`
   - `/api/run-info`
3. Confirm workers are alive and heartbeat checks are healthy.

## Legacy Path Safety Verification

1. Run legacy ingestion with known sample dataset (`upload-bank-csv` / existing CSV path).
2. Confirm pipeline executes and alerts remain retrievable.
3. Confirm no regression in core alert and case APIs.

## Recent Ingestion Inspection

1. Inspect recent JSONL ingestion summaries for:
   - `status` (`failed_validation`, `rejected`, `partially_ingested`)
   - mismatched success vs persisted counts
   - repeated reason categories in validation failures
2. Inspect rollout metrics:
   - `ingestion_failure_total`
   - `ingestion_validation_failure_total`
   - `ingestion_data_quality_inconsistency_total`
   - `ingestion_duration_ms`

## App Version Revert (If Needed)

1. If failures persist, revert to prior stable app version.
2. Keep additive DB schema changes in place unless an explicit and tested DB rollback plan exists.

## Post-Rollback Exit Criteria

1. Health endpoints stable.
2. Legacy ingestion confirmed working.
3. Core queue/detail endpoints stable.
4. No new JSONL ingestion attempts accepted while feature is disabled.

## Notes

1. Additive schema from Phase 1/2 can remain in place during rollback.
2. Do not perform destructive DB rollback during active incident response unless already proven safe.
# Historical note: this Phase 2 rollback runbook is retained for audit trail.
# Current emergency steps are in docs/final_migration_runbook.md.
