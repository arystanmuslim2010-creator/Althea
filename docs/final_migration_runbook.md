# Final Migration Runbook

This runbook covers production operation after migration finalization where alert-centric ingestion is the only normal path.

## A. Verify Stabilization

1. Confirm config:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`
   - `ALTHEA_ENABLE_LEGACY_INGESTION=false`
   - `ALTHEA_PRIMARY_INGESTION_MODE=alert_jsonl`
2. Check `/internal/migration/finalization-status`.
3. Check `/health`, queue, and alert detail endpoints.
4. Validate one known-good JSONL upload path end-to-end.

## B. Interpret Blocked Legacy Attempts

- Expected low-level noise:
  - stale scripts
  - operator mistakes
  - health probes hitting old endpoints
- Investigate if blocked attempts are sustained or increasing:
  - identify endpoint from `legacy_access_by_endpoint`
  - identify caller/source from logs tagged `deprecated_path_access`
  - remove stale callers instead of re-enabling legacy ingestion

## C. Abort Criteria

Abort finalization and move to emergency fallback if any of these hold:

- sustained 5xx increase tied to ingestion path
- repeated ingestion validation failures without dataset changes
- ingestion failure trend increase (`ingestion_failure_total`)
- material rise in data-quality inconsistency metrics
- worker instability or repeated ingestion-related crashes
- queue/detail endpoint regressions after hard-disable

## D. Emergency Fallback (Temporary Only)

Use only when production impact is active and immediate mitigation is needed.

1. Temporarily set:
   - `ALTHEA_ENABLE_LEGACY_INGESTION=true`
2. If required, set:
   - `ALTHEA_PRIMARY_INGESTION_MODE=legacy`
3. Restart API/worker with aligned values.
4. Re-check:
   - `/health`
   - queue/detail endpoints
   - ingestion success/failure metrics
5. Open incident and track exit plan back to:
   - `ALTHEA_ENABLE_LEGACY_INGESTION=false`
   - `ALTHEA_PRIMARY_INGESTION_MODE=alert_jsonl`

Emergency fallback is not normal operation and must be time-boxed.

