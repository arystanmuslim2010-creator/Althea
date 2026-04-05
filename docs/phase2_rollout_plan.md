# Phase 2 Rollout Plan: Controlled Alert-Centric Ingestion

## Prerequisites

1. Phase 1 migration and compatibility changes are already deployed.
2. Alembic schema is at least `20260405_0009`.
3. Legacy ingestion (`upload-bank-csv`, CSV pipeline flow) is currently healthy.
4. API and worker components use identical values for:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT`
   - `ALTHEA_STRICT_INGESTION_VALIDATION`
   - `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS`

## Enablement Steps

1. Keep defaults in production-safe mode:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false`
2. For staging canary, enable only JSONL path:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false`
3. Keep strict mode on for first canary:
   - `ALTHEA_STRICT_INGESTION_VALIDATION=true`
4. Keep canary row limit small:
   - `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS=1000` (or lower for first test batches).

## Small Dataset Canary Procedure

1. Upload a small JSONL file through `/api/data/upload-alert-jsonl`.
2. Validate summary fields:
   - `total_rows`
   - `success_count`
   - `failed_count`
   - `warning_count`
   - `strict_mode_used`
   - `source_system`
   - `elapsed_ms`
   - `status`
3. Verify status progression:
   - expected pass: `accepted`
   - expected strict reject: `failed_validation`
   - expected non-strict mixed quality: `partially_ingested`
4. Confirm no impact on legacy ingestion and core health endpoints.

## Verification Metrics

Track these metrics during rollout:

- `ingestion_attempt_total`
- `ingestion_success_total`
- `ingestion_failure_total`
- `ingestion_validation_failure_total`
- `ingestion_warning_total`
- `ingestion_duration_ms`
- `ingested_alert_count`
- `ingested_transaction_count`
- `ingestion_data_quality_inconsistency_total`

## Full Rollout Criteria

Promote beyond canary only if all hold:

1. Stable success ratio (`ingestion_success_total` vs `ingestion_failure_total`).
2. Validation failures are low and understood.
3. No sustained latency regression in `ingestion_duration_ms`.
4. No worker instability tied to alert-centric ingestion path.
5. Legacy ingestion remains unaffected.

## Abort Criteria

Abort and roll back when any repeated/severe signal appears:

1. Spike in ingestion 5xx responses.
2. Repeated strict validation failures or malformed payload spikes.
3. Inconsistent persisted counts vs ingestion summary counts.
4. Significant ingestion latency regression.
5. Worker failures linked to new JSONL path.
6. Unexpected IBM AMLSim import attempts while IBM flag remains disabled.

## Operational Notes

1. IBM AMLSim import remains separately guarded by `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT`.
2. `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS` is the app-level canary limiter for safe first batches.
3. If reverse proxy upload/body limits or request timeouts are used in deployment, tune them explicitly for JSONL rollout and keep them documented with the same canary sizing assumptions.
# Historical note: this Phase 2 document is retained for audit trail.
# Current operational guidance is in docs/final_migration_runbook.md and docs/final_architecture_overview.md.
