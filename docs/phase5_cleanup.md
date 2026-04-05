# Phase 5 Cleanup and Hardening

## Summary
Phase 5 focused on post-cutover hardening and controlled legacy deprecation, without risky removals that could break production contracts.

## What Changed
- Added legacy ingestion deprecation control:
  - `ALTHEA_ENABLE_LEGACY_INGESTION=true|false` (finalized default `false`)
- Added deprecation usage metric:
  - `legacy_ingestion_usage_total{endpoint,status}`
- Added upload-size safety limit:
  - `ALTHEA_INGESTION_MAX_UPLOAD_BYTES` (default `10485760`)
- Added schema/index hardening migration for alert-centric query paths.
- Added repository fallback index creation for migration-less/legacy local DBs.

## Legacy Deprecation Status
- Legacy ingestion endpoints remain as compatibility stubs.
- Legacy ingestion is explicitly marked as deprecated in routing flow via warning logs.
- Legacy path is now hard-disabled by default and only available via emergency override.

## Safe Removal Decision
- No legacy code was removed in this phase because usage must be validated first.
- Removal is deferred until `legacy_ingestion_usage_total` confirms sustained near-zero usage.

## Performance and Stability Improvements
- JSONL upload row counting no longer decodes full file text for counting.
- Ingestion failure row capture is bounded to prevent unbounded memory growth on malformed files.
- Upload-size checks are applied before ingestion processing to avoid oversized payload pressure.
- Added alert index hardening for `ingestion_run_id`, `source_system`, and `(tenant_id, run_id, created_at)`.

## Config Simplification Notes
- `ALTHEA_ALERT_JSONL_ROLLOUT_MODE` was removed from active runtime config.
- Primary behavior remains controlled by `ALTHEA_PRIMARY_INGESTION_MODE`.

## Architecture State (Post-Phase 5)
- Primary ingestion: alert-centric JSONL.
- Legacy ingestion: disabled by default, measured, and emergency-only.
- Rollback capability: retained (runtime primary mode switch + legacy enable flag).
- API contracts: preserved.
