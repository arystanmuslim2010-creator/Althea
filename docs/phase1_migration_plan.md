# Phase 1 Migration Plan: Alert-Centric Ingestion Preparation

## What Changed

- Added an additive Alembic migration (`20260405_0009`) for alert-ingestion preparation fields.
- Added rollout flags with safe defaults so new ingestion behavior is off by default.
- Added compatibility-safe repository handling for new nullable alert fields.
- Added structured `503` behavior for disabled alert JSONL ingestion route.
- Added startup logging for Phase 1 rollout flag state and active schema/migration visibility.

## New Flags Added

- `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`
- `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false`
- `ALTHEA_ENABLE_HUMAN_INTERPRETATION=true`
- `ALTHEA_STRICT_INGESTION_VALIDATION=false`

Notes:
- Invalid explicit boolean values for these flags fail startup with clear validation errors.
- JSONL ingestion route stays registered, but returns structured `503` when disabled.

## DB Migration Added

- Alembic revision: `20260405_0009_phase1_alert_ingestion_prep.py`
- Down revision: `20260402_0008`
- Additive nullable columns on `alerts` (only if missing):
  - `raw_payload_json`
  - `source_system`
  - `ingestion_run_id`
  - `schema_version`
  - `evaluation_label_is_sar`
  - `ingestion_metadata_json`

Migration policy:
- Alembic migration is the canonical schema path.
- Repository `_ensure_alerts_columns()` remains a compatibility fallback for legacy/local DBs only.

## Serialization Contract (Phase 1)

- `raw_payload_json`:
  - stores the original alert payload contract used for alert-centric ingestion.
  - persisted in a bounded/sanitized form to avoid secret-like keys and unnecessary oversized blobs.
- `ingestion_metadata_json`:
  - stores ingestion metadata only: source, schema, warnings, ingest run/timestamp, and bounded context.
  - must not contain secrets or full raw payload duplication.

## Safe Rollout Prerequisites

- Apply Alembic migrations before enabling any new ingestion flags.
- Keep `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false` and `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false` for initial rollout.
- Ensure API and worker runtimes use identical values for new Phase 1 flags.
- Verify `/health` remains healthy/degraded without startup crashes.

## Rollback Steps

1. Disable new ingestion flags:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT=false`
2. Roll back application version if needed.
3. Keep additive DB migration in place unless rollback is explicitly validated as safe in your environment.
4. Verify health endpoint and core endpoints still respond.
5. Verify legacy ingestion endpoints still function.

## Intentionally Not Changed Yet

- No traffic switch to IBM AMLSim import.
- No legacy ingestion removal/replacement.
- No frontend behavior redesign.
- No workflow/scoring redesign.
- No destructive schema migration or mandatory backfill.
- `evaluation_label_is_sar` is persisted for internal evaluation only and is not exposed by default in analyst-facing APIs/UI.
