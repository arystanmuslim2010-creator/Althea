# Phase 1 Manual Checks

## Startup Checks

1. Start API with default Phase 1 flags.
2. Confirm startup succeeds with no config validation errors.
3. Confirm logs show Phase 1 rollout flag state.
4. Confirm logs show active schema version and, when available, active Alembic revision.

## DB Migration Checks

1. Run Alembic upgrade to head.
2. Confirm `alerts` has nullable columns:
   - `raw_payload_json`
   - `source_system`
   - `ingestion_run_id`
   - `schema_version`
   - `evaluation_label_is_sar`
   - `ingestion_metadata_json`
3. Confirm no legacy columns/tables were dropped or renamed.
4. Confirm existing alert reads still work with old rows (no backfill required).

## Health Endpoint Checks

1. Call `/health` with new flags at defaults.
2. Confirm response shape is unchanged (`ok`, `status`, `checks`, `queue_depth`, `details`).
3. Confirm disabled JSONL route state does not crash startup or health/readiness probes.

## Ingestion Compatibility Checks

1. Legacy ingestion path:
   - Load an existing sample dataset via legacy path (`/api/data/upload-bank-csv` or existing CSV ingestion flow).
   - Run pipeline and confirm alerts are persisted and retrievable.
2. Alert JSONL route disabled:
   - Call `/api/data/upload-alert-jsonl` with defaults.
   - Confirm `503` body:
     - `error=alert_jsonl_ingestion_disabled`
     - `message=Alert JSONL ingestion is disabled by configuration.`
3. Alert JSONL route enabled (controlled check):
   - Set `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`.
   - Upload valid JSONL and confirm ingestion completes.
4. Strict validation check:
   - Set `ALTHEA_STRICT_INGESTION_VALIDATION=true`.
   - Upload JSONL with at least one invalid row.
   - Confirm upload fails atomically (no partial ingest).

## Config Consistency Checks (API vs Workers)

1. Confirm API, worker, and event/streaming worker receive identical values for:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION`
   - `ALTHEA_ENABLE_IBM_AMLSIM_IMPORT`
   - `ALTHEA_ENABLE_HUMAN_INTERPRETATION`
   - `ALTHEA_STRICT_INGESTION_VALIDATION`
2. Confirm defaults remain safe (`false/false/true/false`) across deployment templates.

## Core Regression Checks

1. `GET /health` returns `200`.
2. Existing core alert endpoints still work against legacy data.
3. Existing auth and pipeline trigger paths remain functional.
4. No analyst-facing API regression from Phase 1 metadata additions.
