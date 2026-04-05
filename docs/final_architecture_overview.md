# Final Architecture Overview

## Primary Ingestion Path

- Default production ingestion is alert-centric JSONL.
- Default route `/api/data/upload` resolves to `alert_jsonl` via `ALTHEA_PRIMARY_INGESTION_MODE=alert_jsonl`.
- Dedicated route `/api/data/upload-alert-jsonl` remains active when `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`.

## What Was Disabled/Removed

- Legacy ingestion is hard-disabled by default:
  - `ALTHEA_ENABLE_LEGACY_INGESTION=false`
- Legacy endpoints remain as compatibility stubs and return structured `503` when disabled.
- Staged rollout mode flag (`ALTHEA_ALERT_JSONL_ROLLOUT_MODE`) was removed from active runtime config.

## Compatibility That Remains

- Additive schema and compatibility readers remain in place for existing persisted records.
- Old records and new records are read by the same queue/detail/explainability APIs.
- `evaluation_label_is_sar` remains internal/evaluation-only and is not exposed in analyst-facing payloads by default.

## Rollback Meaning After Finalization

- Normal operation rollback is config-level routing fallback:
  - temporary emergency re-enable of legacy path
  - optional temporary switch of primary mode to `legacy`
- Deep rollback remains application/version rollback, not schema destruction.
- Additive database migration remains in place.

## Final Operational Flags

- `ALTHEA_ENABLE_ALERT_JSONL_INGESTION`
  - kill switch for alert-centric ingestion path.
- `ALTHEA_PRIMARY_INGESTION_MODE`
  - default path selector for `/api/data/upload` (`legacy|alert_jsonl`).
- `ALTHEA_ENABLE_LEGACY_INGESTION`
  - emergency-only override, default `false`.
- `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS`
  - safety cap for upload row count.
- `ALTHEA_INGESTION_MAX_UPLOAD_BYTES`
  - safety cap for upload payload size.

