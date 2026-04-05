# Phase 4 Rollback

## Immediate Rollback Options
1. Runtime switch (fastest):
   - `POST /internal/ingestion/primary-mode`
   - Body: `{ "mode": "legacy" }`
2. Config-driven rollback:
   - Set `ALTHEA_PRIMARY_INGESTION_MODE=legacy`
   - Keep alert JSONL schema and additive columns in place.

## Optional Hard Disable of New Path
- If required, also set:
  - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`

## Post-Rollback Health Verification
1. `GET /health` is healthy/degraded without new fatal errors.
2. Unified upload route now follows legacy path:
   - `POST /api/data/upload` ingests via legacy mode.
3. Legacy dedicated endpoints still work:
   - `/api/data/upload-bank-csv`
   - `/api/data/upload-csv`
4. Core queue/detail endpoints still work for existing alerts.
5. Worker queue depth and heartbeat remain stable.

## Metrics to Check
- `primary_ingestion_mode` should indicate `legacy`.
- `ingestion_path_used_total{ingestion_path="legacy"}` should increase after rollback.
- `ingestion_path_used_total{ingestion_path="alert_jsonl"}` should flatten or reduce.
- Ensure no sustained spike in 5xx or ingestion failure counters.

## Notes
- No schema rollback is required.
- Additive database changes remain compatible with both ingestion modes.
- Legacy ingestion path is retained by design for reversibility.
# Historical note: this Phase 4 rollback document is retained for audit trail.
# Current fallback and stabilization actions are in docs/final_migration_runbook.md.
