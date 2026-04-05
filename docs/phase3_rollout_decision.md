# Phase 3 Rollout Decision Guide

## Scope
Phase 3 introduces production-grade rollout controls for alert-centric JSONL ingestion without removing legacy ingestion.

## Rollout Modes
- `ALTHEA_ALERT_JSONL_ROLLOUT_MODE=disabled`
  - JSONL ingestion path is off.
  - `/api/data/upload-alert-jsonl` returns structured `503`.
- `ALTHEA_ALERT_JSONL_ROLLOUT_MODE=canary`
  - JSONL ingestion is allowed only under controlled conditions.
  - Uploads must stay within `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS` unless explicit request override is set (`X-ALTHEA-CANARY-OVERRIDE: true`).
- `ALTHEA_ALERT_JSONL_ROLLOUT_MODE=full`
  - JSONL ingestion is fully allowed at application level.
  - Rollback remains immediate by switching mode or disabling ingestion flag.

Additional gate:
- `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false|true` must be `true` for any non-disabled rollout mode to take effect.

## Canary Enablement Steps
1. Confirm deployment config is aligned between API and workers:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION`
   - `ALTHEA_ALERT_JSONL_ROLLOUT_MODE`
   - `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS`
   - `ALTHEA_STRICT_INGESTION_VALIDATION`
2. Set:
   - `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=true`
   - `ALTHEA_ALERT_JSONL_ROLLOUT_MODE=canary`
   - conservative `ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS` (for example `100` or `500`).
3. Ingest a small validation dataset.
4. Review `/internal/rollout/status` and ingestion logs before increasing traffic.

## Metrics and Aggregates
Phase 3 decisioning evaluates recent runs (windowed) using:
- `success_rate`
- `failure_rate`
- `validation_error_rate`
- `avg_latency_ms`
- `p95_latency_ms`
- `total_alerts_ingested`
- `data_quality_issue_rate`
- repeated failure count
- critical data-quality issue count

Ingestion telemetry remains available in Prometheus metrics from Phase 2.

## GO / HOLD / ROLLBACK Logic
Decision output:
```json
{
  "decision": "GO | HOLD | ROLLBACK",
  "reasons": ["..."],
  "metrics_snapshot": { "...": "..." }
}
```

Thresholds are implemented in `backend/services/rollout_evaluator.py`:
- Failure rate:
  - hold at `>= 1%`
  - rollback at `>= 5%`
- Validation error rate:
  - hold at `>= 2%`
  - rollback at `>= 8%`
- p95 latency:
  - hold at `>= 8000 ms`
  - rollback at `>= 20000 ms`
- Data quality issue rate:
  - hold at `>= 5%`
  - rollback at `>= 15%`
- Repeated failed runs and critical issue runs also escalate HOLD/ROLLBACK.

## Data Quality Severity
Advanced checks classify issues as warning or critical, including:
- empty alerts
- abnormal alert size
- duplicate alert IDs within file and across recent runs
- inconsistent SAR labeling patterns
- missing critical normalized fields

Critical issues are included in ingestion summaries and influence HOLD/ROLLBACK recommendations.

## Operator Actions
1. `GO`:
   - continue canary and gradually increase dataset size.
2. `HOLD`:
   - stop expansion, inspect reasons/failed rows, fix schema/data quality issues, rerun canary.
3. `ROLLBACK`:
   - set `ALTHEA_ALERT_JSONL_ROLLOUT_MODE=disabled` or `ALTHEA_ENABLE_ALERT_JSONL_INGESTION=false`.
   - continue legacy ingestion only.
   - verify `/health`, queue status, and core API paths.

## Notes
- Legacy ingestion endpoints and paths remain unchanged.
- No destructive schema operations are introduced in Phase 3.
- Internal evaluation labels are not exposed to analyst-facing APIs/UI.
# Historical note: this Phase 3 rollout guidance is retained for audit trail.
# Finalized production behavior is documented in docs/final_architecture_overview.md.
