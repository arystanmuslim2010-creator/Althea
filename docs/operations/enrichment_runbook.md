# Enrichment Runbook

## Purpose
This runbook covers the ALTHEA enrichment plane: canonical event/outcome/case tables, master data sync, entity resolution, and operator-facing health checks.

## Primary commands
- Apply migrations: `cd backend && alembic upgrade head`
- Start stack: use the existing Docker stack plus the enrichment worker when background sync jobs are required.
- Seed canonical internal enrichment tables: `cd backend && python scripts/run_internal_enrichment_backfill.py --tenant-id <tenant-id> --targets case_actions alert_outcomes`
- Manual source sync: `POST /internal/enrichment/sources/{source_name}/sync`
- Internal backfill: `POST /internal/enrichment/backfill/internal`

## Docker services
- `backend`: API and synchronous runtime path.
- `worker`: pipeline RQ worker for ingestion/model pipeline jobs.
- `event-worker`: event subscriber worker.
- `enrichment-worker`: dedicated RQ worker for enrichment sync/backfill/dead-letter jobs.

The enrichment worker listens on `ALTHEA_ENRICHMENT_RQ_QUEUE` and should be deployed separately from the main pipeline worker so enrichment jobs do not compete with scoring pipeline jobs.

## Operator checks
- `GET /internal/enrichment/status`
- `GET /internal/enrichment/sources`
- `GET /internal/enrichment/schema-drift`
- `GET /internal/enrichment/dead-letter`
- `GET /internal/enrichment/coverage`

## Expected source names
- `internal_case`
- `internal_outcome`
- `kyc`
- `watchlist`
- `device`
- `channel`

## Failure handling
- If sync fails, inspect `/internal/enrichment/dead-letter` and `/internal/enrichment/audit`.
- If a source is stale, inspect `enrichment_sync_state`, latest source health, and connector configuration.
- If runtime scoring still works but enrichment is degraded, ALTHEA should continue via fallback/partial context rather than fail closed.

## Security notes
- All enrichment endpoints are internal/admin only.
- Connector secrets must be passed via environment variables, never persisted in API responses or audit details.
- Raw upstream payloads are stored only after secret-safe sanitization.
