# ALTHEA Enrichment Plane

## Design
The enrichment plane is separate from the scoring pipeline.

- `FeatureEnrichmentService` remains the only runtime builder of `BuilderContext`.
- Connectors, sync state, master data, entity resolution, audit, dead-letter, and schema drift live outside `PipelineService`.
- Runtime feature generation reads canonical enrichment tables first and falls back to legacy payload/case-derived enrichment if the canonical store is empty.

## Data layers
- Canonical PIT history:
  - `enrichment_account_events`
  - `enrichment_alert_outcomes`
  - `enrichment_case_actions`
- Sync/health:
  - `enrichment_sync_state`
  - `enrichment_source_health`
- Master data and resolution:
  - `master_customers`
  - `master_accounts`
  - `master_counterparties`
  - `entity_aliases`
  - `entity_links`
  - `master_data_overrides`
- Operational hardening:
  - `enrichment_audit_log`
  - `enrichment_dead_letter`
  - `enrichment_schema_registry`
  - `enrichment_coverage_snapshots`

## Runtime path
1. Alerts enter the scoring path.
2. `FeatureEnrichmentService` resolves relevant history from canonical tables.
3. Feature builders consume the assembled `BuilderContext`.
4. If canonical tables are not yet populated, the service falls back to the older alert-payload/case bridge.

## Sync path
1. Internal/admin endpoint enqueues an enrichment job.
2. Enrichment worker invokes `EnrichmentSyncService`.
3. Service pulls data from an internal source or HTTP connector.
4. Canonical tables and master data are updated.
5. Entity links are rebuilt when master data changes.
6. Health, audit, coverage, and schema observations are recorded.
