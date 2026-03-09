# ALTHEA Enterprise Hardening

This document summarizes the production hardening implemented for ALTHEA while preserving backward compatibility and existing API contracts.

## Architecture

The active runtime architecture is:

- `backend/services/`
- `backend/models/`
- `backend/storage/`
- `backend/workers/`

Legacy paths remain only as compatibility facades to avoid breaking existing functionality.

## Async Pipeline

Pipeline execution is asynchronous only:

1. API endpoint: `POST /api/pipeline/run`
2. Job queued via `JobQueueService` (`rq` or `threaded`)
3. Worker executes `workers.pipeline_worker.run_pipeline_job`
4. Pipeline stages run and persist outputs

Inline queue mode is explicitly disabled (`ALTHEA_QUEUE_MODE=inline` rejected).

## Event-Driven Flow

Events are published to the event bus:

- `alert_ingested`
- `features_generated`
- `alert_scored`
- `alert_governed`
- `case_created`

Event subscriber worker:

- `backend/workers/event_subscriber_worker.py`

## Storage and Migrations

- PostgreSQL is enforced in non-development environments.
- SQLite is allowed only when `ALTHEA_ENV=development` and `ALTHEA_ALLOW_SQLITE_IN_DEV=true`.
- Alembic migrations include:
  - enterprise core tables
  - feature store
  - model monitoring
  - alerts/pipeline run indexes
  - PostgreSQL partition-ready alerts table (`alerts_partitioned`)

## ML Serving and Registry

- Inference uses `MLModelService` and `InferenceService`.
- Model registry persists:
  - model version
  - training metadata
  - approval status
  - approver identity and timestamp
- Monitoring records:
  - PSI
  - drift score
  - degradation flag

## Security

- JWT handled by `python-jose`.
- Refresh-token rotation and session revocation (`logout`, `logout-all`).
- Tenant isolation check between token and tenant header.
- RBAC permissions enforced by dependency middleware.
- Secret indirection supported via `ALTHEA_SECRET_KEY_REF`:
  - `env:VAR_NAME`
  - file path containing the secret value

## Observability

- OpenTelemetry initialization: `backend/core/telemetry.py`
- Prometheus metrics endpoint: `GET /metrics`
- Metrics include:
  - pipeline execution duration/status
  - worker task duration/status
  - ML inference latency
  - alert queue size
  - event bus volume
- Grafana + Prometheus + OTel Collector manifests under `k8s/observability/`.

## Deployment

Kubernetes manifests are structured with environment separation:

- `k8s/base`
- `k8s/overlays/dev`
- `k8s/overlays/staging`
- `k8s/overlays/prod`

CI/CD pipeline:

- `.github/workflows/althea-enterprise-cicd.yml`

It runs tests/builds, publishes container images, and deploys overlays by environment branch/tag.
