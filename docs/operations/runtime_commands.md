# ALTHEA Runtime Commands

## Canonical entrypoints

- Backend API (dev): `make backend`
- Frontend (dev): `make frontend`
- Pipeline worker: `make worker`
- Full local stack (3 terminals): `make dev`
- Tests: `make test`
- Lint/build checks: `make lint`

## Script classification

- Canonical production entrypoint:
  - `backend/main.py` served by `uvicorn main:app`
- Canonical development helpers:
  - `scripts/althea-local.ps1`
  - `scripts/dev-stack.ps1`
- Test helpers:
  - `backend/check_backend.py`
  - `backend/cleanup_db.py`
  - `backend/diagnose.py`
  - `backend/diagnose_queue.py`
- Archived obsolete startup scripts:
  - `backend/scripts/archive/README.txt`
  - `scripts/archive/startup/README.txt`

## Notes

- Archived scripts were retained for historical troubleshooting but are no longer part of the supported operational flow.
- Use environment variables (or `backend/.env.example`) for all credentials and runtime settings.

## Workflow architecture (consolidated)

- Canonical queue/action path: `backend/api/routers/investigation_router.py`
  - `/api/alerts/{id}/assign`
  - `/api/alerts/{id}/status`
  - `/api/alerts/bulk-assign`
  - `/api/alerts/bulk-status`
- Canonical workflow action aliases for frontend detail actions: `backend/api/routers/intelligence_router.py`
  - `/api/workflows/alerts/{id}/assign`
  - `/api/workflows/alerts/{id}/escalate`
  - `/api/workflows/alerts/{id}/close`
- Unified state model source of truth:
  - `backend/workflows/state_model.py`
  - `backend/workflows/alert_workflow_service.py`
