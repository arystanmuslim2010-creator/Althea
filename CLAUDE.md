# ALTHEA - Engineering Notes

## Overview

ALTHEA is a multi-tenant AML alert prioritization platform for bank investigations.

## Backend Architecture

The backend uses one modular architecture under `backend/`:

- `api/` FastAPI routers and HTTP contracts
- `core/` config, security, observability, shared runtime utilities
- `services/` business workflows (pipeline, features, governance, cases, queue)
- `models/` model registry, feature schema, and inference services
- `storage/` Postgres/object-store/Redis adapters and repositories
- `workers/` async job workers and event subscribers
- `events/` event bus and event contracts
- `migrations/` Alembic schema migrations

Legacy `backend/src` has been removed and must not be reintroduced.

## Scoring Flow

Single scoring path:

1. Pipeline generates features in `services/pipeline_service.py`
2. Inference runs in `models/inference_service.py` using model artifacts from `models/model_registry.py`
3. Results persist through `storage/postgres_repository.py`

Event workers must not recompute ML scores.

## Security and Tenancy

- Registration is controlled by provisioning modes (`ADMIN_INVITE`, `TENANT_BOOTSTRAP`, `SSO_PROVISIONING`)
- Role assignment is admin-controlled and validated
- Tenant isolation is enforced with PostgreSQL RLS and tenant session context (`app.tenant_id`)
- User identity is tenant scoped (`UNIQUE(tenant_id, email)`)

## Operations

- Health endpoint validates database, Redis, queue, and model registry
- Structured logging includes tenant and model context
- Metrics include alerts processed, pipeline runtime, inference latency, and queue depth

## Local Run

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload

# tests
pytest tests -q
```

```bash
cd frontend
npm install
npm run dev
```
