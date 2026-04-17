# ALTHEA Pilot Operations Runbook

This runbook covers the repo-supported operating model for ALTHEA. It is intentionally limited to what the repository can implement and verify today.

## Startup

Required runtime dependencies:

- PostgreSQL
- Redis
- `ALTHEA_JWT_SECRET`
- Trusted `ALTHEA_ALLOWED_ORIGINS` and `ALTHEA_ALLOWED_HOSTS`

Local boot sequence:

1. Populate environment variables from [backend/.env.example](../../backend/.env.example).
2. Start infrastructure with `docker compose -f docker/docker-compose.dev.yml up -d postgres redis`.
3. Start the API with `python -m uvicorn main:app --host 127.0.0.1 --port 8000` from `backend/`.
4. Start workers with `python -m workers.pipeline_worker`, `python -m workers.event_worker`, and `python -m workers.streaming_worker`.
5. Validate liveness with `GET /health` and readiness with `GET /readyz`.

Expected signals:

- `/health` returns `{"ok": true, "status": "alive"}`
- `/readyz` returns HTTP 200 and `status=ready`
- worker heartbeats appear in Redis

## Migrations

Repository migration assets live under `backend/migrations/`.

Execution sequence:

1. Point `ALTHEA_DATABASE_URL` at the target PostgreSQL instance.
2. Run `alembic upgrade head` from `backend/`.
3. Run `pytest backend/tests -q` with a non-placeholder `ALTHEA_JWT_SECRET`.
4. Validate the API can import with `python -m compileall backend -x "backend[\\/](\\.venv|__pycache__)"`.

Rollback note:

- The repo contains migrations and rollback planning docs, but rollback safety still depends on database backup discipline in the target environment.

## Worker Operations

Primary worker processes:

- `workers.pipeline_worker`
- `workers.event_worker`
- `workers.streaming_worker`

Operational checks:

- Redis queue depth through `/internal/health`
- Worker heartbeats through `/internal/health`
- Queue diagnostics with `python backend/diagnose_queue.py`

If pipeline jobs stall:

1. Check Redis connectivity.
2. Check worker heartbeats.
3. Inspect dead-letter artifacts under `data/dead_letter/`.
4. Confirm the queued job still exists in the database.
5. Restart the affected worker process only after capturing logs.

## Upload and Storage Safety

Current repo behavior:

- alert JSONL uploads are validated for extension, content type, size, and row count
- transient upload files are deleted after processing
- runtime data stays under `/app/data` in containers and `data/` locally

Operator guidance:

- do not mount source-controlled sample data as the runtime object store
- back up PostgreSQL; local object storage in this repo is an implementation fallback, not a bank-grade archive

## Security-Sensitive Configuration

Must-review settings before non-development use:

- `ALTHEA_JWT_SECRET`
- `ALTHEA_ALLOWED_ORIGINS`
- `ALTHEA_ALLOWED_HOSTS`
- `ALTHEA_REFRESH_COOKIE_SECURE`
- `ALTHEA_ALLOW_REFRESH_TOKEN_IN_BODY`
- `ALTHEA_EXPOSE_REFRESH_TOKEN_IN_RESPONSE`
- `ALTHEA_ENABLE_PUBLIC_TENANT_BOOTSTRAP`
- `ALTHEA_TRUST_PROXY_HEADERS`

Security expectations enforced by the repo:

- non-development requires PostgreSQL
- non-development rejects insecure origins and wildcard hosts
- refresh cookies must be secure outside development
- placeholder JWT secrets fail fast
- login and refresh flows are rate-limited, using Redis when available

## Incident Triage

Initial triage checklist:

1. Capture `X-Request-ID` from the failing request.
2. Check `/readyz` and `/internal/health`.
3. Export audit logs from `/api/admin/logs/export`.
4. Review recent auth and investigation mutations.
5. Check queue backlog and worker heartbeats.
6. Check dead-letter files and recent ingestion validation failures.

Focus areas by symptom:

- `401` or `403`: token expiry, session revocation, disabled user, tenant mismatch, RBAC
- `503`: disabled ingestion mode, missing dependency readiness, rollout guard
- missing investigation state: worker failure, queue lag, missing active run context

## Backup / Restore Template

The repo does not implement full backup orchestration. The minimum supported template is:

1. Back up PostgreSQL before migrations and before pilot cutovers.
2. Preserve Redis only if operational forensics require it; it is not the system of record.
3. If local object storage is used, snapshot `/app/data` alongside the database backup.
4. Test restore into an isolated environment before any production-like change window.

This remains an environment responsibility, not something the repository can certify by itself.
