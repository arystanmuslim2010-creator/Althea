# Repository Cleanup Notes

Date: 2026-04-01
Scope: Safe repository cleanup and structure hardening without backend business-logic refactors.

## Removed Runtime/Generated Artifacts

- Removed tracked generated probe files from `data/data/models/_health/`.
- Removed tracked legacy generated dataset snapshots from `data/data/models/datasets/default-bank/public/` including `*.raw.csv` runtime copies.
- Removed duplicate root-level Kubernetes manifests:
  - `k8s/backend-deployment.yaml`
  - `k8s/service.yaml`
  - `k8s/worker-deployment.yaml`
- Removed migrated ignore file after merge:
  - `.gitignore.migrated`

## Dataset Consolidation

Created canonical dataset locations:

- `data/fixtures/`
- `data/reference_datasets/`

Moved canonical datasets:

- `backend/sample_data.csv` -> `data/fixtures/sample_data.csv`
- `data/bank_alerts_template.csv` -> `data/fixtures/bank_alerts_template.csv`
- `data/bank_alerts_1000.csv` -> `data/reference_datasets/bank_alerts_1000.csv`

Deduplication outcome:

- Removed hash-named dataset copies under `data/data/models/datasets/default-bank/public/` after confirming no active references in code/tests/docs.
- Removed duplicate `.raw.csv` variants that matched canonical dataset content.

## Archive/Legacy Consolidation

Moved historical archive notes into docs:

- `backend/scripts/archive/README.txt` -> `docs/legacy/backend_scripts_archive_README.txt`
- `scripts/archive/startup/README.txt` -> `docs/legacy/scripts_archive_startup_README.txt`

Updated references:

- `docs/operations/runtime_commands.md`
- `docs/reports/ALTHEA Integration and Production Hardening Report.md`

## Root Cleanup

Moved root report documents into `docs/reports/`:

- `ALTHEA Explainability Integrity Fix Report.md`
- `ALTHEA Graph and Narrative Integration Report.md`
- `ALTHEA Integration and Production Hardening Report.md`

## .gitignore Hardening

Main `.gitignore` updated to block future noise for:

- Python caches and tooling outputs (`__pycache__/`, `*.py[cod]`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.coverage`, `htmlcov/`, virtualenv dirs)
- Runtime artifacts and generated data (`data/object_storage/`, `data/dead_letter/`, `data/data/models/_health/`, `data/data/models/datasets/default-bank/public/`, `*.raw.csv`, `*.tmp`, `*.bak`, `*.swp`)
- Local secrets and keys (`*.pem`, `*.key`, `secrets/`)
- Editor/IDE files (`.idea/`, `.vscode/*`) while preserving shared `.vscode/settings.json`
- Frontend build outputs (`dist/`, `build/`)

Canonical datasets remain trackable under:

- `data/fixtures/`
- `data/reference_datasets/`

## Manual Review Flags

- Local workspace may still contain untracked runtime artifacts under `data/object_storage/` and `data/reports/`; these are intentionally ignored and were not committed by this cleanup.
- If deployment workflows ever applied root `k8s/*.yaml` directly, they should use `kubectl apply -k k8s/overlays/<env>` going forward.
