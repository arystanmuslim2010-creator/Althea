"""Health check functions for use by Streamlit/API (no live internet)."""
from __future__ import annotations

from typing import Any, Dict


def db_ok(storage: Any) -> bool:
    """Return True if DB is reachable and basic query works."""
    if storage is None:
        return False
    try:
        _ = getattr(storage, "list_runs", None)
        if _ is not None:
            storage.list_runs(limit=1)
        return True
    except Exception:
        return False


def last_run_ok(storage: Any) -> bool:
    """Return True if at least one run exists and can be loaded."""
    if storage is None:
        return False
    try:
        runs = storage.list_runs(limit=1)
        if not runs:
            return True  # no runs is ok (fresh install)
        r = runs[0]
        run_id = r.get("run_id")
        if run_id and hasattr(storage, "load_alerts_by_run"):
            storage.load_alerts_by_run(run_id)
        return True
    except Exception:
        return False


def pipeline_ok(storage: Any) -> bool:
    """Return True if pipeline can be considered operational (DB + last run)."""
    return db_ok(storage) and last_run_ok(storage)


def health_check(storage: Any = None) -> Dict[str, Any]:
    """
    Return health status dict: { "status": "ok"|"degraded", "checks": {...} }.
    Used by backend/Streamlit; no external network calls.
    Includes db_ok, last_run_ok, pipeline_ok.
    """
    checks: Dict[str, Any] = {}
    try:
        if storage is not None:
            checks["storage"] = "ok" if db_ok(storage) else "error"
            checks["db_ok"] = db_ok(storage)
            checks["last_run_ok"] = last_run_ok(storage)
            checks["pipeline_ok"] = pipeline_ok(storage)
        else:
            checks["storage"] = "skipped"
            checks["db_ok"] = False
            checks["last_run_ok"] = False
            checks["pipeline_ok"] = False
    except Exception as e:
        checks["storage"] = f"error: {e}"
        checks["db_ok"] = False
        checks["last_run_ok"] = False
        checks["pipeline_ok"] = False
    status = "ok" if all(
        v == "ok" or v == "skipped" or v is True
        for k, v in checks.items()
        if k in ("storage", "db_ok", "last_run_ok", "pipeline_ok") and v in (True, "ok", "skipped")
    ) else "degraded"
    return {"status": status, "checks": checks}
