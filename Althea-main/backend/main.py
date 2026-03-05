"""FastAPI backend for AML Alert Prioritization - exposes existing Python logic."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("aml-backend")

# Backend root = this directory; project root = parent (where frontend/ and data/ live)
_BACKEND_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_BACKEND_ROOT))

from src import config, features, scoring, utils
from src.rule_engine import aggregate_rule_score, run_all_rules
from src.external_data import load_all_configured_sources
from src.synth_data import generate_synthetic_alerts, generate_synthetic_transactions
from src import suppression, risk_governance, alert_governance, adaptive_threshold, risk_drift
from src import health_monitor
from src.queue_governance import apply_alert_governance
from src.services import CaseService, FeatureService, ScoringService, OpsService, MissingColumnsError
from src.services.ingestion_service import IngestionService, IngestionError
from src.domain.schemas import OverlayInputError
from src.storage import Storage
from src.governance.drift_monitor import score_distribution_monitor, feature_drift_monitor
from src.governance.psi import compute_psi_table
from src.governance.performance_monitor import performance_trends
from src.governance.decision_logger import DecisionLogger

app = FastAPI(title="AML Alert Prioritization API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://althea-uolo.vercel.app",
        "https://althea-gamma.vercel.app",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(OverlayInputError)
def overlay_input_exception_handler(request, exc: OverlayInputError):
    """Overlay mode: reject transaction-level input with 400."""
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
def global_exception_handler(request, exc: Exception):
    """Return JSON with detail for any unhandled exception so frontend can show the message."""
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    tb = traceback.format_exc()
    logger.error("Unhandled exception: %s\n%s", exc, tb)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc) or "Internal server error"},
    )


# ---------------------------------------------------------------------------
# State – data/ and model_cache at project root (parent of backend/)
# ---------------------------------------------------------------------------
_ROOT = _BACKEND_ROOT.parent
_DATA = _ROOT / "data" if (_ROOT / "data").exists() else _BACKEND_ROOT / "data"
config.MODEL_CACHE_DIR = str(_DATA / "model_cache")
storage = Storage(db_path=str(_DATA / "app.db"))
feature_service = FeatureService()
scoring_service = ScoringService()
case_service = CaseService(storage=storage)
ops_service = OpsService()
ingestion_service = IngestionService()

# Session state per "session" (simplified: single global for demo)
_state = {
    "df": None,
    "active_run_id": None,
    "_run_source": None,
    "_run_info": None,
    "_bank_csv_hash": None,
    "_bank_csv_bytes": None,  # for overlay pipeline determinism
    "cases": {},
    "case_counter": 1,
    "actor": "Analyst_1",
}

# Load cases from DB on startup
def _load_cases_from_db():
    try:
        cases_dict, case_counter, _ = storage.load_state_from_db()
        if cases_dict:
            _state["cases"] = cases_dict
            _state["case_counter"] = case_counter
    except Exception:
        pass

_load_cases_from_db()


def _ensure_risk_band(df_in: pd.DataFrame) -> pd.DataFrame:
    if "risk_score" not in df_in.columns:
        return df_in
    t1 = int(getattr(config, "RISK_BAND_T1", 40))
    t2 = int(getattr(config, "RISK_BAND_T2", 70))
    t3 = int(getattr(config, "RISK_BAND_T3", 90))
    df_out = df_in.copy()
    rs = pd.to_numeric(df_out["risk_score"], errors="coerce").fillna(0.0)
    df_out["risk_band"] = np.select(
        [rs < t1, rs < t2, rs < t3],
        ["LOW", "MEDIUM", "HIGH"],
        default="CRITICAL",
    )
    return df_out


def _make_run_id() -> str:
    ts = int(time.time() * 1000)
    short = hashlib.sha256(str(ts).encode()).hexdigest()[:8]
    return f"run_{ts}_{short}"


def _dataset_hash_from_df(df: pd.DataFrame) -> str:
    try:
        raw = df.head(5000).to_csv(index=False).encode("utf-8")
    except Exception:
        raw = str(df.shape).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _dataset_hash_from_bytes(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()[:16]


def _make_json_serializable(val):
    """Convert numpy/pandas types and NaN to JSON-serializable values."""
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return None
    if isinstance(val, np.floating):
        return float(val) if np.isfinite(val) else None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat() if pd.notna(val) else None
    if isinstance(val, (dict, list)):
        if isinstance(val, dict):
            return {k: _make_json_serializable(v) for k, v in val.items()}
        return [_make_json_serializable(v) for v in val]
    if pd.isna(val):
        return None
    return val


def _sanitize_record(r: dict) -> dict:
    """Return a copy of the record with all values JSON-serializable."""
    out = {}
    for k, v in r.items():
        try:
            out[k] = _make_json_serializable(v)
        except Exception:
            out[k] = str(v) if v is not None else None
    return out


def _parse_json_field(raw, default):
    if raw is None:
        return default
    if isinstance(raw, str):
        if not raw.strip():
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default
    return raw


def _apply_scoring_api_fields(record: dict) -> dict:
    out = dict(record)
    score = float(out.get("risk_score", 0.0) or 0.0)
    risk_band = str(out.get("risk_band", "") or "").lower()
    out["score"] = score
    out["priority"] = str(out.get("priority") or risk_band or "low")
    out["model_version"] = str(out.get("model_version") or getattr(config, "MODEL_VERSION", "v1.0"))

    top_features = _parse_json_field(out.get("top_features_json"), [])
    if not isinstance(top_features, list) or not top_features:
        contrib = _parse_json_field(out.get("top_feature_contributions_json"), [])
        if isinstance(contrib, list):
            top_features = [str(item.get("feature")) for item in contrib if isinstance(item, dict) and item.get("feature")]
    out["top_features"] = top_features if isinstance(top_features, list) else []
    return out


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "app": "AML Alert Prioritization API"}


@app.get("/health")
def health_check():
    return {"ok": True}


@app.post("/api/data/generate-synthetic")
def generate_synthetic(n_rows: int = 400):
    """Generate synthetic data: alert-level AML alerts in overlay mode, else transaction-level demo."""
    try:
        if getattr(config, "OVERLAY_MODE", False):
            cfg = SimpleNamespace(**{name: getattr(config, name) for name in dir(config) if name.isupper()})
            df = generate_synthetic_alerts(n_rows=n_rows, cfg=cfg, seed=getattr(config, "DEMO_SEED", 42))
        else:
            cfg = SimpleNamespace(**{name: getattr(config, name) for name in dir(config) if name.isupper()})
            n_users = max(80, n_rows // 5)
            df = generate_synthetic_transactions(n_users=n_users, tx_per_user=n_rows, cfg=cfg)
        _state["df"] = df
        _state["_run_source"] = "Synthetic"
        return {"rows": len(df), "source": "Synthetic"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload generic CSV of transactions (rejected in overlay mode)."""
    try:
        contents = await file.read()
        from io import BytesIO
        df = feature_service.load_transactions_csv(BytesIO(contents))
        _state["df"] = df
        _state["_run_source"] = "CSV"
        return {"rows": len(df), "source": "CSV"}
    except OverlayInputError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except MissingColumnsError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/generate-bank-csv")
def generate_bank_csv(n_rows: int = 1000, seed: int = 42):
    """Generate a bank-alerts CSV with n_rows alerts and save to data/bank_alerts_<n>.csv.
    Use the returned path to upload via Upload bank CSV for ingestion testing."""
    try:
        from types import SimpleNamespace
        cfg = SimpleNamespace(**{name: getattr(config, name) for name in dir(config) if name.isupper()})
        df = generate_synthetic_alerts(n_rows=max(1, min(n_rows, 10000)), cfg=cfg, seed=seed)
        out = df[["alert_id", "user_id", "amount", "segment", "country", "typology", "source_system"]].copy()
        out["timestamp_utc"] = df["timestamp"]
        out["channel"] = "bank_transfer"
        out["time_gap"] = 86400
        out["num_transactions"] = 1
        if "alert_risk_band" in df.columns:
            out["alert_risk_band"] = df["alert_risk_band"]
        cols = [
            "alert_id", "user_id", "amount", "segment", "country", "channel",
            "timestamp_utc", "time_gap", "num_transactions", "typology", "source_system",
        ]
        if "alert_risk_band" in out.columns:
            cols.append("alert_risk_band")
        out = out[cols]
        _DATA.mkdir(parents=True, exist_ok=True)
        out_path = _DATA / f"bank_alerts_{len(out)}.csv"
        out.to_csv(out_path, index=False, encoding="utf-8")
        return {"rows": len(out), "path": str(out_path), "message": f"Saved to {out_path}. Upload this file via Upload bank CSV."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/upload-bank-csv")
async def upload_bank_csv(file: UploadFile = File(...)):
    """Upload bank alerts CSV. Does not fall back to demo data."""
    try:
        contents = await file.read()
        if not contents or len(contents) == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        from io import BytesIO
        df = ingestion_service.load_bank_alerts_csv(BytesIO(contents))
        _state["df"] = df
        _state["_run_source"] = "BankCSV"
        _state["_bank_csv_hash"] = _dataset_hash_from_bytes(contents)
        _state["_bank_csv_bytes"] = contents  # for overlay pipeline determinism
        return {"rows": len(df), "source": "BankCSV"}
    except IngestionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/run")
def run_pipeline():
    """Run the full AML overlay pipeline on loaded data (deterministic: INGEST->NORMALIZE->...->METRICS)."""
    df = _state.get("df")
    if df is None or df.empty:
        raise HTTPException(status_code=400, detail="No data loaded. Generate or upload data first.")
    run_source = _state.get("_run_source", "Unknown")

    # Overlay mode: reject transaction-level CSV only; Synthetic is alert-level after generator change
    if getattr(config, "OVERLAY_MODE", False):
        if run_source == "CSV":
            raise HTTPException(
                status_code=400,
                detail="Overlay requires alert-level input from AML monitoring systems.",
            )
        try:
            from pathlib import Path
            from src.pipeline import run_pipeline as run_overlay_pipeline
            _data = Path(_DATA)
            source = "csv" if run_source == "BankCSV" and _state.get("_bank_csv_bytes") else "dataframe"
            input_bytes = _state.get("_bank_csv_bytes") if source == "csv" else None
            run_id = run_overlay_pipeline(
                source=source,
                config_overrides={"policy_version": getattr(config, "CURRENT_POLICY_VERSION", "1.0")},
                input_df=df,
                input_bytes=input_bytes,
                storage=storage,
                data_dir=_data,
                reports_dir=_data / "reports",
                dead_letter_dir=_data / "dead_letter",
            )
            dataset_hash = _state.get("_bank_csv_hash") or _dataset_hash_from_df(df)
            _state["active_run_id"] = run_id
            _state["_run_info"] = {
                "run_id": run_id,
                "source": run_source,
                "dataset_hash": dataset_hash,
                "row_count": len(df),
            }
            run_meta = storage.get_run(run_id)
            return {"run_id": run_id, "alerts": run_meta["row_count"] if run_meta else len(df)}
        except OverlayInputError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.exception("Overlay pipeline failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    # Non-overlay: try overlay pipeline first, then legacy path
    try:
        from pathlib import Path
        from src.pipeline import run_pipeline as run_overlay_pipeline
        _data = Path(_DATA)
        source = "csv" if run_source in ("BankCSV", "CSV") and _state.get("_bank_csv_bytes") else "dataframe"
        input_bytes = _state.get("_bank_csv_bytes") if source == "csv" else None
        run_id = run_overlay_pipeline(
            source=source,
            config_overrides={"policy_version": getattr(config, "CURRENT_POLICY_VERSION", "1.0")},
            input_df=df,
            input_bytes=input_bytes,
            storage=storage,
            data_dir=_data,
            reports_dir=_data / "reports",
            dead_letter_dir=_data / "dead_letter",
        )
        dataset_hash = _state.get("_bank_csv_hash") or _dataset_hash_from_df(df)
        _state["active_run_id"] = run_id
        _state["_run_info"] = {
            "run_id": run_id,
            "source": run_source,
            "dataset_hash": dataset_hash,
            "row_count": len(df),
        }
        run_meta = storage.get_run(run_id)
        return {"run_id": run_id, "alerts": run_meta["row_count"] if run_meta else len(df)}
    except ImportError:
        pass
    except Exception as e:
        logger.exception("Overlay pipeline failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    # Legacy path (non-overlay only)
    try:
        cfg = SimpleNamespace(**{name: getattr(config, name) for name in dir(config) if name.isupper()})
        try:
            loaded_external_sources = load_all_configured_sources()
        except Exception:
            loaded_external_sources = {}
        df, feature_groups = features.compute_behavioral_features(df, cfg)
        df = run_all_rules(df, cfg)
        df = aggregate_rule_score(df, cfg)
        all_feature_cols = feature_groups["all_feature_cols"]
        X = features.build_feature_matrix(df, all_feature_cols)
        df = scoring_service.run_anomaly_detection(df, X)
        models, calibrator = scoring.train_risk_engine(df, feature_groups)
        df = scoring.score_with_risk_engine(df, models, calibrator, external_sources=loaded_external_sources)
        df["risk_score_original"] = df["risk_score"].copy()
        df = risk_governance.apply_risk_governance(df)
        df["risk_score"] = df["risk_score_governed"].copy()
        df = risk_governance.stabilize_risk_scores(df, cfg)
        df["risk_score"] = df["risk_score_final"].copy()
        df = apply_alert_governance(df)
        df = alert_governance.apply_alert_suppression(df, cfg=cfg)
        if "governance_status" not in df.columns and "alert_id" in df.columns:
            df = apply_alert_governance(df)
        if "in_queue" not in df.columns and "governance_status" in df.columns:
            gov = df["governance_status"].astype(str).str.lower()
            df["in_queue"] = gov.isin(["eligible", "mandatory_review"])
        df = _ensure_risk_band(df)
        if "priority" not in df.columns:
            if "risk_band" in df.columns:
                df["priority"] = df["risk_band"].astype(str).str.lower()
            else:
                df["priority"] = "low"
        if "model_version" not in df.columns:
            df["model_version"] = str(getattr(config, "MODEL_VERSION", "v1.0"))
        DecisionLogger().log_decisions(df, model_version=str(getattr(config, "MODEL_VERSION", "v1.0")))
        df = scoring_service.generate_explainability_drivers(df)
        run_id = _make_run_id()
        dataset_hash = _state.get("_bank_csv_hash") or _dataset_hash_from_df(df)
        storage.save_run(run_id, run_source, dataset_hash, len(df))
        records = df.to_dict("records")
        clean_records = []
        for r in records:
            r["alert_id"] = str(r.get("alert_id", ""))
            r["user_id"] = str(r.get("user_id", ""))
            for json_col in ["risk_explain_json", "rules_json", "rule_evidence_json"]:
                if json_col in r and not isinstance(r[json_col], str):
                    r[json_col] = json.dumps(r[json_col])
            clean_records.append(r)
        storage.upsert_alerts(clean_records, run_id=run_id)
        _state["df"] = df
        _state["active_run_id"] = run_id
        _state["_run_info"] = {"run_id": run_id, "source": run_source, "dataset_hash": dataset_hash, "row_count": len(df)}
        return {"run_id": run_id, "alerts": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Pipeline failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pipeline/clear")
def clear_run():
    """Clear active run."""
    _state["df"] = None
    _state["active_run_id"] = None
    _state["_run_source"] = None
    _state["_run_info"] = None
    _state["_bank_csv_hash"] = None
    _state["_bank_csv_bytes"] = None
    return {"status": "cleared"}


@app.get("/api/alerts")
def get_alerts(
    status_filter: str = "Eligible",
    min_risk: float = 0,
    typology: str = "All",
    segment: str = "All",
    search: str = "",
    run_id: Optional[str] = None,
):
    """Get filtered alerts for the selected run (run_id query param or active run). Never mix runs."""
    try:
        run_id = run_id or _state.get("active_run_id")
        if not run_id:
            return {"alerts": [], "run_id": None}

        alerts_df = storage.load_alerts_by_run(run_id)
        if alerts_df.empty:
            return {"alerts": [], "run_id": run_id}

        queue = alerts_df.copy()

        if status_filter == "Eligible":
            if "governance_status" in queue.columns:
                gov = queue["governance_status"].astype(str).str.lower()
                queue = queue[gov.isin(["eligible", "mandatory_review"])]
            elif "in_queue" in queue.columns:
                queue = queue[queue["in_queue"] == True]
        elif status_filter == "Suppressed":
            if "governance_status" in queue.columns:
                queue = queue[queue["governance_status"].astype(str).str.lower() == "suppressed"]
            elif "in_queue" in queue.columns:
                queue = queue[queue["in_queue"] == False]

        if min_risk > 0 and "risk_score" in queue.columns:
            queue = queue[queue["risk_score"] >= min_risk]
        if segment != "All" and "segment" in queue.columns:
            queue = queue[queue["segment"].astype(str) == segment]
        if typology != "All" and "typology" in queue.columns:
            queue = queue[queue["typology"].astype(str) == typology]
        if search:
            term = search.lower()
            mask = (
                queue["user_id"].astype(str).str.lower().str.contains(term, na=False)
                | queue["alert_id"].astype(str).str.lower().str.contains(term, na=False)
            )
            queue = queue[mask]

        if "risk_score" in queue.columns:
            queue = queue.sort_values("risk_score", ascending=False)

        records = queue.head(200).to_dict("records")
        records = [_sanitize_record(_apply_scoring_api_fields(r)) for r in records]

        return {"alerts": records, "run_id": run_id, "total": len(records)}
    except Exception as e:
        logger.warning("get_alerts failed: %s", e, exc_info=True)
        return {"alerts": [], "run_id": _state.get("active_run_id"), "total": 0}


def _alert_row_to_dict(row) -> dict:
    """Convert alert row to serializable dict."""
    return _sanitize_record(row.to_dict())


def _generate_alert_summary(row_dict: dict) -> str:
    """Generate AML analyst summary with actionable insights."""
    segment = row_dict.get("segment") or "unknown"
    risk_score = float(row_dict.get("risk_score") or 0)
    typology = row_dict.get("typology") or "N/A"
    risk_band = (row_dict.get("risk_band") or "").upper()
    user_id = row_dict.get("user_id", "")
    rules_raw = row_dict.get("rules_json") or row_dict.get("rules") or []
    if isinstance(rules_raw, str):
        try:
            rules_raw = json.loads(rules_raw) if rules_raw else []
        except Exception:
            rules_raw = []
    rule_ids = []
    for r in rules_raw[:5]:
        if isinstance(r, dict):
            rule_ids.append(r.get("rule_id", r.get("id", str(r))))
        else:
            rule_ids.append(str(r))
    rule_hits = ", ".join(rule_ids) if rule_ids else "N/A"
    typology_lower = typology.lower()
    aml_guidance = ""
    if "structuring" in typology_lower:
        aml_guidance = "Recommendation: check splitting of payments below limit, related accounts, beneficiaries."
    elif "dormant" in typology_lower:
        aml_guidance = "Recommendation: verify client identity, ascertain source of sudden activity."
    elif "rapid" in typology_lower or "velocity" in typology_lower:
        aml_guidance = "Recommendation: verify legitimacy of urgency of operations, related channels."
    elif "flow" in typology_lower or "through" in typology_lower:
        aml_guidance = "Recommendation: trace chain of transfers, identify ultimate beneficiary."
    elif "high_amount" in typology_lower or "outlier" in typology_lower:
        aml_guidance = "Recommendation: request confirmation of source of funds, transaction documentation."
    elif "high_risk_country" in typology_lower or "country" in typology_lower:
        aml_guidance = "Recommendation: check sanctions lists, transaction purpose, country linkage."
    else:
        aml_guidance = "Recommendation: run sanctions screening, assess client profile and purpose of operations."
    return (
        f"Segment: {segment} | User: {user_id}\n"
        f"Risk: {risk_score:.1f} ({risk_band}) | Typology: {typology}\n"
        f"Triggered rules: {rule_hits}\n\n"
        f"AML analysis: Alert indicates anomaly in client behavior. "
        f"{aml_guidance}"
    )


@app.get("/api/runs")
def list_runs():
    """List recent runs for UI run selector. Do not mix alerts across runs."""
    try:
        runs = storage.list_runs(limit=50)
        return {"runs": runs}
    except Exception as e:
        logger.warning("list_runs failed: %s", e)
        return {"runs": []}


@app.get("/api/alerts/{alert_id}")
def get_alert(alert_id: str):
    """Get single alert details."""
    run_id = _state.get("active_run_id")
    if not run_id:
        raise HTTPException(status_code=404, detail="No active run")

    alerts_df = storage.load_alerts_by_run(run_id)
    row = alerts_df[alerts_df["alert_id"].astype(str) == str(alert_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail="Alert not found")

    return _sanitize_record(_apply_scoring_api_fields(_alert_row_to_dict(row.iloc[0])))


@app.get("/api/alerts/{alert_id}/explain")
def get_alert_explain(alert_id: str, run_id: Optional[str] = None):
    """Get decision trace for alert (Why? drilldown). Uses active run_id if run_id not provided."""
    rid = run_id or _state.get("active_run_id")
    if not rid:
        raise HTTPException(status_code=404, detail="No active run")
    try:
        from src.services.explain_service import explain_alert
        result = explain_alert(alert_id, rid, storage)
        if result is None:
            raise HTTPException(status_code=404, detail="Alert or decision trace not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("explain_alert failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _get_alert_row(alert_id: str):
    """Get alert row for run_id or fallback to load_alerts_df."""
    run_id = _state.get("active_run_id")
    if run_id:
        alerts_df = storage.load_alerts_by_run(run_id)
    else:
        alerts_df = storage.load_alerts_df()
    if alerts_df.empty:
        return None
    row = alerts_df[alerts_df["alert_id"].astype(str) == str(alert_id)]
    return row.iloc[0] if not row.empty else None


@app.get("/api/alerts/{alert_id}/ai-summary")
def get_ai_summary(alert_id: str):
    """Get stored AI summary for alert."""
    try:
        rec = storage.get_ai_summary("alert", alert_id)
        if rec:
            return {"summary": rec["summary"], "ts": rec.get("ts", "")}
    except Exception:
        pass
    return {"summary": None, "ts": None}


@app.post("/api/alerts/{alert_id}/ai-summary")
def generate_ai_summary(alert_id: str):
    """Generate and save AI summary for alert."""
    row = _get_alert_row(alert_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    row_dict = _alert_row_to_dict(row)
    summary = _generate_alert_summary(row_dict)
    actor = _state.get("actor", "Analyst_1")
    run_id = _state.get("active_run_id") or ""
    try:
        storage.save_ai_summary("alert", alert_id, summary, run_id=run_id, actor=actor)
    except Exception:
        pass
    return {"summary": summary, "ts": pd.Timestamp.utcnow().isoformat()}


@app.delete("/api/alerts/{alert_id}/ai-summary")
def clear_ai_summary(alert_id: str):
    """Clear stored AI summary for alert."""
    try:
        storage.delete_ai_summary("alert", alert_id)
    except Exception:
        pass
    return {"status": "cleared"}


@app.get("/api/run-info")
def get_run_info():
    """Get current run metadata."""
    return _state.get("_run_info") or {}


@app.get("/api/queue-metrics")
def get_queue_metrics():
    """Get KPI metrics for the queue."""
    safe = {"total_alerts": 0, "in_queue": 0, "suppressed": 0, "high_risk": 0}
    try:
        run_id = _state.get("active_run_id")
        if not run_id:
            return safe

        alerts_df = storage.load_alerts_by_run(run_id)
        if alerts_df.empty:
            return safe

        total = len(alerts_df)
        eligible = 0
        suppressed = 0
        if "governance_status" in alerts_df.columns:
            gov = alerts_df["governance_status"].astype(str).str.lower()
            eligible = int(gov.isin(["eligible", "mandatory_review"]).sum())
            suppressed = int((gov == "suppressed").sum())
        elif "in_queue" in alerts_df.columns:
            eligible = int(alerts_df["in_queue"].fillna(False).astype(bool).sum())
            suppressed = int((~alerts_df["in_queue"].fillna(False).astype(bool)).sum())

        high_risk = 0
        if "risk_band" in alerts_df.columns:
            bands = alerts_df["risk_band"].astype(str).str.upper()
            high_risk = int(bands.isin(["HIGH", "CRITICAL"]).sum())
        elif "risk_score" in alerts_df.columns:
            high_risk = int((alerts_df["risk_score"] >= 70).sum())

        return {"total_alerts": total, "in_queue": eligible, "suppressed": suppressed, "high_risk": high_risk}
    except Exception as e:
        logger.warning("get_queue_metrics failed: %s", e, exc_info=True)
        return safe


@app.get("/api/cases")
def get_cases():
    """Get all cases. Synced with DB on each request."""
    try:
        cases_dict, case_counter, _ = storage.load_state_from_db()
        _state["cases"] = cases_dict
        _state["case_counter"] = case_counter
    except Exception:
        pass
    return {"cases": _state.get("cases", {})}


class CreateCaseRequest(BaseModel):
    alert_ids: list[str]
    actor: str = "Analyst_1"


@app.post("/api/cases")
def create_case(req: CreateCaseRequest):
    """Create a new case from alert IDs."""
    df = _state.get("df")
    if df is None or df.empty:
        raise HTTPException(status_code=400, detail="No data loaded")

    cases = _state.get("cases", {})
    counter = _state.get("case_counter", 1)
    case_id = f"CASE_{counter:05d}"
    _state["case_counter"] = counter + 1

    now_iso = pd.Timestamp.utcnow().isoformat()
    case_dict = {
        "case_id": case_id,
        "status": "OPEN",
        "state": "OPEN",
        "assigned_to": "Unassigned",
        "owner": "Unassigned",
        "alert_ids": list(req.alert_ids),
        "notes": "",
        "version": 0,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    cases[case_id] = case_dict
    _state["cases"] = cases

    try:
        storage.save_case_to_db(case_dict)
        storage.append_audit("case", case_id, req.actor, "CREATE_CASE", {"alert_ids": list(req.alert_ids)})
    except Exception:
        pass

    return {"case_id": case_id, "status": "OPEN"}


class SetActorRequest(BaseModel):
    actor: str


@app.get("/api/actor")
def get_actor():
    """Get current analyst actor."""
    return {"actor": _state.get("actor", "Analyst_1")}


@app.put("/api/actor")
def set_actor(req: SetActorRequest):
    """Set current analyst actor."""
    _state["actor"] = req.actor
    return {"actor": req.actor}


class UpdateCaseRequest(BaseModel):
    status: str | None = None
    assigned_to: str | None = None
    notes: str | None = None


@app.put("/api/cases/{case_id}")
def update_case(case_id: str, req: UpdateCaseRequest):
    """Update case status, assignee, or notes."""
    cases = _state.get("cases", {})
    if case_id not in cases:
        raise HTTPException(status_code=404, detail="Case not found")

    case = cases[case_id]
    new_state = req.status if req.status is not None else case.get("status", case.get("state", "OPEN"))
    new_owner = req.assigned_to if req.assigned_to is not None else case.get("assigned_to", case.get("owner", ""))
    new_notes = req.notes if req.notes is not None else case.get("notes", "")

    case["status"] = new_state
    case["state"] = new_state
    case["assigned_to"] = new_owner
    case["owner"] = new_owner
    case["notes"] = new_notes
    case["updated_at"] = pd.Timestamp.utcnow().isoformat()

    try:
        storage.save_case_to_db(case)
        actor = _state.get("actor", "Analyst_1")
        payload = {"status": new_state, "assigned_to": new_owner}
        if req.notes is not None:
            payload["notes"] = new_notes
        storage.append_audit("case", case_id, actor, "UPDATE_CASE", payload)
    except Exception:
        pass

    return {"case_id": case_id, "status": new_state, "assigned_to": new_owner}


@app.delete("/api/cases/{case_id}")
def delete_case(case_id: str):
    """Delete a case."""
    cases = _state.get("cases", {})
    if case_id in cases:
        del cases[case_id]
        _state["cases"] = cases

    try:
        storage.delete_case(case_id)
    except Exception:
        pass

    return {"status": "deleted", "case_id": case_id}


@app.get("/api/cases/{case_id}/audit")
def get_case_audit(case_id: str):
    """Get audit log for a case."""
    cases = _state.get("cases", {})
    if case_id not in cases:
        raise HTTPException(status_code=404, detail="Case not found")

    try:
        events = storage.get_audit_log_for_case(case_id)
        return {"events": events}
    except Exception:
        return {"events": []}


@app.get("/api/health")
def get_health():
    """Model health metrics."""
    alerts_df = storage.load_alerts_by_run(_state.get("active_run_id") or "")
    if alerts_df.empty or "risk_score" not in alerts_df.columns:
        return {"status": "N/A"}

    report = health_monitor.compute_health_report(
        alerts_df=alerts_df,
        daily_stats=[],
        baseline_df=None,
    )
    return {"status": report.get("status", "N/A")}


@app.get("/api/ops-metrics")
def get_ops_metrics(analyst_capacity: int = 50):
    """Operational metrics."""
    alerts_df = storage.load_alerts_by_run(_state.get("active_run_id") or "")
    if alerts_df.empty or "risk_score" not in alerts_df.columns:
        return {"precision_k": 0, "alerts_per_case": 0, "suppression_rate": 0}

    metrics = ops_service.compute_ops_metrics(alerts_df, analyst_capacity, 30)
    return {k: float(v) if isinstance(v, (int, float)) else v for k, v in metrics.items()}
