from __future__ import annotations

from types import SimpleNamespace

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile

from core.observability import metrics_response
from core.security import get_tenant_id
from src import config as legacy_config
from src.domain.schemas import OverlayInputError
from src.services.ingestion_service import IngestionError
from src.synth_data import generate_synthetic_alerts

router = APIRouter(tags=["pipeline"])


def _user_scope(request: Request) -> str:
    return request.headers.get("X-User-Scope") or "public"


@router.get("/")
def root() -> dict:
    return {"status": "ok", "app": "AML Alert Prioritization API"}


@router.get("/health")
def health_check() -> dict:
    return {"ok": True}


@router.get("/metrics")
def metrics(request: Request):
    return metrics_response(request.app.state.metrics)


@router.post("/api/data/generate-synthetic")
def generate_synthetic(n_rows: int = 400, request: Request = None, tenant_id: str = Depends(get_tenant_id)):
    return request.app.state.ingestion_service.generate_synthetic(tenant_id=tenant_id, user_scope=_user_scope(request), n_rows=n_rows)


@router.post("/api/data/upload-csv")
async def upload_csv(request: Request, file: UploadFile = File(...), tenant_id: str = Depends(get_tenant_id)):
    try:
        contents = await file.read()
        return request.app.state.ingestion_service.upload_transactions_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
    except OverlayInputError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/data/generate-bank-csv")
def generate_bank_csv(n_rows: int = 1000, seed: int = 42, request: Request = None):
    try:
        cfg = SimpleNamespace(**{name: getattr(legacy_config, name) for name in dir(legacy_config) if name.isupper()})
        df = generate_synthetic_alerts(n_rows=max(1, min(n_rows, 10000)), cfg=cfg, seed=seed)
        out = df[["alert_id", "user_id", "amount", "segment", "country", "typology", "source_system"]].copy()
        out["timestamp_utc"] = df["timestamp"]
        out["channel"] = "bank_transfer"
        out["time_gap"] = 86400
        out["num_transactions"] = 1
        if "alert_risk_band" in df.columns:
            out["alert_risk_band"] = df["alert_risk_band"]
        cols = [
            "alert_id",
            "user_id",
            "amount",
            "segment",
            "country",
            "channel",
            "timestamp_utc",
            "time_gap",
            "num_transactions",
            "typology",
            "source_system",
        ]
        if "alert_risk_band" in out.columns:
            cols.append("alert_risk_band")
        out = out[cols]
        out_path = request.app.state.settings.data_dir / f"bank_alerts_{len(out)}.csv"
        out.to_csv(out_path, index=False, encoding="utf-8")
        return {"rows": len(out), "path": str(out_path), "message": f"Saved to {out_path}. Upload this file via Upload bank CSV."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/data/upload-bank-csv")
async def upload_bank_csv(request: Request, file: UploadFile = File(...), tenant_id: str = Depends(get_tenant_id)):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        return request.app.state.ingestion_service.upload_bank_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
    except IngestionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/pipeline/run")
def run_pipeline(request: Request, tenant_id: str = Depends(get_tenant_id)):
    try:
        initiated_by = request.headers.get("X-Actor")
        return request.app.state.pipeline_service.enqueue_pipeline_run(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            initiated_by=initiated_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/pipeline/jobs/{job_id}")
def get_pipeline_job(job_id: str, request: Request, tenant_id: str = Depends(get_tenant_id)):
    return request.app.state.pipeline_service.get_job_status(tenant_id=tenant_id, job_id=job_id)


@router.post("/api/pipeline/clear")
def clear_run(request: Request, tenant_id: str = Depends(get_tenant_id)):
    return request.app.state.pipeline_service.clear_active_run(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/run-info")
def get_run_info(request: Request, tenant_id: str = Depends(get_tenant_id)):
    return request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/health")
def get_model_health(request: Request, tenant_id: str = Depends(get_tenant_id)):
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))
    return request.app.state.pipeline_service.compute_health(run_info.get("run_id") or "")
