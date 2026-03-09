"""
Deterministic AML overlay pipeline orchestrator.
Same input + same config + same artifacts => same outputs.
Callable from backend/Streamlit as run_pipeline(source=..., config=...) -> run_id.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .. import config
from ..domain import RunRecord
from ..domain.schemas import OverlayInputError
from ..observability.logging import get_logger, log_stage
from ..rule_engine import RULE_ID_TO_LEGACY
from .stages.ingest import run_ingest
from .stages.normalize import run_normalize
from .stages.enrich import run_enrich
from .stages.score import run_score
from .stages.rules import run_rules_stage
from .stages.governance import run_governance
from .stages.persist import run_persist
from .stages.explain import run_explain
from .stages.metrics import run_metrics

logger = get_logger("pipeline")


def _canonical_config(cfg: Dict[str, Any]) -> str:
    """Canonical JSON for config hashing (sorted keys, no None values)."""
    def _clean(obj: Any) -> Any:
        if obj is None:
            return ""
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in sorted(obj.items())}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj
    return json.dumps(_clean(cfg), sort_keys=True)


def _config_hash(cfg: Dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_config(cfg).encode()).hexdigest()[:32]


def _rules_hash() -> str:
    """Deterministic hash of rule source identifiers + versions."""
    parts = [f"{k}:1.0" for k in sorted(RULE_ID_TO_LEGACY.keys())]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]


def _model_hash(dataset_hash: str, config_hash: str) -> str:
    """Deterministic model identifier from run inputs (model cache key derived from data+config)."""
    return hashlib.sha256(f"{dataset_hash}|{config_hash}".encode()).hexdigest()[:32]


def _make_run_id(dataset_hash: str, config_hash: str, policy_version: str, schema_version: str = "1.0") -> str:
    """Deterministic run_id from dataset_hash + config_hash + policy_version + schema_version (no timestamp)."""
    payload = f"{dataset_hash}|{config_hash}|{policy_version}|{schema_version}"
    h = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return f"run_{h}"


def run_pipeline(
    source: str,
    config_overrides: Optional[Dict[str, Any]] = None,
    *,
    input_path: Optional[Path] = None,
    input_df: Optional[pd.DataFrame] = None,
    input_bytes: Optional[bytes] = None,
    storage: Any = None,
    data_dir: Optional[Path] = None,
    reports_dir: Optional[Path] = None,
    dead_letter_dir: Optional[Path] = None,
) -> str:
    """
    Run the full deterministic pipeline. Returns run_id.

    Args:
        source: One of "csv", "json", "dataframe", "db_pull".
        config_overrides: Optional dict to override config (e.g. policy_version).
        input_path: Path to CSV/JSON file (for source=csv or json).
        input_df: In-memory DataFrame (for source=dataframe).
        input_bytes: Raw bytes of file (for source=csv/json when file is in memory).
        storage: Storage adapter (must have save_run, upsert_alerts, load_alerts_by_run, etc.).
        data_dir: Base data directory (data/).
        reports_dir: Where to write pilot reports (data/reports/).
        dead_letter_dir: Where to write dead-letter rows (data/dead_letter/).

    Returns:
        run_id: Unique run identifier.
    """
    cfg = config_overrides or {}
    _data = data_dir or Path(getattr(config, "DATA_DIR", "data"))
    _reports = reports_dir or (_data / "reports")
    _reports.mkdir(parents=True, exist_ok=True)
    _dead_letter = dead_letter_dir or (_data / "dead_letter")
    _dead_letter.mkdir(parents=True, exist_ok=True)

    if storage is None:
        from ..storage import get_storage
        _db_path = str(_data / "app.db")
        storage = get_storage(_db_path)

    run_id: Optional[str] = None
    dataset_hash = ""
    df: Optional[pd.DataFrame] = None
    ingest_result: Dict[str, Any] = {}

    if getattr(config, "OVERLAY_MODE", False):
        logger.info("Running in strict post-detection behavioral overlay mode. No transaction-level detection active.")

    # ----- INGEST -----
    with log_stage("ingest", run_id=None):
        ingest_result = run_ingest(
            source=source,
            input_path=input_path,
            input_df=input_df,
            input_bytes=input_bytes,
            config=cfg,
            dead_letter_dir=_dead_letter,
        )
    df = ingest_result.get("df")
    dataset_hash = ingest_result.get("dataset_hash", "")
    dataset_type = ingest_result.get("dataset_type", "alert")
    if df is None or df.empty:
        if getattr(config, "OVERLAY_MODE", False):
            raise OverlayInputError("Overlay requires alert-level input from AML monitoring systems. Ingest produced no alerts.")
        raise ValueError("Ingest produced no alerts. Check dead_letter for rejected rows.")

    # Deterministic run_id (no timestamp): same input + config => same run_id
    policy_version = str(cfg.get("policy_version", getattr(config, "CURRENT_POLICY_VERSION", "1.0")))
    schema_version = str(cfg.get("schema_version", "1.0"))
    config_hash = _config_hash(cfg)
    run_id = _make_run_id(dataset_hash, config_hash, policy_version, schema_version)

    # Idempotent run: if this run_id already exists, short-circuit and return it
    if hasattr(storage, "get_run"):
        existing = storage.get_run(run_id)
        if existing is not None:
            logger.info("Idempotent run: run_id=%s already exists, skipping pipeline", run_id)
            return run_id

    # ----- NORMALIZE -----
    with log_stage("normalize", run_id):
        df = run_normalize(df, run_id=run_id, config=cfg)

    # ----- ENRICH -----
    with log_stage("enrich", run_id):
        df = run_enrich(df, run_id=run_id, config=cfg, storage=storage)

    # ----- SCORE (overlay: only for alert-level input) -----
    if getattr(config, "OVERLAY_MODE", False) and dataset_type != "alert":
        raise OverlayInputError("Transaction-level behavioral analysis not supported. Pipeline aborted before SCORE.")
    with log_stage("score", run_id):
        df = run_score(df, run_id=run_id, config_overrides=cfg, dataset_type=dataset_type)

    # ----- RULES -----
    with log_stage("rules", run_id):
        df = run_rules_stage(df, run_id=run_id, config=cfg)

    # ----- GOVERNANCE -----
    with log_stage("governance", run_id):
        df = run_governance(df, run_id=run_id, config_overrides=cfg)

    # ----- PERSIST -----
    with log_stage("persist", run_id):
        run_persist(
            df=df,
            run_id=run_id,
            source=source,
            dataset_hash=dataset_hash,
            row_count=len(df),
            storage=storage,
            config=cfg,
        )

    # ----- RUN ARTIFACTS (determinism) -----
    policy_version = str(cfg.get("policy_version", getattr(config, "CURRENT_POLICY_VERSION", "1.0")))
    schema_version = "1.0"
    config_hash = _config_hash(cfg)
    rules_hash = _rules_hash()
    model_hash = _model_hash(dataset_hash, config_hash)
    external_versions_json = "{}"
    try:
        from ..external_data import load_all_configured_sources
        from ..external_data.constraints import compute_external_flags_and_versions
        loaded = load_all_configured_sources()
        _, ext_ver = compute_external_flags_and_versions(df.head(1), loaded)
        external_versions_json = json.dumps(ext_ver, sort_keys=True)
    except Exception:
        pass
    if hasattr(storage, "save_run_artifacts"):
        storage.save_run_artifacts(
            run_id=run_id,
            policy_version=policy_version,
            schema_version=schema_version,
            model_hash=model_hash,
            rules_hash=rules_hash,
            external_versions_json=external_versions_json,
            config_hash=config_hash,
        )

    # ----- EXPLAIN -----
    with log_stage("explain", run_id):
        df = run_explain(df, run_id=run_id, storage=storage, config=cfg)

    # Re-persist with decision_trace_json if explain added it
    with log_stage("persist_after_explain", run_id):
        run_persist(
            df=df,
            run_id=run_id,
            source=source,
            dataset_hash=dataset_hash,
            row_count=len(df),
            storage=storage,
            config=cfg,
        )

    # ----- METRICS -----
    with log_stage("metrics", run_id):
        run_metrics(
            df=df,
            run_id=run_id,
            reports_dir=_reports,
            config=cfg,
        )

    return run_id
