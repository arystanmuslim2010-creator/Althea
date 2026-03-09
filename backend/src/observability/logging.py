"""Structured logging with run_id and stage name for auditability."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger("aml.overlay")


def get_logger(name: str) -> logging.Logger:
    """Return a logger with optional prefix (e.g. pipeline, ingest)."""
    if name:
        return logging.getLogger(f"aml.overlay.{name}")
    return logger


@contextmanager
def log_stage(stage: str, run_id: Optional[str] = None):
    """Context manager to log stage start/end with run_id."""
    log = get_logger("pipeline")
    extra = {"run_id": run_id or "", "stage": stage}
    log.info("stage_start", extra=extra)
    try:
        yield
        log.info("stage_end", extra=extra)
    except Exception as e:
        log.exception("stage_failed", extra={**extra, "error": str(e)})
        raise
