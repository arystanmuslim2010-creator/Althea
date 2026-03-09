# AML Overlay Pipeline: INGEST -> NORMALIZE -> ENRICH -> SCORE -> RULES -> GOVERNANCE -> PERSIST -> EXPLAIN -> METRICS
from .orchestrator import run_pipeline

__all__ = ["run_pipeline"]
