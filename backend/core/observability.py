from __future__ import annotations

import json
import re
import time
import uuid
from collections import defaultdict, deque
from threading import Lock
from typing import Any

from fastapi import Request
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

_HTTP_REQUESTS = Counter(
    "althea_http_requests_total",
    "Total HTTP requests processed by ALTHEA.",
    ["path", "method", "status_code"],
)
_HTTP_LATENCY = Histogram(
    "althea_http_request_latency_seconds",
    "HTTP request latency in seconds.",
    ["path", "method"],
)
_PIPELINE_RUNS = Counter(
    "althea_pipeline_runs_total",
    "Pipeline runs by final status.",
    ["status"],
)
_PIPELINE_RUN_LATENCY = Histogram(
    "althea_pipeline_run_duration_seconds",
    "Pipeline run duration in seconds.",
    buckets=(0.1, 0.5, 1, 3, 5, 10, 30, 60, 120, 300, 600, 1800),
)
_PIPELINE_ALERTS = Histogram(
    "althea_pipeline_alerts_processed",
    "Alerts processed per pipeline run.",
    buckets=(10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000),
)
_WORKER_TASK_LATENCY = Histogram(
    "althea_worker_task_duration_seconds",
    "Worker task duration in seconds.",
    ["worker", "status"],
)
_ML_INFERENCE_LATENCY = Histogram(
    "althea_ml_inference_latency_seconds",
    "ML inference latency in seconds.",
    ["model_version"],
)
_EVENT_COUNT = Counter(
    "althea_event_bus_events_total",
    "Total events processed by ALTHEA event bus.",
    ["event_name"],
)
_FEATURE_RETRIEVAL_LATENCY = Histogram(
    "althea_feature_retrieval_latency_seconds",
    "Latency of feature retrieval and registry operations.",
    ["operation"],
)
_COPILOT_LATENCY = Histogram(
    "althea_copilot_generation_latency_seconds",
    "Latency of AI copilot generation endpoints.",
    ["operation"],
)
_WORKFLOW_TRANSITIONS = Counter(
    "althea_workflow_transitions_total",
    "Workflow transitions grouped by source and target states.",
    ["from_state", "to_state", "result"],
)
_INTEGRATION_ERRORS = Counter(
    "althea_integration_errors_total",
    "Frontend-backend integration errors by area.",
    ["area"],
)
_GRAPH_GENERATION_LATENCY = Histogram(
    "althea_graph_generation_latency_seconds",
    "Latency for investigation graph generation.",
)
_GRAPH_GENERATION_FAILURES = Counter(
    "althea_graph_generation_failures_total",
    "Total failures while generating investigation graphs.",
)
_NARRATIVE_GENERATION_LATENCY = Histogram(
    "althea_narrative_generation_latency_seconds",
    "Latency for investigation narrative draft generation.",
)
_NARRATIVE_GENERATION_FAILURES = Counter(
    "althea_narrative_generation_failures_total",
    "Total failures while generating investigation narrative drafts.",
)
_EXPLANATION_GENERATION_LATENCY = Histogram(
    "althea_explanation_generation_latency_seconds",
    "Latency for explanation generation.",
    ["method", "status"],
)
_EXPLANATION_GENERATION_FAILURES = Counter(
    "althea_explanation_generation_failures_total",
    "Total failures while generating model explanations.",
    ["reason"],
)
_EXPLANATION_METHOD_COUNT = Counter(
    "althea_explanation_method_count_total",
    "Count of generated explanations by method.",
    ["method"],
)
_EXPLANATION_FALLBACK_COUNT = Counter(
    "althea_explanation_fallback_count_total",
    "Count of explanation fallbacks by method and reason.",
    ["method", "reason"],
)
_DYNAMIC_GAUGES: dict[str, Gauge] = {}
_ALERTS_PROCESSED_TOTAL = Counter("alerts_processed_total", "Total number of alerts processed by pipeline jobs.")
_PIPELINE_RUNTIME_SECONDS = Histogram(
    "pipeline_runtime_seconds",
    "Pipeline runtime in seconds.",
    buckets=(0.1, 0.5, 1, 3, 5, 10, 30, 60, 120, 300, 600, 1800),
)
_ML_INFERENCE_LATENCY_SECONDS = Histogram(
    "ml_inference_latency",
    "ML inference latency in seconds.",
    ["model_version"],
)
_QUEUE_DEPTH = Gauge("queue_depth", "Current depth of background queue.")
_INGESTION_ATTEMPT_TOTAL = Counter(
    "ingestion_attempt_total",
    "Total alert-centric ingestion attempts.",
    ["source_system", "strict_mode"],
)
_INGESTION_SUCCESS_TOTAL = Counter(
    "ingestion_success_total",
    "Total successful alert-centric ingestions.",
    ["source_system", "status"],
)
_INGESTION_FAILURE_TOTAL = Counter(
    "ingestion_failure_total",
    "Total failed alert-centric ingestions.",
    ["source_system", "status", "reason_category"],
)
_INGESTION_VALIDATION_FAILURE_TOTAL = Counter(
    "ingestion_validation_failure_total",
    "Total validation-failed ingestion rows.",
    ["source_system", "reason_category"],
)
_INGESTION_WARNING_TOTAL = Counter(
    "ingestion_warning_total",
    "Total ingestion warnings emitted by rollout-time checks.",
    ["source_system", "warning_type"],
)
_INGESTION_DURATION_MS = Histogram(
    "ingestion_duration_ms",
    "Alert-centric ingestion duration in milliseconds.",
    ["source_system", "status"],
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 3000, 10000, 30000, 60000),
)
_INGESTED_ALERT_COUNT = Counter(
    "ingested_alert_count",
    "Count of successfully ingested alerts.",
    ["source_system"],
)
_INGESTED_TRANSACTION_COUNT = Counter(
    "ingested_transaction_count",
    "Count of successfully ingested transactions.",
    ["source_system"],
)
_INGESTION_DATA_QUALITY_INCONSISTENCY_TOTAL = Counter(
    "ingestion_data_quality_inconsistency_total",
    "Count of data quality inconsistencies detected during ingestion.",
    ["source_system", "issue_type"],
)
_PRIMARY_INGESTION_MODE = Gauge(
    "primary_ingestion_mode",
    "Primary ingestion mode indicator (1 for active mode).",
    ["mode"],
)
_INGESTION_PATH_USED_TOTAL = Counter(
    "ingestion_path_used_total",
    "Count of ingestion path executions by mode and outcome.",
    ["ingestion_path", "primary_mode", "status"],
)
_ALERTS_INGESTED_PER_MODE = Counter(
    "alerts_ingested_per_mode",
    "Count of ingested alerts attributed to ingestion mode.",
    ["ingestion_mode"],
)
_LEGACY_INGESTION_USAGE_TOTAL = Counter(
    "legacy_ingestion_usage_total",
    "Legacy ingestion endpoint usage for deprecation tracking.",
    ["endpoint", "status"],
)
_LEGACY_PATH_ACCESS_ATTEMPT_TOTAL = Counter(
    "legacy_path_access_attempt_total",
    "Total attempts to access legacy ingestion paths after finalization.",
    ["endpoint", "caller"],
)
_LEGACY_PATH_ACCESS_BLOCKED_TOTAL = Counter(
    "legacy_path_access_blocked_total",
    "Total blocked attempts to access disabled legacy ingestion paths.",
    ["endpoint", "caller"],
)
_ENRICHMENT_SYNC_ATTEMPT_TOTAL = Counter(
    "enrichment_sync_attempt_total",
    "Total enrichment sync attempts by source and status.",
    ["source", "status"],
)
_ENRICHMENT_SYNC_DURATION_SECONDS = Histogram(
    "enrichment_sync_duration_seconds",
    "Enrichment sync duration in seconds.",
    ["source"],
)
_ENRICHMENT_RECORDS_WRITTEN_TOTAL = Counter(
    "enrichment_records_written_total",
    "Total enrichment records written by source.",
    ["source"],
)
_ENRICHMENT_RECORDS_FAILED_TOTAL = Counter(
    "enrichment_records_failed_total",
    "Total enrichment records failed by source.",
    ["source"],
)
_ENRICHMENT_SOURCE_FRESHNESS_SECONDS = Gauge(
    "enrichment_source_freshness_seconds",
    "Freshness of enrichment source snapshots in seconds.",
    ["source"],
)
_ENRICHMENT_SOURCE_COVERAGE_RATIO = Gauge(
    "enrichment_source_coverage_ratio",
    "Coverage ratio of enrichment source over current alert population.",
    ["source"],
)
_ENRICHMENT_CONTEXT_BUILD_TOTAL = Counter(
    "enrichment_context_build_total",
    "Count of runtime enrichment context builds by status.",
    ["status"],
)
_INGESTION_RUN_HISTORY: deque[dict[str, Any]] = deque(maxlen=500)
_INGESTION_RUN_HISTORY_LOCK = Lock()
_LEGACY_ACCESS_HISTORY: deque[dict[str, Any]] = deque(maxlen=500)
_LEGACY_ACCESS_HISTORY_LOCK = Lock()


def _safe_metric_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (name or "").strip())
    if not cleaned:
        cleaned = "althea_custom_gauge"
    if not re.match(r"^[a-zA-Z_]", cleaned):
        cleaned = f"althea_{cleaned}"
    return cleaned


class MetricsRegistry:
    def __init__(self) -> None:
        self.request_count = 0
        self.path_counts: dict[str, int] = defaultdict(int)
        self.latency_sum: dict[str, float] = defaultdict(float)
        self.custom_gauges: dict[str, float] = defaultdict(float)

    def observe_request(self, path: str, duration: float, method: str = "GET", status_code: int = 200) -> None:
        self.request_count += 1
        self.path_counts[path] += 1
        self.latency_sum[path] += duration
        _HTTP_REQUESTS.labels(path=path, method=method, status_code=str(status_code)).inc()
        _HTTP_LATENCY.labels(path=path, method=method).observe(max(0.0, float(duration)))

    def set_gauge(self, name: str, value: float) -> None:
        self.custom_gauges[name] = value
        safe_name = _safe_metric_name(name)
        gauge = _DYNAMIC_GAUGES.get(safe_name)
        if gauge is None:
            gauge = Gauge(safe_name, f"ALTHEA dynamic gauge {safe_name}")
            _DYNAMIC_GAUGES[safe_name] = gauge
        gauge.set(float(value))

    def observe_pipeline_run(self, status: str, duration_seconds: float, alerts_processed: int) -> None:
        record_pipeline_run(status=status, duration_seconds=duration_seconds, alerts_processed=alerts_processed)

    def observe_worker_task(self, worker_name: str, status: str, duration_seconds: float) -> None:
        record_worker_task(worker_name=worker_name, status=status, duration_seconds=duration_seconds)

    def observe_ml_inference(self, model_version: str, duration_seconds: float) -> None:
        record_ml_inference(model_version=model_version, duration_seconds=duration_seconds)

    def observe_event(self, event_name: str) -> None:
        record_event(event_name=event_name)

    def prometheus(self) -> str:
        return generate_latest().decode("utf-8")


async def correlation_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request.state.request_id = request_id
    start = time.perf_counter()
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception:
        duration = time.perf_counter() - start
        metrics: MetricsRegistry = request.app.state.metrics
        metrics.observe_request(request.url.path, duration, method=request.method, status_code=500)
        raise
    duration = time.perf_counter() - start
    metrics: MetricsRegistry = request.app.state.metrics
    metrics.observe_request(request.url.path, duration, method=request.method, status_code=status_code)
    response.headers["X-Request-ID"] = request_id
    return response


def log_event(logger, request: Request, event: str, **payload: Any) -> None:
    envelope = {
        "event": event,
        "request_id": getattr(request.state, "request_id", ""),
        "path": str(request.url.path),
        **payload,
    }
    logger.info(json.dumps(envelope, ensure_ascii=True, default=str))


def record_pipeline_run(status: str, duration_seconds: float, alerts_processed: int) -> None:
    _PIPELINE_RUNS.labels(status=(status or "unknown")).inc()
    _PIPELINE_RUN_LATENCY.observe(max(0.0, float(duration_seconds)))
    _PIPELINE_ALERTS.observe(max(0.0, float(alerts_processed)))
    _PIPELINE_RUNTIME_SECONDS.observe(max(0.0, float(duration_seconds)))
    _ALERTS_PROCESSED_TOTAL.inc(max(0, int(alerts_processed)))


def record_worker_task(worker_name: str, status: str, duration_seconds: float) -> None:
    _WORKER_TASK_LATENCY.labels(worker=worker_name or "unknown", status=status or "unknown").observe(
        max(0.0, float(duration_seconds))
    )


def record_ml_inference(model_version: str, duration_seconds: float) -> None:
    _ML_INFERENCE_LATENCY.labels(model_version=model_version or "unknown").observe(max(0.0, float(duration_seconds)))
    _ML_INFERENCE_LATENCY_SECONDS.labels(model_version=model_version or "unknown").observe(max(0.0, float(duration_seconds)))


def record_event(event_name: str) -> None:
    _EVENT_COUNT.labels(event_name=event_name or "unknown").inc()


def record_feature_retrieval(operation: str, duration_seconds: float) -> None:
    _FEATURE_RETRIEVAL_LATENCY.labels(operation=operation or "unknown").observe(max(0.0, float(duration_seconds)))


def record_copilot_generation(operation: str, duration_seconds: float) -> None:
    _COPILOT_LATENCY.labels(operation=operation or "unknown").observe(max(0.0, float(duration_seconds)))


def record_workflow_transition(from_state: str, to_state: str, result: str = "success") -> None:
    _WORKFLOW_TRANSITIONS.labels(
        from_state=(from_state or "unknown"),
        to_state=(to_state or "unknown"),
        result=(result or "unknown"),
    ).inc()


def record_integration_error(area: str) -> None:
    _INTEGRATION_ERRORS.labels(area=area or "unknown").inc()


def record_graph_generation(duration_seconds: float) -> None:
    _GRAPH_GENERATION_LATENCY.observe(max(0.0, float(duration_seconds)))


def record_graph_generation_failure() -> None:
    _GRAPH_GENERATION_FAILURES.inc()


def record_narrative_generation(duration_seconds: float) -> None:
    _NARRATIVE_GENERATION_LATENCY.observe(max(0.0, float(duration_seconds)))


def record_narrative_generation_failure() -> None:
    _NARRATIVE_GENERATION_FAILURES.inc()


def record_explanation_generation(method: str, status: str, duration_seconds: float) -> None:
    normalized_method = (method or "unknown").strip().lower() or "unknown"
    normalized_status = (status or "unknown").strip().lower() or "unknown"
    _EXPLANATION_METHOD_COUNT.labels(method=normalized_method).inc()
    _EXPLANATION_GENERATION_LATENCY.labels(method=normalized_method, status=normalized_status).observe(
        max(0.0, float(duration_seconds))
    )


def record_explanation_failure(reason: str) -> None:
    normalized_reason = (reason or "unknown").strip().lower() or "unknown"
    _EXPLANATION_GENERATION_FAILURES.labels(reason=normalized_reason).inc()


def record_explanation_fallback(method: str, reason: str) -> None:
    normalized_method = (method or "unknown").strip().lower() or "unknown"
    normalized_reason = (reason or "unknown").strip().lower() or "unknown"
    _EXPLANATION_FALLBACK_COUNT.labels(method=normalized_method, reason=normalized_reason).inc()


def record_queue_depth(depth: int) -> None:
    _QUEUE_DEPTH.set(max(0, int(depth)))


def record_ingestion_attempt(source_system: str, strict_mode: bool) -> None:
    _INGESTION_ATTEMPT_TOTAL.labels(
        source_system=(source_system or "unknown"),
        strict_mode=("true" if strict_mode else "false"),
    ).inc()


def record_ingestion_summary(summary: dict[str, Any]) -> None:
    source_system = str(summary.get("source_system") or "unknown")
    status = str(summary.get("status") or "unknown")
    reason_category = str(summary.get("failure_reason_category") or "none")
    failed_count = max(0, int(summary.get("failed_count") or 0))
    warning_count = max(0, int(summary.get("warning_count") or 0))
    duration_ms = max(0.0, float(summary.get("elapsed_ms") or 0.0))
    success_count = max(0, int(summary.get("success_count") or 0))
    tx_count = max(0, int(summary.get("ingested_transaction_count") or 0))

    if status in {"accepted", "partially_ingested"}:
        _INGESTION_SUCCESS_TOTAL.labels(source_system=source_system, status=status).inc()
    else:
        _INGESTION_FAILURE_TOTAL.labels(
            source_system=source_system,
            status=status,
            reason_category=reason_category,
        ).inc()

    if failed_count > 0:
        _INGESTION_VALIDATION_FAILURE_TOTAL.labels(
            source_system=source_system,
            reason_category=reason_category,
        ).inc(failed_count)
    if warning_count > 0:
        _INGESTION_WARNING_TOTAL.labels(source_system=source_system, warning_type="generic").inc(warning_count)

    _INGESTION_DURATION_MS.labels(source_system=source_system, status=status).observe(duration_ms)
    if success_count > 0:
        _INGESTED_ALERT_COUNT.labels(source_system=source_system).inc(success_count)
    if tx_count > 0:
        _INGESTED_TRANSACTION_COUNT.labels(source_system=source_system).inc(tx_count)

    data_quality = summary.get("data_quality_counts")
    if isinstance(data_quality, dict):
        for issue_type, count in data_quality.items():
            issue_count = max(0, int(count or 0))
            if issue_count <= 0:
                continue
            _INGESTION_DATA_QUALITY_INCONSISTENCY_TOTAL.labels(
                source_system=source_system,
                issue_type=str(issue_type or "unknown"),
            ).inc(issue_count)

    run_snapshot = {
        "run_id": str(summary.get("run_id") or ""),
        "source_system": source_system,
        "status": status,
        "reason_category": reason_category,
        "total_rows": max(0, int(summary.get("total_rows") or 0)),
        "success_count": success_count,
        "failed_count": failed_count,
        "warning_count": warning_count,
        "elapsed_ms": duration_ms,
        "ingested_alert_count": max(0, int(summary.get("ingested_alert_count") or success_count)),
        "ingested_transaction_count": tx_count,
        "data_quality_inconsistency_count": max(0, int(summary.get("data_quality_inconsistency_count") or 0)),
        "critical_data_quality_issues": list(summary.get("critical_data_quality_issues") or []),
        "critical_issue_count": max(0, int(summary.get("critical_issue_count") or 0)),
        "recorded_at_epoch_ms": int(time.time() * 1000),
    }
    with _INGESTION_RUN_HISTORY_LOCK:
        _INGESTION_RUN_HISTORY.append(run_snapshot)


def record_primary_ingestion_mode(mode: str) -> None:
    normalized = str(mode or "legacy").strip().lower()
    if normalized not in {"legacy", "alert_jsonl"}:
        normalized = "legacy"
    for item in ("legacy", "alert_jsonl"):
        _PRIMARY_INGESTION_MODE.labels(mode=item).set(1.0 if item == normalized else 0.0)


def record_ingestion_path_used(
    ingestion_path: str,
    primary_mode: str,
    status: str,
    alerts_ingested: int = 0,
) -> None:
    normalized_path = str(ingestion_path or "unknown").strip().lower() or "unknown"
    normalized_primary = str(primary_mode or "legacy").strip().lower() or "legacy"
    normalized_status = str(status or "unknown").strip().lower() or "unknown"
    _INGESTION_PATH_USED_TOTAL.labels(
        ingestion_path=normalized_path,
        primary_mode=normalized_primary,
        status=normalized_status,
    ).inc()
    if int(alerts_ingested or 0) > 0:
        _ALERTS_INGESTED_PER_MODE.labels(ingestion_mode=normalized_path).inc(int(alerts_ingested))


def record_legacy_ingestion_usage(endpoint: str, status: str) -> None:
    normalized_endpoint = str(endpoint or "unknown").strip().lower() or "unknown"
    normalized_status = str(status or "unknown").strip().lower() or "unknown"
    _LEGACY_INGESTION_USAGE_TOTAL.labels(endpoint=normalized_endpoint, status=normalized_status).inc()


def record_legacy_path_access(endpoint: str, caller: str = "unknown", blocked: bool = True) -> None:
    normalized_endpoint = str(endpoint or "unknown").strip().lower() or "unknown"
    normalized_caller = str(caller or "unknown").strip().lower() or "unknown"
    _LEGACY_PATH_ACCESS_ATTEMPT_TOTAL.labels(
        endpoint=normalized_endpoint,
        caller=normalized_caller,
    ).inc()
    if bool(blocked):
        _LEGACY_PATH_ACCESS_BLOCKED_TOTAL.labels(
            endpoint=normalized_endpoint,
            caller=normalized_caller,
        ).inc()
    with _LEGACY_ACCESS_HISTORY_LOCK:
        _LEGACY_ACCESS_HISTORY.append(
            {
                "endpoint": normalized_endpoint,
                "caller": normalized_caller,
                "blocked": bool(blocked),
                "recorded_at_epoch_ms": int(time.time() * 1000),
            }
        )


def get_recent_legacy_path_accesses(limit: int = 50) -> list[dict[str, Any]]:
    bounded = max(1, min(int(limit or 50), 500))
    with _LEGACY_ACCESS_HISTORY_LOCK:
        items = list(_LEGACY_ACCESS_HISTORY)
    if not items:
        return []
    return items[-bounded:]


def get_legacy_path_access_snapshot(limit: int = 100) -> dict[str, Any]:
    rows = get_recent_legacy_path_accesses(limit=limit)
    attempts = len(rows)
    blocked = sum(1 for row in rows if bool(row.get("blocked")))
    by_endpoint: dict[str, int] = defaultdict(int)
    for row in rows:
        endpoint = str(row.get("endpoint") or "unknown")
        by_endpoint[endpoint] += 1
    return {
        "window_events": max(1, min(int(limit or 100), 500)),
        "attempt_count": attempts,
        "blocked_count": blocked,
        "by_endpoint": dict(by_endpoint),
        "last_event": dict(rows[-1]) if rows else {},
    }


def get_recent_ingestion_summaries(limit: int = 20, source_system: str | None = "alert_jsonl") -> list[dict[str, Any]]:
    bounded = max(1, min(int(limit or 20), 500))
    with _INGESTION_RUN_HISTORY_LOCK:
        items = list(_INGESTION_RUN_HISTORY)
    if source_system:
        target = str(source_system).strip().lower()
        items = [item for item in items if str(item.get("source_system") or "").strip().lower() == target]
    if not items:
        return []
    return items[-bounded:]


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    p = max(0.0, min(100.0, float(percentile)))
    rank = int(round(((len(ordered) - 1) * p) / 100.0))
    rank = max(0, min(rank, len(ordered) - 1))
    return float(ordered[rank])


def get_rollout_metrics_snapshot(window_runs: int = 20, source_system: str | None = "alert_jsonl") -> dict[str, Any]:
    rows = get_recent_ingestion_summaries(limit=window_runs, source_system=source_system)
    if not rows:
        return {
            "window_runs": max(1, min(int(window_runs or 20), 500)),
            "run_count": 0,
            "success_rate": 0.0,
            "failure_rate": 0.0,
            "validation_error_rate": 0.0,
            "avg_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "total_alerts_ingested": 0,
            "total_rows_seen": 0,
            "data_quality_issue_rate": 0.0,
            "critical_issue_runs": 0,
            "repeated_failure_runs": 0,
            "last_status": "none",
            "last_run_id": "",
        }

    run_count = len(rows)
    success_runs = sum(1 for row in rows if str(row.get("status") or "") in {"accepted", "partially_ingested"})
    failed_runs = run_count - success_runs
    total_rows = sum(max(0, int(row.get("total_rows") or 0)) for row in rows)
    failed_rows = sum(max(0, int(row.get("failed_count") or 0)) for row in rows)
    quality_issues = sum(max(0, int(row.get("data_quality_inconsistency_count") or 0)) for row in rows)
    total_alerts = sum(max(0, int(row.get("ingested_alert_count") or 0)) for row in rows)
    latencies = [max(0.0, float(row.get("elapsed_ms") or 0.0)) for row in rows]
    critical_issue_runs = sum(1 for row in rows if int(row.get("critical_issue_count") or 0) > 0)
    repeated_failure_runs = sum(
        1 for row in rows if str(row.get("status") or "") in {"rejected", "failed_validation"}
    )
    last_row = rows[-1]

    return {
        "window_runs": max(1, min(int(window_runs or 20), 500)),
        "run_count": run_count,
        "success_rate": float(success_runs / run_count) if run_count else 0.0,
        "failure_rate": float(failed_runs / run_count) if run_count else 0.0,
        "validation_error_rate": float(failed_rows / total_rows) if total_rows else 0.0,
        "avg_latency_ms": float(sum(latencies) / len(latencies)) if latencies else 0.0,
        "p95_latency_ms": _percentile(latencies, 95.0),
        "total_alerts_ingested": total_alerts,
        "total_rows_seen": total_rows,
        "data_quality_issue_rate": float(quality_issues / total_rows) if total_rows else 0.0,
        "critical_issue_runs": critical_issue_runs,
        "repeated_failure_runs": repeated_failure_runs,
        "last_status": str(last_row.get("status") or "unknown"),
        "last_run_id": str(last_row.get("run_id") or ""),
    }


def record_enrichment_sync_attempt(source: str, status: str) -> None:
    _ENRICHMENT_SYNC_ATTEMPT_TOTAL.labels(source=source or "unknown", status=status or "unknown").inc()


def record_enrichment_sync_duration(source: str, duration_seconds: float) -> None:
    _ENRICHMENT_SYNC_DURATION_SECONDS.labels(source=source or "unknown").observe(max(0.0, float(duration_seconds)))


def record_enrichment_records_written(source: str, count: int) -> None:
    if int(count or 0) > 0:
        _ENRICHMENT_RECORDS_WRITTEN_TOTAL.labels(source=source or "unknown").inc(int(count))


def record_enrichment_records_failed(source: str, count: int) -> None:
    if int(count or 0) > 0:
        _ENRICHMENT_RECORDS_FAILED_TOTAL.labels(source=source or "unknown").inc(int(count))


def set_enrichment_source_health(source: str, freshness_seconds: float, coverage_ratio: float) -> None:
    _ENRICHMENT_SOURCE_FRESHNESS_SECONDS.labels(source=source or "unknown").set(max(0.0, float(freshness_seconds)))
    _ENRICHMENT_SOURCE_COVERAGE_RATIO.labels(source=source or "unknown").set(max(0.0, float(coverage_ratio)))


def record_enrichment_context_build(status: str) -> None:
    _ENRICHMENT_CONTEXT_BUILD_TOTAL.labels(status=status or "unknown").inc()


def metrics_response(metrics: MetricsRegistry) -> PlainTextResponse:
    return PlainTextResponse(metrics.prometheus(), media_type=CONTENT_TYPE_LATEST)
