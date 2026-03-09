from __future__ import annotations

import json
import re
import time
import uuid
from collections import defaultdict
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


def record_queue_depth(depth: int) -> None:
    _QUEUE_DEPTH.set(max(0, int(depth)))


def metrics_response(metrics: MetricsRegistry) -> PlainTextResponse:
    return PlainTextResponse(metrics.prometheus(), media_type=CONTENT_TYPE_LATEST)
