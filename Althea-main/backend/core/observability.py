from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from typing import Any

from fastapi import Request
from fastapi.responses import PlainTextResponse


class MetricsRegistry:
    def __init__(self) -> None:
        self.request_count = 0
        self.path_counts: dict[str, int] = defaultdict(int)
        self.latency_sum: dict[str, float] = defaultdict(float)
        self.custom_gauges: dict[str, float] = defaultdict(float)

    def observe_request(self, path: str, duration: float) -> None:
        self.request_count += 1
        self.path_counts[path] += 1
        self.latency_sum[path] += duration

    def set_gauge(self, name: str, value: float) -> None:
        self.custom_gauges[name] = value

    def prometheus(self) -> str:
        lines = [
            "# TYPE althea_http_requests_total counter",
            f"althea_http_requests_total {self.request_count}",
        ]
        for path, value in sorted(self.path_counts.items()):
            safe_path = path.replace('"', '\\"')
            lines.append(f'althea_http_path_requests_total{{path="{safe_path}"}} {value}')
        for path, value in sorted(self.latency_sum.items()):
            safe_path = path.replace('"', '\\"')
            lines.append(f'althea_http_path_latency_seconds_sum{{path="{safe_path}"}} {value}')
        for name, value in sorted(self.custom_gauges.items()):
            lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


async def correlation_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request.state.request_id = request_id
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start
    metrics: MetricsRegistry = request.app.state.metrics
    metrics.observe_request(request.url.path, duration)
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


def metrics_response(metrics: MetricsRegistry) -> PlainTextResponse:
    return PlainTextResponse(metrics.prometheus(), media_type="text/plain; version=0.0.4")
