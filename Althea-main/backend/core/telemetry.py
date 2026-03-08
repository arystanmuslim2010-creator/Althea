from __future__ import annotations

import logging

from fastapi import FastAPI

from core.config import Settings

logger = logging.getLogger("althea.telemetry")


def setup_telemetry(app: FastAPI, settings: Settings) -> None:
    """
    Configure OpenTelemetry tracing for FastAPI when optional dependencies are installed.

    This remains non-fatal in local/dev environments to preserve backward compatibility.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except Exception:
        logger.info("OpenTelemetry instrumentation packages not available; running without OTel exporter.")
        return

    service_name = "althea-backend"
    resource = Resource.create(
        {
            "service.name": service_name,
            "deployment.environment": settings.app_env,
        }
    )
    provider = TracerProvider(resource=resource)

    otlp_endpoint = (
        getattr(settings, "otel_exporter_otlp_endpoint", None)
        or "http://localhost:4318/v1/traces"
    )
    try:
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    except Exception:
        logger.exception("Failed to initialize OTLP exporter endpoint=%s", otlp_endpoint)
        return

    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    logger.info("OpenTelemetry initialized endpoint=%s env=%s", otlp_endpoint, settings.app_env)

