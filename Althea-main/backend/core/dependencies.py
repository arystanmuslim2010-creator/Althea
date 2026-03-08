from __future__ import annotations

import uuid
from functools import lru_cache

from core.config import Settings, get_settings
from core.observability import MetricsRegistry
from events.event_bus import EventBus
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from models.ml_model_service import MLModelService
from models.model_registry import ModelRegistry
from services.case_service import CaseWorkflowService
from services.explain_service import ExplainabilityService
from services.feature_service import EnterpriseFeatureService
from services.governance_service import GovernanceService
from services.ingestion_service import EnterpriseIngestionService
from services.job_queue_service import JobQueueService
from services.model_monitoring_service import ModelMonitoringService
from services.ops_service import OpsService
from services.pipeline_service import PipelineService
from services.scoring_service import EnterpriseScoringService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache


def _validate_dependency_graph(
    settings: Settings,
    repository: EnterpriseRepository,
    cache: RedisCache,
    object_storage: ObjectStorage,
) -> None:
    repository.ping()
    if not settings.jwt_secret or len(settings.jwt_secret.strip()) < 8:
        raise RuntimeError("JWT secret is not configured.")
    if settings.queue_mode != "rq":
        raise RuntimeError("Only ALTHEA_QUEUE_MODE='rq' is supported in hardened architecture.")
    if not settings.redis_url:
        raise RuntimeError("ALTHEA_REDIS_URL must be configured.")
    cache.ping()
    object_storage_root = settings.object_storage_root
    object_storage_root.mkdir(parents=True, exist_ok=True)
    probe_uri = f"_health/probe-{uuid.uuid4().hex}.txt"
    object_storage.put_bytes(probe_uri, b"ok")
    _ = object_storage.get_bytes(probe_uri)


@lru_cache(maxsize=1)
def get_repository() -> EnterpriseRepository:
    settings = get_settings()
    return EnterpriseRepository(settings.runtime_database_url)


@lru_cache(maxsize=1)
def get_cache() -> RedisCache:
    settings = get_settings()
    return RedisCache(settings.redis_url)


@lru_cache(maxsize=1)
def get_object_storage() -> ObjectStorage:
    settings = get_settings()
    return ObjectStorage(settings.object_storage_root)


@lru_cache(maxsize=1)
def get_event_bus() -> EventBus:
    return EventBus(get_cache())


@lru_cache(maxsize=1)
def get_feature_schema_validator() -> FeatureSchemaValidator:
    return FeatureSchemaValidator()


@lru_cache(maxsize=1)
def get_feature_service() -> EnterpriseFeatureService:
    return EnterpriseFeatureService(get_feature_schema_validator())


@lru_cache(maxsize=1)
def get_model_registry() -> ModelRegistry:
    return ModelRegistry(get_repository(), get_object_storage())


@lru_cache(maxsize=1)
def get_inference_service() -> InferenceService:
    return InferenceService(registry=get_model_registry(), schema_validator=get_feature_schema_validator())


@lru_cache(maxsize=1)
def get_ml_service() -> MLModelService:
    return MLModelService(get_model_registry(), get_inference_service())


@lru_cache(maxsize=1)
def get_scoring_service() -> EnterpriseScoringService:
    return EnterpriseScoringService(get_ml_service())


@lru_cache(maxsize=1)
def get_case_service() -> CaseWorkflowService:
    return CaseWorkflowService(get_repository())


@lru_cache(maxsize=1)
def get_explain_service() -> ExplainabilityService:
    return ExplainabilityService(get_repository())


@lru_cache(maxsize=1)
def get_ingestion_service() -> EnterpriseIngestionService:
    return EnterpriseIngestionService(get_repository(), get_object_storage())


@lru_cache(maxsize=1)
def get_job_queue_service() -> JobQueueService:
    return JobQueueService(get_repository(), get_cache())


@lru_cache(maxsize=1)
def get_governance_service() -> GovernanceService:
    return GovernanceService()


@lru_cache(maxsize=1)
def get_model_monitoring_service() -> ModelMonitoringService:
    return ModelMonitoringService(get_repository())


@lru_cache(maxsize=1)
def get_pipeline_service() -> PipelineService:
    settings = get_settings()
    return PipelineService(
        settings=settings,
        repository=get_repository(),
        event_bus=get_event_bus(),
        job_queue=get_job_queue_service(),
        ingestion_service=get_ingestion_service(),
        feature_service=get_feature_service(),
        inference_service=get_inference_service(),
        governance_service=get_governance_service(),
        model_monitoring_service=get_model_monitoring_service(),
    )


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    return OpsService()


@lru_cache(maxsize=1)
def get_metrics_registry() -> MetricsRegistry:
    return MetricsRegistry()


def build_app_state() -> dict:
    settings = get_settings()
    repository = get_repository()
    cache = get_cache()
    object_storage = get_object_storage()
    _validate_dependency_graph(settings, repository, cache, object_storage)

    return {
        "settings": settings,
        "repository": repository,
        "cache": cache,
        "object_storage": object_storage,
        "event_bus": get_event_bus(),
        "feature_service": get_feature_service(),
        "scoring_service": get_scoring_service(),
        "case_service": get_case_service(),
        "explain_service": get_explain_service(),
        "ingestion_service": get_ingestion_service(),
        "pipeline_service": get_pipeline_service(),
        "job_queue_service": get_job_queue_service(),
        "governance_service": get_governance_service(),
        "inference_service": get_inference_service(),
        "ml_service": get_ml_service(),
        "model_monitoring_service": get_model_monitoring_service(),
        "ops_service": get_ops_service(),
        "metrics": get_metrics_registry(),
    }

