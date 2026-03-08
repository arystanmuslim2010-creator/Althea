from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from core.config import Settings, get_settings
from events.event_bus import EventBus
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from models.ml_model_service import MLModelService
from models.model_registry import ModelRegistry
from services.case_service import CaseWorkflowService
from services.explain_service import ExplainabilityService
from services.feature_service import EnterpriseFeatureService
from services.ingestion_service import EnterpriseIngestionService
from services.job_queue_service import JobQueueService
from services.pipeline_service import PipelineService
from services.scoring_service import EnterpriseScoringService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache


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
def get_model_registry() -> ModelRegistry:
    return ModelRegistry(get_repository(), get_object_storage())


@lru_cache(maxsize=1)
def get_ml_service() -> MLModelService:
    schema = FeatureSchemaValidator()
    registry = get_model_registry()
    inference = InferenceService(registry, schema)
    return MLModelService(registry, inference)


@lru_cache(maxsize=1)
def get_feature_service() -> EnterpriseFeatureService:
    return EnterpriseFeatureService(FeatureSchemaValidator())


@lru_cache(maxsize=1)
def get_scoring_service() -> EnterpriseScoringService:
    return EnterpriseScoringService(get_ml_service())


@lru_cache(maxsize=1)
def get_case_service() -> CaseWorkflowService:
    settings = get_settings()
    return CaseWorkflowService(get_repository(), settings.legacy_sqlite_path)


@lru_cache(maxsize=1)
def get_explain_service() -> ExplainabilityService:
    return ExplainabilityService()


@lru_cache(maxsize=1)
def get_ingestion_service() -> EnterpriseIngestionService:
    return EnterpriseIngestionService(get_repository(), get_object_storage())


@lru_cache(maxsize=1)
def get_job_queue_service() -> JobQueueService:
    return JobQueueService(get_repository(), get_cache())


@lru_cache(maxsize=1)
def get_pipeline_service() -> PipelineService:
    settings = get_settings()
    return PipelineService(
        settings=settings,
        repository=get_repository(),
        object_storage=get_object_storage(),
        event_bus=get_event_bus(),
        job_queue=get_job_queue_service(),
        feature_service=get_feature_service(),
    )


def build_app_state() -> dict:
    settings = get_settings()
    return {
        "settings": settings,
        "repository": get_repository(),
        "cache": get_cache(),
        "object_storage": get_object_storage(),
        "event_bus": get_event_bus(),
        "feature_service": get_feature_service(),
        "scoring_service": get_scoring_service(),
        "case_service": get_case_service(),
        "explain_service": get_explain_service(),
        "ingestion_service": get_ingestion_service(),
        "pipeline_service": get_pipeline_service(),
        "job_queue_service": get_job_queue_service(),
        "ml_service": get_ml_service(),
    }
