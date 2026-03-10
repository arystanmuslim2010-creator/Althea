from __future__ import annotations

import uuid
from functools import lru_cache

from core.config import Settings, get_settings
from core.observability import MetricsRegistry
from ai_copilot.copilot_service import AICopilotService
from events.event_bus import EventBus
from events.streaming.broker import StreamingBackbone
from events.streaming.consumers import (
    CaseCreationConsumer,
    FeatureServiceConsumer,
    GovernanceConsumer,
    ModelScoringConsumer,
    StreamingPipelineOrchestrator,
)
from features.feature_materialization import FeatureMaterializationService
from features.feature_registry import FeatureRegistry
from features.offline_feature_store import OfflineFeatureStore
from features.online_feature_store import OnlineFeatureStore
from model_governance.explainability import GovernanceExplainabilityService
from model_governance.lifecycle import ModelGovernanceLifecycle
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
from workflows.workflow_engine import InvestigationWorkflowEngine


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
def get_feature_registry() -> FeatureRegistry:
    return FeatureRegistry(get_repository())


@lru_cache(maxsize=1)
def get_online_feature_store() -> OnlineFeatureStore:
    settings = get_settings()
    return OnlineFeatureStore(get_cache(), ttl_seconds=settings.feature_online_ttl_seconds)


@lru_cache(maxsize=1)
def get_offline_feature_store() -> OfflineFeatureStore:
    settings = get_settings()
    return OfflineFeatureStore(get_repository(), root_dir=settings.object_storage_root)


@lru_cache(maxsize=1)
def get_feature_materialization_service() -> FeatureMaterializationService:
    return FeatureMaterializationService(
        registry=get_feature_registry(),
        online_store=get_online_feature_store(),
        offline_store=get_offline_feature_store(),
    )


@lru_cache(maxsize=1)
def get_model_registry() -> ModelRegistry:
    return ModelRegistry(get_repository(), get_object_storage())


@lru_cache(maxsize=1)
def get_inference_service() -> InferenceService:
    return InferenceService(
        registry=get_model_registry(),
        schema_validator=get_feature_schema_validator(),
        online_feature_store=get_online_feature_store(),
        feature_registry=get_feature_registry(),
    )


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
    return GovernanceService(
        repository=get_repository(),
        explainability_service=get_governance_explainability_service(),
        lifecycle_service=get_model_governance_lifecycle(),
    )


@lru_cache(maxsize=1)
def get_model_governance_lifecycle() -> ModelGovernanceLifecycle:
    return ModelGovernanceLifecycle(get_repository())


@lru_cache(maxsize=1)
def get_governance_explainability_service() -> GovernanceExplainabilityService:
    return GovernanceExplainabilityService()


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
        streaming_orchestrator=get_streaming_orchestrator(),
    )


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    return OpsService()


@lru_cache(maxsize=1)
def get_workflow_engine() -> InvestigationWorkflowEngine:
    return InvestigationWorkflowEngine(get_repository(), get_case_service())


@lru_cache(maxsize=1)
def get_ai_copilot_service() -> AICopilotService:
    return AICopilotService(get_repository(), get_explain_service())


@lru_cache(maxsize=1)
def get_streaming_backbone() -> StreamingBackbone:
    settings = get_settings()
    return StreamingBackbone(
        cache=get_cache(),
        provider=settings.streaming_provider,
        stream_prefix=settings.streaming_prefix,
    )


@lru_cache(maxsize=1)
def get_streaming_orchestrator() -> StreamingPipelineOrchestrator:
    stream = get_streaming_backbone()
    repository = get_repository()
    feature_consumer = FeatureServiceConsumer(stream, repository, get_feature_service(), get_online_feature_store())
    scoring_consumer = ModelScoringConsumer(stream, repository, get_inference_service())
    governance_consumer = GovernanceConsumer(stream, repository, get_governance_service())
    case_consumer = CaseCreationConsumer(stream, repository, get_workflow_engine())
    return StreamingPipelineOrchestrator(
        stream=stream,
        feature_consumer=feature_consumer,
        scoring_consumer=scoring_consumer,
        governance_consumer=governance_consumer,
        case_consumer=case_consumer,
    )


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
        "streaming_backbone": get_streaming_backbone(),
        "streaming_orchestrator": get_streaming_orchestrator(),
        "feature_service": get_feature_service(),
        "feature_registry": get_feature_registry(),
        "feature_materialization_service": get_feature_materialization_service(),
        "scoring_service": get_scoring_service(),
        "case_service": get_case_service(),
        "explain_service": get_explain_service(),
        "governance_explainability_service": get_governance_explainability_service(),
        "model_governance_lifecycle": get_model_governance_lifecycle(),
        "ingestion_service": get_ingestion_service(),
        "pipeline_service": get_pipeline_service(),
        "job_queue_service": get_job_queue_service(),
        "governance_service": get_governance_service(),
        "workflow_engine": get_workflow_engine(),
        "ai_copilot_service": get_ai_copilot_service(),
        "inference_service": get_inference_service(),
        "ml_service": get_ml_service(),
        "model_monitoring_service": get_model_monitoring_service(),
        "ops_service": get_ops_service(),
        "metrics": get_metrics_registry(),
    }
