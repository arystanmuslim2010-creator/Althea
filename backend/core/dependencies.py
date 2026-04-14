from __future__ import annotations

import uuid
from functools import lru_cache

from core.config import Settings, get_settings
from core.observability import MetricsRegistry
from ai_copilot.copilot_service import AICopilotService
from events.event_bus import EventBus
from graph.relationship_graph_service import RelationshipGraphService
from intelligence.global_pattern_service import GlobalPatternService
from investigation.guidance_service import InvestigationGuidanceService
from investigation.investigation_summary_service import InvestigationSummaryService
from investigation.narrative_service import InvestigationNarrativeService
from investigation.risk_explanation_service import RiskExplanationService
from investigation.sar_generator import SARNarrativeGenerator
from learning.feedback_collection_service import FeedbackCollectionService
from models.investigation_time_service import InvestigationTimeService
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
from models.explainability_service import get_explainability_service
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from models.ml_model_service import MLModelService
from models.model_registry import ModelRegistry
from services.case_service import CaseWorkflowService
from services.explain_service import ExplainabilityService
from services.alert_ingestion_service import AlertIngestionService
from services.feature_adapter import AlertFeatureAdapter
from services.feature_service import EnterpriseFeatureService
from services.governance_service import GovernanceService
from services.ingestion_service import EnterpriseIngestionService
from services.job_queue_service import JobQueueService
from services.model_monitoring_service import ModelMonitoringService
from services.ops_service import OpsService
from services.pipeline_service import PipelineService
from services.scoring_service import EnterpriseScoringService
from services.time_scoring_service import TimeScoringService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache
from training.outcome_joiner import OutcomeJoiner
from training.retraining_scheduler import RetrainingScheduler
from training.training_run_service import TrainingRunService
from retrieval.retrieval_service import RetrievalService
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
    settings = get_settings()
    return InferenceService(
        registry=get_model_registry(),
        schema_validator=get_feature_schema_validator(),
        online_feature_store=get_online_feature_store(),
        feature_registry=get_feature_registry(),
        explainability_service=get_explainability_service(),
        allow_dev_models=settings.allow_dev_models,
        max_cached_models=5,
    )


@lru_cache(maxsize=1)
def get_ml_service() -> MLModelService:
    return MLModelService(get_model_registry(), get_inference_service())


@lru_cache(maxsize=1)
def get_investigation_time_service() -> InvestigationTimeService:
    return InvestigationTimeService(
        registry=get_model_registry(),
        object_storage=get_object_storage(),
    )


@lru_cache(maxsize=1)
def get_time_scoring_service() -> TimeScoringService:
    return TimeScoringService(get_investigation_time_service())


@lru_cache(maxsize=1)
def get_scoring_service() -> EnterpriseScoringService:
    return EnterpriseScoringService(get_ml_service(), get_time_scoring_service())


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
def get_alert_ingestion_service() -> AlertIngestionService:
    return AlertIngestionService()


@lru_cache(maxsize=1)
def get_alert_feature_adapter() -> AlertFeatureAdapter:
    return AlertFeatureAdapter()


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
    return GovernanceExplainabilityService(explainability_service=get_explainability_service())


@lru_cache(maxsize=1)
def get_model_monitoring_service() -> ModelMonitoringService:
    return ModelMonitoringService(get_repository())


@lru_cache(maxsize=1)
def get_outcome_joiner() -> OutcomeJoiner:
    return OutcomeJoiner(get_repository())


@lru_cache(maxsize=1)
def get_training_run_service() -> TrainingRunService:
    return TrainingRunService(
        repository=get_repository(),
        object_storage=get_object_storage(),
        model_registry=get_model_registry(),
    )


@lru_cache(maxsize=1)
def get_retraining_scheduler() -> RetrainingScheduler:
    return RetrainingScheduler(
        training_run_service=get_training_run_service(),
        outcome_joiner=get_outcome_joiner(),
    )


@lru_cache(maxsize=1)
def get_retrieval_service() -> RetrievalService:
    return RetrievalService(get_repository(), get_object_storage())


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
        alert_ingestion_service=get_alert_ingestion_service(),
        feature_adapter=get_alert_feature_adapter(),
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


# ── Investigation Intelligence Services ─────────────────────────────────────


@lru_cache(maxsize=1)
def get_investigation_summary_service() -> InvestigationSummaryService:
    return InvestigationSummaryService(get_repository(), get_explain_service())


@lru_cache(maxsize=1)
def get_risk_explanation_service() -> RiskExplanationService:
    return RiskExplanationService(get_repository(), get_explain_service())


@lru_cache(maxsize=1)
def get_guidance_service() -> InvestigationGuidanceService:
    return InvestigationGuidanceService(get_repository())


@lru_cache(maxsize=1)
def get_sar_generator() -> SARNarrativeGenerator:
    return SARNarrativeGenerator(get_repository(), get_explain_service())


@lru_cache(maxsize=1)
def get_narrative_service() -> InvestigationNarrativeService:
    return InvestigationNarrativeService(get_repository(), get_explain_service())


@lru_cache(maxsize=1)
def get_relationship_graph_service() -> RelationshipGraphService:
    return RelationshipGraphService(get_repository())


@lru_cache(maxsize=1)
def get_feedback_service() -> FeedbackCollectionService:
    return FeedbackCollectionService(get_repository())


@lru_cache(maxsize=1)
def get_global_pattern_service() -> GlobalPatternService:
    return GlobalPatternService(get_repository())


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
        "alert_ingestion_service": get_alert_ingestion_service(),
        "feature_adapter": get_alert_feature_adapter(),
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
        # Investigation Intelligence
        "investigation_summary_service": get_investigation_summary_service(),
        "risk_explanation_service": get_risk_explanation_service(),
        "guidance_service": get_guidance_service(),
        "sar_generator": get_sar_generator(),
        "narrative_service": get_narrative_service(),
        "relationship_graph_service": get_relationship_graph_service(),
        "feedback_service": get_feedback_service(),
        "global_pattern_service": get_global_pattern_service(),
        "investigation_time_service": get_investigation_time_service(),
        "time_scoring_service": get_time_scoring_service(),
        "training_run_service": get_training_run_service(),
        "retraining_scheduler": get_retraining_scheduler(),
        "retrieval_service": get_retrieval_service(),
    }
