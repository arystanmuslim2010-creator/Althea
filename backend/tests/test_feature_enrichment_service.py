from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from services.enrichment_repository import EnrichmentRepository
from events.streaming.consumers import ConsumerContext, FeatureServiceConsumer
from features.builders.base import BuilderContext
from services.feature_enrichment_service import FeatureEnrichmentService
from services.pipeline_service import PipelineService
from storage.postgres_repository import EnterpriseRepository


class _StubRepo:
    def __init__(self) -> None:
        self.pipeline_runs = [{"run_id": "run-history-1"}]
        self.payloads = [
            {
                "alert_id": "H1",
                "user_id": "U1",
                "amount": 1250.0,
                "num_transactions": 2,
                "segment": "retail",
                "country": "US",
                "typology": "structuring",
                "timestamp": "2026-01-01T00:00:00Z",
                "transactions": [
                    {
                        "transaction_id": "TX-1",
                        "amount": 700.0,
                        "timestamp": "2025-12-31T23:50:00Z",
                        "sender": "U1",
                        "receiver": "CP-1",
                    },
                    {
                        "transaction_id": "TX-2",
                        "amount": 550.0,
                        "timestamp": "2025-12-31T23:58:00Z",
                        "sender": "U1",
                        "receiver": "CP-2",
                    },
                ],
            }
        ]
        self.cases = [
            {
                "case_id": "CASE-1",
                "alert_id": "H1",
                "status": "sar_filed",
                "created_at": "2026-01-01T01:00:00Z",
                "updated_at": "2026-01-02T01:00:00Z",
                "touch_count": 4,
            }
        ]

    def list_pipeline_runs(self, tenant_id: str, limit: int = 5) -> list[dict]:
        return list(self.pipeline_runs)[:limit]

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 5000) -> list[dict]:
        return list(self.payloads)[:limit]

    def list_cases(self, tenant_id: str) -> list[dict]:
        return list(self.cases)

    def save_decision_audit_records(self, tenant_id: str, records: list[dict]) -> int:
        return len(records)

    def store_feature_rows(
        self,
        tenant_id: str,
        run_id: str,
        feature_schema_hash: str,
        feature_rows: list[dict],
    ) -> int:
        return len(feature_rows)


class _StubGraphFeatureService:
    def extract_features_for_batch(self, alerts_df: pd.DataFrame, context=None) -> pd.DataFrame:
        rows = []
        for _, row in alerts_df.iterrows():
            rows.append(
                {
                    "alert_id": str(row.get("alert_id") or ""),
                    "linked_entity_count": 3.0,
                    "graph_complexity_proxy": 0.6,
                }
            )
        return pd.DataFrame(rows)


class _CapturingFeatureService:
    def __init__(self) -> None:
        self.context = None

    def generate_features_batch(self, df: pd.DataFrame, context=None) -> dict:
        self.context = context
        alerts_df = df.copy()
        feature_matrix = pd.DataFrame({"feature_amount": pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)})
        return {
            "alerts_df": alerts_df,
            "feature_matrix": feature_matrix,
            "feature_schema": {"schema_hash": "test"},
            "feature_groups": {"alert": ["feature_amount"]},
        }

    def generate_inference_features(self, df: pd.DataFrame, context=None) -> dict:
        return self.generate_features_batch(df, context=context)


def _sample_alert_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "alert_id": ["A1"],
            "user_id": ["U1"],
            "amount": [9000.0],
            "segment": ["retail"],
            "country": ["US"],
            "typology": ["structuring"],
            "timestamp": [pd.Timestamp("2026-01-03T00:00:00Z")],
            "num_transactions": [3],
        }
    )


def test_feature_enrichment_service_builds_context_frames() -> None:
    service = FeatureEnrichmentService(repository=_StubRepo(), graph_feature_service=_StubGraphFeatureService())
    context = service.build_context(
        tenant_id="tenant-a",
        alerts_df=_sample_alert_frame(),
        run_id="run-current",
    )

    assert isinstance(context, BuilderContext)
    assert context.transaction_history is not None and not context.transaction_history.empty
    assert context.outcome_history is not None and not context.outcome_history.empty
    assert context.peer_stats is not None and not context.peer_stats.empty
    assert context.case_history is not None and not context.case_history.empty
    assert context.graph_features is not None and not context.graph_features.empty
    assert getattr(context, "enrichment_stats").transaction_history_rows >= 1


def test_pipeline_service_passes_enrichment_context_to_feature_generation(monkeypatch) -> None:
    feature_service = _CapturingFeatureService()
    pipeline = PipelineService(
        settings=SimpleNamespace(pipeline_batch_size=1000),  # type: ignore[arg-type]
        repository=_StubRepo(),  # type: ignore[arg-type]
        event_bus=SimpleNamespace(),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(),  # type: ignore[arg-type]
        ingestion_service=SimpleNamespace(),  # type: ignore[arg-type]
        feature_service=feature_service,  # type: ignore[arg-type]
        inference_service=SimpleNamespace(
            predict=lambda **kwargs: {
                "model_version": "model-v1",
                "scores": [77.0],
                "explanations": [
                    {
                        "feature_attribution": [{"feature": "feature_amount", "value": 9000.0}],
                        "risk_reason_codes": ["amount:increase"],
                        "explanation_method": "numeric_fallback",
                        "explanation_status": "fallback",
                        "explanation_warning": "fallback",
                        "explanation_warning_code": "model_artifact_unavailable",
                    }
                ],
            }
        ),  # type: ignore[arg-type]
        governance_service=SimpleNamespace(
            apply_governance=lambda alerts_df, stabilize_for_demo=False: alerts_df.assign(
                priority_score=alerts_df["risk_score"],
                governance_status="eligible",
                queue_action="review",
                alert_priority="P1",
            )
        ),  # type: ignore[arg-type]
        model_monitoring_service=SimpleNamespace(),  # type: ignore[arg-type]
        feature_enrichment_service=FeatureEnrichmentService(
            repository=_StubRepo(),
            graph_feature_service=_StubGraphFeatureService(),
        ),  # type: ignore[arg-type]
        feature_adapter=SimpleNamespace(),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(pipeline, "_build_decision_audit_records", lambda **kwargs: [])
    monkeypatch.setattr(pipeline, "_persist_outputs", lambda **kwargs: 1)

    pipeline._run_pipeline(_sample_alert_frame(), tenant_id="tenant-a", run_id="run-current")

    assert isinstance(feature_service.context, BuilderContext)
    assert feature_service.context.transaction_history is not None
    assert not feature_service.context.transaction_history.empty


def test_feature_service_consumer_passes_enrichment_context() -> None:
    feature_service = _CapturingFeatureService()
    consumer = FeatureServiceConsumer(
        stream=SimpleNamespace(),
        repository=_StubRepo(),
        feature_service=feature_service,
        online_feature_store=SimpleNamespace(put_features=lambda **kwargs: None),
        feature_enrichment_service=FeatureEnrichmentService(
            repository=_StubRepo(),
            graph_feature_service=_StubGraphFeatureService(),
        ),
    )
    message_context = ConsumerContext(
        tenant_id="tenant-a",
        correlation_id="corr-1",
        payload={"run_id": "run-history-1", "alert_ids": ["H1"]},
    )

    out = consumer.process(message_context)

    assert out is not None
    assert isinstance(feature_service.context, BuilderContext)
    assert feature_service.context.transaction_history is not None
    assert not feature_service.context.transaction_history.empty


def test_feature_enrichment_service_prefers_canonical_enrichment_tables(tmp_path) -> None:
    db_path = tmp_path / "enrichment_feature.db"
    repository = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    enrichment_repository = EnrichmentRepository(repository)
    tenant_id = "tenant-a"

    enrichment_repository.append_account_events(
        tenant_id,
        [
            {
                "source_name": "internal_case",
                "source_record_id": "evt-1",
                "entity_id": "U1",
                "account_id": "AC1",
                "counterparty_id": "CP1",
                "amount": 4200.0,
                "country": "US",
                "direction": "debit",
                "event_time": "2026-01-02T00:00:00Z",
            }
        ],
    )
    enrichment_repository.append_alert_outcomes(
        tenant_id,
        [
            {
                "source_name": "internal_outcome",
                "alert_id": "A1",
                "entity_id": "U1",
                "decision": "sar_filed",
                "event_time": "2026-01-02T01:00:00Z",
            }
        ],
    )
    enrichment_repository.append_case_actions(
        tenant_id,
        [
            {
                "source_name": "internal_case",
                "case_id": "CASE-1",
                "alert_id": "A1",
                "entity_id": "U1",
                "action": "case_created",
                "event_time": "2026-01-02T01:30:00Z",
            }
        ],
    )

    service = FeatureEnrichmentService(
        repository=repository,
        enrichment_repository=enrichment_repository,
        graph_feature_service=_StubGraphFeatureService(),
    )
    context = service.build_context(
        tenant_id=tenant_id,
        alerts_df=_sample_alert_frame(),
        run_id="run-current",
    )

    assert isinstance(context, BuilderContext)
    assert not context.transaction_history.empty
    assert float(context.transaction_history["amount"].iloc[0]) == 4200.0
    assert not context.outcome_history.empty
    assert not context.case_history.empty
