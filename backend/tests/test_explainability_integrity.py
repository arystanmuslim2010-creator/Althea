from __future__ import annotations

import io
import json

import numpy as np
import pandas as pd
from joblib import dump as joblib_dump
from sklearn.dummy import DummyClassifier

from model_governance.explainability import GovernanceExplainabilityService
from models.explainability_service import (
    ExplainabilityService,
    ExplanationMethod,
    ExplanationStatus,
)
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from services.governance_service import GovernanceService


class _StubRegistry:
    def __init__(self, schema: dict, artifact_bytes: bytes):
        self._schema = schema
        self._artifact_bytes = artifact_bytes

    def resolve_model(self, tenant_id: str, strategy: str = "approved_latest"):
        return {
            "id": "test-model-id",
            "tenant_id": tenant_id,
            "model_version": "test-model-v1",
            "artifact_uri": f"models/{tenant_id}/test-model-v1/model.joblib",
            "training_metadata_json": {"feature_schema_version": "v2", "artifact_format": "joblib"},
        }

    def load_feature_schema(self, model_record: dict):
        return self._schema

    def load_model_artifact(self, model_record: dict):
        return self._artifact_bytes


class _RecordingExplainability:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def generate_explanation(self, **kwargs):
        self.calls.append(dict(kwargs))
        return dict(self.payload)


class _GovernanceRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def generate_explanation(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {
            "feature_attribution": [{"feature": "amount", "value": 12.0, "shap_value": None}],
            "risk_reason_codes": ["amount:increase"],
            "explanation_method": "numeric_fallback",
            "explanation_status": "fallback",
            "explanation_warning": "Heuristic feature highlights; not model contribution attribution.",
            "explanation_warning_code": "model_artifact_unavailable",
            "model_version": str(kwargs.get("model_version") or "unknown"),
        }

    def merge_into_alert_metadata(self, alert_payload: dict[str, object], explanation: dict[str, object]) -> dict[str, object]:
        out = dict(alert_payload)
        out["risk_explain_json"] = json.dumps(explanation, ensure_ascii=True)
        out["top_feature_contributions_json"] = json.dumps(explanation.get("feature_attribution", []), ensure_ascii=True)
        out["top_features_json"] = json.dumps(["amount"], ensure_ascii=True)
        return out


class _GovernanceRepo:
    def __init__(self, payloads: list[dict[str, object]]):
        self._payloads = [dict(item) for item in payloads]
        self.saved_records: list[dict[str, object]] = []

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 500000):
        return [dict(item) for item in self._payloads]

    def list_pipeline_runs(self, tenant_id: str, limit: int = 500):
        return [{"run_id": "run-1", "source": "synthetic"}]

    def save_alert_payloads(self, tenant_id: str, run_id: str, records: list[dict[str, object]]):
        self.saved_records = [dict(item) for item in records]
        return len(records)


def _feature_frame(rows: int = 3, seed: int = 11) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "amount": rng.normal(1000.0, 50.0, size=rows),
            "time_gap": rng.uniform(10.0, 50.0, size=rows),
            "num_transactions": rng.integers(1, 6, size=rows),
        }
    )


def _build_registry(feature_frame: pd.DataFrame) -> _StubRegistry:
    y = np.array([0, 1, 0] * 10)[: len(feature_frame)]
    model = DummyClassifier(strategy="prior")
    model.fit(feature_frame, y)
    buffer = io.BytesIO()
    joblib_dump(model, buffer)
    schema = FeatureSchemaValidator().from_frame(feature_frame)
    return _StubRegistry(schema=schema, artifact_bytes=buffer.getvalue())


def test_runtime_inference_returns_model_based_explanation_with_context():
    frame = _feature_frame(rows=2)
    registry = _build_registry(frame)
    recorder = _RecordingExplainability(
        {
            "feature_attribution": [{"feature": "amount", "value": 0.4, "shap_value": 0.4}],
            "risk_reason_codes": ["amount:increase"],
            "explanation_method": "shap",
            "explanation_status": "ok",
            "explanation_warning": None,
            "explanation_warning_code": None,
            "model_version": "test-model-v1",
        }
    )
    service = InferenceService(
        registry=registry,
        schema_validator=FeatureSchemaValidator(),
        explainability_service=recorder,  # type: ignore[arg-type]
    )
    result = service.predict(
        tenant_id="tenant-a",
        feature_frame=frame,
        alert_ids=["A1", "A2"],
    )

    assert len(result["explanations"]) == 2
    assert all(item.get("explanation_method") == "shap" for item in result["explanations"])
    assert recorder.calls[0]["tenant_id"] == "tenant-a"
    assert recorder.calls[0]["alert_id"] == "A1"
    assert recorder.calls[0]["feature_schema_version"] == "v2"


def test_runtime_inference_preserves_row_specific_explanations(monkeypatch):
    frame = pd.DataFrame(
        {
            "amount": [125.0, 250000.0],
            "time_gap": [7200.0, 15.0],
            "num_transactions": [1.0, 18.0],
        }
    )
    registry = _build_registry(frame)
    explainability = ExplainabilityService()
    monkeypatch.setattr(explainability, "_get_shap", lambda: None)
    service = InferenceService(
        registry=registry,
        schema_validator=FeatureSchemaValidator(),
        explainability_service=explainability,
    )

    result = service.predict(
        tenant_id="tenant-row-specific",
        feature_frame=frame,
        alert_ids=["A-row-1", "A-row-2"],
    )

    first, second = result["explanations"]
    assert first["explanation_method"] == "numeric_fallback"
    assert second["explanation_method"] == "numeric_fallback"
    assert first["feature_attribution"] != second["feature_attribution"]
    assert first["risk_reason_codes"] != second["risk_reason_codes"]


def test_row_specific_explanations_merge_into_distinct_alert_payloads(monkeypatch):
    frame = pd.DataFrame(
        {
            "amount": [250.0, 98000.0],
            "time_gap": [3600.0, 5.0],
            "num_transactions": [2.0, 21.0],
        }
    )
    service = ExplainabilityService()
    monkeypatch.setattr(service, "_get_shap", lambda: None)

    first = service.generate_explanation(model=None, feature_frame=frame.iloc[[0]], model_version="v-test")
    second = service.generate_explanation(model=None, feature_frame=frame.iloc[[1]], model_version="v-test")
    payload_a = service.merge_into_alert_metadata({"alert_id": "A-1"}, first)
    payload_b = service.merge_into_alert_metadata({"alert_id": "A-2"}, second)

    assert payload_a["risk_explain_json"] != payload_b["risk_explain_json"]
    assert payload_a["top_feature_contributions_json"] != payload_b["top_feature_contributions_json"]


def test_runtime_inference_fallback_is_explicit_and_warning_present():
    frame = _feature_frame(rows=2)
    registry = _build_registry(frame)
    recorder = _RecordingExplainability(
        {
            "feature_attribution": [{"feature": "amount", "value": 1200.0, "shap_value": None}],
            "risk_reason_codes": ["amount:increase"],
            "explanation_method": "numeric_fallback",
            "explanation_status": "fallback",
            "explanation_warning": "Heuristic feature highlights; not model contribution attribution.",
            "explanation_warning_code": "shap_not_installed",
            "model_version": "test-model-v1",
        }
    )
    service = InferenceService(
        registry=registry,
        schema_validator=FeatureSchemaValidator(),
        explainability_service=recorder,  # type: ignore[arg-type]
    )
    result = service.predict(tenant_id="tenant-b", feature_frame=frame)
    explanation = result["explanations"][0]

    assert explanation["explanation_method"] == "numeric_fallback"
    assert explanation["explanation_status"] == "fallback"
    assert "heuristic" in str(explanation["explanation_warning"]).lower()
    assert explanation["explanation_warning_code"] == "shap_not_installed"


def test_main_inference_path_never_returns_unlabeled_explanations():
    frame = _feature_frame(rows=2)
    registry = _build_registry(frame)
    recorder = _RecordingExplainability(
        {
            "feature_attribution": [{"feature": "amount", "value": 1.0, "shap_value": None}],
            "risk_reason_codes": ["amount:increase"],
            "explanation_method": "numeric_fallback",
            "explanation_status": "fallback",
            "explanation_warning": "fallback",
            "explanation_warning_code": "unsupported_model",
            "model_version": "test-model-v1",
        }
    )
    service = InferenceService(
        registry=registry,
        schema_validator=FeatureSchemaValidator(),
        explainability_service=recorder,  # type: ignore[arg-type]
    )
    result = service.predict(tenant_id="tenant-c", feature_frame=frame)

    assert result["explanations"]
    for item in result["explanations"]:
        assert "explanation_method" in item
        assert "explanation_status" in item
        assert item["explanation_method"] in {"shap", "tree_shap", "numeric_fallback", "unavailable"}


def test_inference_schema_alignment_imputes_missing_and_drops_extra_columns():
    train_frame = _feature_frame(rows=8)
    train_frame["user_amount_mean"] = train_frame["amount"] * 0.9
    registry = _build_registry(train_frame)
    recorder = _RecordingExplainability(
        {
            "feature_attribution": [{"feature": "amount", "value": 0.2, "shap_value": 0.2}],
            "risk_reason_codes": ["amount:increase"],
            "explanation_method": "shap",
            "explanation_status": "ok",
            "explanation_warning": None,
            "explanation_warning_code": None,
            "model_version": "test-model-v1",
        }
    )
    service = InferenceService(
        registry=registry,
        schema_validator=FeatureSchemaValidator(),
        explainability_service=recorder,  # type: ignore[arg-type]
    )

    infer_frame = train_frame.drop(columns=["user_amount_mean"]).iloc[:2].copy()
    infer_frame["debug_only_extra"] = 1
    result = service.predict(tenant_id="tenant-d", feature_frame=infer_frame)

    assert len(result["scores"]) == 2
    schema_validation = result.get("schema_validation") or {}
    assert schema_validation.get("is_valid") is True
    assert "user_amount_mean" in (schema_validation.get("imputed_columns") or [])
    assert "debug_only_extra" in (schema_validation.get("dropped_columns") or [])


def test_governance_and_runtime_explainability_share_same_engine():
    shared = ExplainabilityService()
    governance = GovernanceExplainabilityService(explainability_service=shared)

    assert governance._service is shared  # explicit adapter over the runtime engine


def test_governance_prioritization_passes_tenant_context_to_explainer():
    repo = _GovernanceRepo(
        [
            {
                "alert_id": "A1",
                "risk_score": 65.0,
                "governance_status": "eligible",
                "top_feature_contributions_json": "[]",
                "top_features_json": "[]",
                "risk_explain_json": "{}",
                "rules_json": "[]",
                "rule_evidence_json": "{}",
            }
        ]
    )
    explain = _GovernanceRecorder()
    service = GovernanceService(repository=repo, explainability_service=explain)
    updates = service.prioritize_alerts(
        tenant_id="tenant-z",
        run_id="run-1",
        alert_ids=["A1"],
        model_version="model-v9",
    )

    assert updates
    assert explain.calls
    assert explain.calls[0]["tenant_id"] == "tenant-z"
    assert explain.calls[0]["alert_id"] == "A1"


def test_shared_explainability_fallback_when_shap_is_unavailable(monkeypatch):
    frame = _feature_frame(rows=1)
    model = DummyClassifier(strategy="prior").fit(frame, [0])
    service = ExplainabilityService()
    monkeypatch.setattr(service, "_get_shap", lambda: None)

    result = service.generate_explanation(model=model, feature_frame=frame, model_version="v1")

    assert result["explanation_method"] == ExplanationMethod.NUMERIC_FALLBACK.value
    assert result["explanation_status"] == ExplanationStatus.FALLBACK.value
    assert result["explanation_warning_code"] == "shap_not_installed"


def test_shared_explainability_marks_unsupported_models_as_fallback():
    class _CustomModel:
        __module__ = "custom_models"

    service = ExplainabilityService()
    frame = _feature_frame(rows=1)
    result = service.generate_explanation(model=_CustomModel(), feature_frame=frame, model_version="v2")

    assert result["explanation_method"] == "numeric_fallback"
    assert result["explanation_status"] == "fallback"
    assert result["explanation_warning_code"] == "unsupported_model"


def test_shared_explainability_labels_feature_frame_incompatibility(monkeypatch):
    class _FakeShap:
        class Explainer:
            def __init__(self, model, frame):
                pass

            def __call__(self, frame):
                raise ValueError("Feature shape mismatch for explainer")

    frame = _feature_frame(rows=1)
    model = DummyClassifier(strategy="prior").fit(frame, [1])
    service = ExplainabilityService()
    monkeypatch.setattr(service, "_get_shap", lambda: _FakeShap)

    result = service.generate_explanation(model=model, feature_frame=frame, model_version="v3")

    assert result["explanation_method"] == "numeric_fallback"
    assert result["explanation_status"] == "fallback"
    assert result["explanation_warning_code"] == "feature_frame_incompatible"


def test_shared_explainability_marks_model_unavailable_explicitly():
    frame = _feature_frame(rows=1)
    service = ExplainabilityService()

    result = service.generate_explanation(model=None, feature_frame=frame, model_version="v4")

    assert result["explanation_method"] == "numeric_fallback"
    assert result["explanation_status"] == "fallback"
    assert result["explanation_warning_code"] == "model_artifact_unavailable"
