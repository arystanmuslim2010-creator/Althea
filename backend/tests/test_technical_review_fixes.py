from __future__ import annotations

import io

import numpy as np
import pandas as pd
from joblib import dump as joblib_dump
from sklearn.dummy import DummyClassifier

from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from services.feature_service import EnterpriseFeatureService
from services.governance_service import GovernanceService


def _sample_df(n: int = 120, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    return pd.DataFrame(
        {
            "alert_id": [f"ALT{idx+1:06d}" for idx in range(n)],
            "user_id": [f"USR{idx % 25:05d}" for idx in range(n)],
            "amount": rng.lognormal(mean=8.5, sigma=0.8, size=n),
            "segment": rng.choice(["retail", "corporate", "private_banking"], size=n),
            "country": rng.choice(["US", "GB", "DE", "AE", "RU", "IR"], size=n),
            "typology": rng.choice(["cross_border", "structuring", "sanctions"], size=n),
            "source_system": rng.choice(["core_bank", "cards"], size=n),
            "timestamp": [base + pd.Timedelta(minutes=int(v)) for v in rng.integers(1, 250000, size=n)],
            "time_gap": rng.uniform(60, 86400, size=n),
            "num_transactions": rng.integers(1, 20, size=n),
        }
    )


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
            "training_metadata_json": {"feature_schema_version": "v1", "artifact_format": "joblib"},
        }

    def load_feature_schema(self, model_record: dict):
        return self._schema

    def load_model_artifact(self, model_record: dict):
        return self._artifact_bytes


def test_feature_generation_uses_single_builder_for_training_and_inference():
    service = EnterpriseFeatureService(FeatureSchemaValidator())
    df = _sample_df(100)
    train = service.generate_training_features(df)
    infer = service.generate_inference_features(df)

    assert list(train["feature_matrix"].columns) == list(infer["feature_matrix"].columns)
    assert len(train["feature_matrix"]) == len(df)
    assert len(infer["feature_matrix"]) == len(df)
    assert train["feature_schema"]["schema_hash"] == infer["feature_schema"]["schema_hash"]


def test_feature_builder_produces_stable_numeric_matrix():
    service = EnterpriseFeatureService(FeatureSchemaValidator())
    df = _sample_df(80)
    out = service.generate_inference_features(df)
    matrix = out["feature_matrix"]
    assert not matrix.empty
    assert matrix.select_dtypes(include=["number"]).shape[1] == matrix.shape[1]
    assert np.isfinite(matrix.to_numpy(dtype=float)).all()


def test_inference_service_outputs_scores_in_valid_range():
    service = EnterpriseFeatureService(FeatureSchemaValidator())
    bundle = service.generate_inference_features(_sample_df(90))
    x = bundle["feature_matrix"]
    y = np.random.default_rng(19).integers(0, 2, size=len(x))
    model = DummyClassifier(strategy="prior")
    model.fit(x, y)
    buffer = io.BytesIO()
    joblib_dump(model, buffer)
    registry = _StubRegistry(schema=bundle["feature_schema"], artifact_bytes=buffer.getvalue())

    inference = InferenceService(registry=registry, schema_validator=FeatureSchemaValidator())
    result = inference.predict(tenant_id="default-bank", feature_frame=bundle["feature_matrix"])

    scores = np.asarray(result["scores"], dtype=float)
    assert len(scores) == len(bundle["feature_matrix"])
    assert (scores >= 0).all()
    assert (scores <= 100).all()


def test_governance_assigns_queue_status_consistently():
    gov = GovernanceService()
    df = pd.DataFrame(
        {
            "alert_id": ["A1", "A2", "A3", "A4"],
            "user_id": ["U1", "U2", "U3", "U4"],
            "risk_score": [25.0, 60.0, 80.0, 95.0],
        }
    )
    out = gov.apply_governance(df)
    status = dict(zip(out["alert_id"], out["governance_status"]))
    assert status["A1"] == "suppressed"
    assert status["A2"] == "eligible"
    assert status["A3"] == "mandatory_review"
    assert status["A4"] == "mandatory_review"
    assert out.loc[out["alert_id"] == "A1", "in_queue"].iloc[0] is False
    assert out.loc[out["alert_id"] == "A4", "alert_priority"].iloc[0] == "P0"
