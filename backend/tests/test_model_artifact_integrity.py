from __future__ import annotations

import hashlib

import pytest

from models.inference_service import verify_model_artifact_integrity


def test_model_artifact_integrity_matching_hash_passes():
    artifact = b"model-bytes"
    expected = hashlib.sha256(artifact).hexdigest()
    assert verify_model_artifact_integrity(artifact, expected) is True


def test_model_artifact_integrity_mismatch_fails_before_load():
    with pytest.raises(ValueError, match="integrity"):
        verify_model_artifact_integrity(b"model-bytes", "0" * 64)
