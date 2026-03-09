# ML Training and Serving (Enterprise)

This repository now uses the modular backend architecture (`backend/services`, `backend/models`, `backend/storage`) and no longer uses `backend/src`.

## Current serving path

- Inference is executed by `backend/models/inference_service.py`.
- Model metadata is resolved from `backend/models/model_registry.py`.
- Artifacts are loaded from `backend/storage/object_storage.py`.
- Pipeline scoring is executed only through `backend/services/pipeline_service.py`.

## Supported model artifact types

- scikit-learn estimators and pipelines (joblib/pickle artifacts)
- LightGBM models
- XGBoost models

## Feature schema enforcement

Inference validates incoming features against the registered feature schema before scoring.

## Training/inference parity

Feature generation for training and inference uses a shared builder in `backend/services/feature_service.py`.
