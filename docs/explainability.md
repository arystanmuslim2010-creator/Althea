# Explainability Integrity

## Core Principle
- Numeric input magnitude is **not** model attribution.
- ALTHEA only labels an explanation as `shap` or `tree_shap` when attribution is generated from the actual scored model.
- Any heuristic output is explicitly marked fallback.

## Unified Architecture
- Runtime inference and governance enrichment use one shared engine:
  - `backend/models/explainability_service.py`
- Governance adapter:
  - `backend/model_governance/explainability.py`
  - delegates to the same shared engine for backward compatibility.

## Explanation Contract
Each explanation payload includes:
- `feature_attribution`
- `risk_reason_codes`
- `explanation_method` (`shap`, `tree_shap`, `numeric_fallback`, `unavailable`)
- `explanation_status` (`ok`, `fallback`, `unavailable`)
- `explanation_warning`
- `explanation_warning_code`

Backward-compatible fields remain:
- `top_feature_contributions_json`
- `top_features_json`
- `risk_explain_json`
- `ml_service_explain_json`

## Fallback Behavior
Scoring continues where possible, and explanations are labeled honestly.

Common fallback warning codes:
- `shap_not_installed`
- `unsupported_model`
- `feature_frame_incompatible`
- `explainer_runtime_error`
- `model_artifact_unavailable`
- `no_feature_data`
- `no_numeric_features`

## UI Guidance
- For `shap` / `tree_shap`: show as model attribution.
- For `numeric_fallback` / `unavailable`: show disclaimer:
  - “Heuristic feature highlights; not model contribution attribution.”
- Frontend normalization handles older records missing metadata by defaulting method/status to `unknown`.

## Observability
Explainability emits metrics and structured logs:
- `althea_explanation_generation_latency_seconds`
- `althea_explanation_generation_failures_total`
- `althea_explanation_method_count_total`
- `althea_explanation_fallback_count_total`

Structured logs include:
- `tenant_id`
- `alert_id` (when available)
- `model_version`
- `feature_schema_version`
- `explanation_method`
- `explanation_status`
