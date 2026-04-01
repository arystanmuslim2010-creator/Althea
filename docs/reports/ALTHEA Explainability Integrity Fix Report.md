# ALTHEA Explainability Integrity Fix Report

## 1. Files Modified
- `backend/models/explainability_service.py` (new shared runtime+governance explainability engine)
- `backend/model_governance/explainability.py` (governance adapter now delegates to shared engine)
- `backend/models/inference_service.py` (runtime inference now passes tenant/alert/schema context into shared explanation engine)
- `backend/events/streaming/consumers.py` (stream scoring now persists explanation metadata and aligned payloads)
- `backend/services/pipeline_service.py` (persisted explanation warning code in risk/ml signal payloads)
- `backend/services/governance_service.py` (governance explainability call uses shared metadata contract with tenant/alert context)
- `backend/services/explain_service.py` (normalized explainability payloads for `/api/alerts/{id}/explain`)
- `backend/api/routers/alerts_router.py` (additive top-level explanation metadata in alert API responses)
- `backend/investigation/risk_explanation_service.py` (propagates explanation method/status/warnings)
- `backend/investigation/investigation_summary_service.py` (contribution extraction supports fallback values)
- `backend/investigation/sar_generator.py` (contribution extraction supports fallback values)
- `backend/core/observability.py` (new explainability latency/failure/method/fallback metrics)
- `backend/core/dependencies.py` (governance explainability now bound to shared explainability engine)
- `backend/tests/test_explainability_integrity.py` (new backend explainability integrity tests)
- `frontend/src/services/contracts.js` (new `normalizeExplanationPayload`)
- `frontend/src/services/contracts.test.js` (normalization tests incl. fallback + old payloads)
- `frontend/src/pages/AlertQueue.jsx` (safe explanation labeling/disclaimer rendering)
- `frontend/src/pages/AlertDetails.jsx` (safe explanation labeling/disclaimer rendering)
- `frontend/src/pages/AlertDetails.test.jsx` (UI tests for SHAP/fallback/old payload safety)
- `docs/explainability.md` (updated integrity documentation)

## 2. Old vs New Explainability Path
### Old
- Runtime path could expose feature lists that were effectively numeric-magnitude heuristics.
- Governance explainability lived in a separate implementation and could silently fallback without explicit provenance metadata.
- Stream scoring path persisted scores but not aligned explanation metadata.

### New
- One shared explainability engine: `backend/models/explainability_service.py`.
- Runtime inference and governance both call that shared engine.
- Real model attribution is labeled `shap` / `tree_shap` only when produced from model explainer output.
- Any heuristic/unavailable result is explicitly labeled with:
  - `explanation_method`
  - `explanation_status`
  - `explanation_warning`
  - `explanation_warning_code`
- Streaming scoring now persists aligned explanation payloads (`risk_explain_json`, `top_feature_contributions_json`, `top_features_json`, `ml_signals_json`) with metadata.

## 3. Runtime Routes / Services Affected
- `POST /internal/ml/predict` (via `InferenceService.predict`)
- `GET /api/alerts` and `GET /api/alerts/{alert_id}` (additive explanation metadata fields)
- `GET /api/alerts/{alert_id}/explain` (normalized explanation metadata surfaced)
- `GET /api/alerts/{alert_id}/risk-explanation` (method/status/warnings propagated through investigation risk explanation output)
- `GET /api/alerts/{alert_id}/investigation-context` (now carries explanation metadata through integrated services)
- Pipeline and streaming services:
  - `PipelineService._run_pipeline`
  - `ModelScoringConsumer.process`
  - `GovernanceService.prioritize_alerts`

## 4. Frontend Components Updated
- `frontend/src/pages/AlertQueue.jsx`
  - explanation display text no longer implies SHAP for fallback.
  - fallback disclaimer shown for `numeric_fallback` / unavailable fallback conditions.
  - SHAP badge explicitly labeled model attribution.
- `frontend/src/pages/AlertDetails.jsx`
  - method/status rendering added.
  - fallback disclaimer added.
  - SHAP model-attribution message shown only when method indicates SHAP.
- `frontend/src/services/contracts.js`
  - added `normalizeExplanationPayload(...)` with compatibility defaults for older records.

## 5. Fallback Behavior Contract
Explanation payload now explicitly supports:

```json
{
  "feature_attribution": [...],
  "risk_reason_codes": [...],
  "explanation_method": "shap | tree_shap | numeric_fallback | unavailable",
  "explanation_status": "ok | fallback | unavailable",
  "explanation_warning": "string|null",
  "explanation_warning_code": "string|null"
}
```

Implemented warning codes:
- `shap_not_installed`
- `unsupported_model`
- `feature_frame_incompatible`
- `explainer_runtime_error`
- `model_artifact_unavailable`
- `no_feature_data`
- `no_numeric_features`

No fallback output is labeled as SHAP.

## 6. Test Results
### Backend
Command:
- `pytest backend/tests`

Result:
- `31 passed`

### Frontend
Commands:
- `npm --prefix frontend test -- --run`
- `npm --prefix frontend run build`

Results:
- `18 passed` (Vitest)
- production build succeeded (Vite)

## 7. Remaining Limitations
- SHAP availability still depends on runtime environment/library installation and model compatibility.
- Unsupported model classes intentionally fall back with explicit metadata.
- Fallback heuristic remains numeric-feature-based guidance; it is clearly labeled non-attribution.
- Full browser-click manual UX walkthrough was validated through component tests/build, not an interactive UI session in this run.

## Manual Verification Checklist Outcome
Validated in this change set via tests/build:
- Alert details page still loads (`AlertDetails` tests pass).
- Explanation payload renders (`AlertDetails` tests pass).
- Fallback disclaimer appears for `numeric_fallback` (`AlertDetails` tests pass).
- Frontend/backend integration contracts remain intact (`backend/tests/test_frontend_backend_contracts.py` pass).
