# Explainability and Decision Logging

## Overview
The ALTHEA scoring pipeline now produces model explainability and audit logs for every alert scored.

## SHAP explainability
- The scorer uses `shap.TreeExplainer(model, feature_perturbation="tree_path_dependent")`.
- SHAP values are computed during scoring and converted into top feature contributions per alert.
- Output fields include:
  - `top_feature_contributions_json`: ordered list of feature/impact pairs.
  - `top_features_json`: ordered list of top feature names.
- Example contribution item:
  - `{"feature": "transaction_amount", "impact": 0.24}`

## Decision logging
- A governance logger writes one JSON object per alert to:
  - `logs/decision_logs.jsonl`
- Each log entry includes:
  - `alert_id`
  - `timestamp`
  - `model_version`
  - `features_used`
  - `score`
  - `priority`
  - `top_features`
  - `governance_rules_triggered`
- If governance suppresses or deprioritizes an alert, the same record includes:
  - `decision` (`suppressed` or `deprioritized`)
  - `reason` (for example: `low_expected_investigative_yield`)

## Governance explainability
- Suppression and governance context is captured from:
  - `governance_status`
  - `suppression_code`
  - `suppression_reason`
  - hard-constraint metadata
- This metadata is also included in the decision log under `governance_rules_triggered`.

## API interpretation for analysts
- Alert responses now expose:
  - `score`
  - `priority`
  - `top_features`
  - `model_version`
- Analysts should interpret `top_features` as the strongest local drivers of the model score for that specific alert.
- Positive impact means the feature pushed risk up; negative impact means it pushed risk down.

## Performance controls
- SHAP can be constrained with config:
  - `SHAP_TOP_FEATURES` (default: 5)
  - `SHAP_MAX_ALERTS` (default: 0 = all alerts; set > 0 to compute only top-N alerts by score)
- For alerts outside SHAP scope (when capped), fallback contributions are provided from risk components.
