# ML Training Pipeline (AML Alert Governance)

This document describes the production-style ML training and evaluation pipeline for the AML alert governance overlay (backend only).

## Required input columns

- **Time column** (one of): `alert_created_at`, `alert_date`, `created_at`, `timestamp`, `event_time`. Required for time-based split; if missing, training fails with a clear error.
- **Label / disposition**: One of `disposition`, `outcome`, `case_outcome`, `alert_outcome`, `synthetic_true_suspicious`, `is_suspicious`, `label`, `y_true`, `true_label`, `ground_truth`. Used to derive `y_sar` and `y_escalated`.
- **Features**: Numeric columns used for modeling (e.g. `amount_dev`, `velocity_dev`, `activity_dev`, or any set defined in `config/ml.yaml` → `feature_columns`). Optional time-safe features require `rule_id`, `entity_id`, and optionally `segment` (see Time-safe features).

## Label mapping

Defined in **`backend/config/ml.yaml`** under `labels`:

- **sar_values**: Disposition values that map to **y_sar = 1** (SAR/STR/confirmed suspicious or escalated+confirmed). Default includes: SAR, STR, CONFIRMED, Yes, TP, etc.
- **escalated_values**: Values that map to **y_escalated = 1** (escalated for deeper review). Default includes: ESCALATED, IN_REVIEW, PENDING, OPEN, ASSIGNED.
- **negative_values**: Optional; explicit negatives (e.g. FP, CLOSED_FP, No) map to 0.

Two-stage training (optional):

- **stage1_target**: `y_escalated` — train escalation model first.
- **stage2_target**: `y_sar` — train SAR model (optionally only on escalated subset).
- **stage2_train_on_escalated_only**: If true, stage 2 uses only rows with `y_escalated == 1`.

Label logic is implemented in **`backend/src/ml/labels.py`** (`compute_labels`, `get_label_config`).

## Time split strategy

- **No random split.** All splits are time-based.
- **Config**: `config/ml.yaml` → `time_split`:
  - **time_column** / **time_column_candidates**: Column used for ordering.
  - **validation_window_months**, **test_window_months**: Typically 1 month each.
- **Logic**:
  - **Train**: All rows with timestamp **before** the validation window.
  - **Validation**: Next `validation_window_months` month(s).
  - **Test**: Following `test_window_months` month(s) (last month of data).
- Implemented in **`backend/src/ml/split.py`** (`time_split`). If the timestamp column is missing or all NaT, the script fails with an explicit error.

## Metrics

- **PR-AUC** (primary): Precision–Recall AUC; primary for imbalanced binary classification.
- **TP retention at suppression rate**: If we suppress the bottom X% of alerts by predicted score, what fraction of true positives remain? (`tp_retention_at_suppression(y_true, y_score, suppression_rate)`).
- **Review reduction at TP retention target**: Maximum fraction of alerts that can be suppressed while keeping ≥ 98% (or configured) of TPs (`suppression_at_tp_retention(..., retention_target=0.98)`).
- **Precision in top quantile**: Precision in top 10% / 20% by score (`precision_at_k_percent(..., k=0.1)`).
- **ROC-AUC**: Optional; reported when both classes present.
- **Calibration**: Brier score and ECE (Expected Calibration Error, bin-based), computed on the **test** set after calibrating on the **validation** set.

Implemented in **`backend/src/ml/metrics.py`** and **`backend/src/ml/calibration_metrics.py`**.

## How to run training

From the **backend** directory:

```bash
# Use default config (backend/config/ml.yaml) and synthetic demo data if no --data
python scripts/train_model.py

# With your alerts CSV (must have time + label/disposition columns)
python scripts/train_model.py --data path/to/alerts.csv

# Custom config and output directory
python scripts/train_model.py --data alerts.csv --config config/ml.yaml --out-dir artifacts/models/run1

# Target: y_sar (default) or y_escalated
python scripts/train_model.py --data alerts.csv --target y_escalated
```

Or with `PYTHONPATH` set:

```bash
cd backend
set PYTHONPATH=.
python scripts/train_model.py --data data/alerts.csv
```

The script prints:

- Dataset stats and split date ranges  
- Class imbalance (pos/neg, scale_pos_weight)  
- PR-AUC, TP retention at configured suppression rates, suppression at TP retention target, precision@10% and @20%  
- Brier and ECE on test (calibrated)  
- Artifact path: **`backend/artifacts/models/<timestamp>/`**

Artifacts include:

- `model.txt` (LightGBM), `metadata.json` (feature_version, feature list, monotonic constraints, metrics)  
- `calibrator.pkl` (validation-fit calibrator for serving)

## Active learning: export / import

- **Export** alerts for human labeling (e.g. uncertain or top-impact):
  - **Uncertain**: Scores near 0.5 (or high entropy).
  - **Top-impact**: Top N by score (near top of queue).
- **Import** labeled CSV back into a standardized label table for appending to training data.

**Export** (from code or a small script):

```python
from src.ml.active_learning import export_label_batch
import pandas as pd
df_scored = pd.read_csv("scored_alerts.csv")  # must have alert_id, entity_id, rule_id, created_at, model_score
export_label_batch(df_scored, strategy="uncertain", n=200, out_path="label_batch.csv")
```

**Import** labeled results:

```python
from src.ml.active_learning import ingest_labels
labels_df = ingest_labels("label_batch_labeled.csv", disposition_column="disposition", alert_id_column="alert_id")
# labels_df has: alert_id, disposition, y_sar, y_escalated
# Append to your training dataset and re-run training.
```

Implemented in **`backend/src/ml/active_learning.py`** (`export_label_batch`, `ingest_labels`).

## Time-safe features

Features that use only “as-of time” (no future leakage):

- **rule_fatigue**: Alerts per TP by rule historically (as-of time).
- **entity_alert_velocity**: Rolling alert counts (e.g. 7d/30d) per entity (as-of time).
- **recency_weighted_outcomes**: Exponentially decayed outcome history for entity/global (as-of time).
- **peer_deviation**: Entity value vs cohort baseline (as-of time).

Implemented in **`backend/src/ml/features_time_safe.py`** (`build_time_safe_features`). Unit tests in **`backend/tests/ml/test_feature_time_safety.py`** assert no leakage (e.g. features at time t do not use t+1).

## Class imbalance

- **scale_pos_weight** = (count_neg / count_pos) for the training target; used in LightGBM.
- Config: `config/ml.yaml` → `imbalance.use_scale_pos_weight` (default true).
- Implemented in **`backend/src/ml/imbalance.py`** (`compute_scale_pos_weight`).

## Calibration

- Base model is trained on **train**.
- Calibrator (isotonic or Platt) is fit on **validation** scores only.
- Test metrics (Brier, ECE, PR-AUC, etc.) are computed on **test** using **calibrated** scores.
- Implemented in **`backend/src/ml/calibration.py`** and **`backend/src/ml/calibration_metrics.py`**.

## Model and explainability

- **LightGBM** (or XGBoost) with optional **monotonic constraints** (e.g. higher `source_risk_score` → not lower predicted yield). Mapping: `config/ml.yaml` → `model.monotonic_constraints` (feature_name → 1 / -1 / 0).
- **SHAP**: **`backend/src/ml/model.py`** provides `get_shap_values` and `top_contributing_features` for an explain payload compatible with the existing explain layer.

## Tests

From **backend**:

```bash
pytest tests/ml/ -v
```

- **test_time_split_no_leakage**: Train/val/test are time-ordered and disjoint; missing or all-NaT time column raises.
- **test_tp_retention_metrics**: TP retention and suppression-at-retention behave as specified.
- **test_calibration_on_val_only**: Calibrator fit on val and apply on scores.
- **test_feature_time_safety**: Rule fatigue and entity velocity use only past data (no t+1).

## Deterministic runs

- **random_seed** in **`config/ml.yaml`** (default 42) is used for model training and any sampling. Set it for reproducible runs.
