# ALTHEA - AML Alert Prioritization Platform

## Project Overview

ALTHEA is a B2B enterprise platform for banks and financial institutions. It sits between existing AML detection systems and human compliance analysts, prioritizing which alerts are most likely to lead to a SAR (Suspicious Activity Report) filing. The goal is to reduce manual review volume while ensuring SAR-worthy cases always reach analysts.

**Key principle:** ALTHEA does not detect crime. It optimizes the order in which humans investigate alerts that existing detection systems have already flagged.

---

## Architecture

```
AML Detection System -> [ALTHEA] -> Analyst Queue
                           |
                    ingest -> normalize -> enrich -> rules -> score -> governance -> explain -> persist -> metrics
```

### Backend (`backend/`)
- **Language:** Python 3.11+
- **Framework:** FastAPI with uvicorn
- **ML Stack:** pandas, numpy, scikit-learn, LightGBM, SHAP
- **Storage:** SQLite (development) / PostgreSQL (production)
- **Config:** `backend/src/config.py` - single source of truth for all constants

### Frontend (`frontend/`)
- **Framework:** React 18 + Vite
- **Styling:** Tailwind CSS
- **API:** `frontend/src/services/api.js`

---

## Backend Module Map

| Module | Purpose |
|--------|---------|
| `src/pipeline/` | Stage-based orchestration (ingest -> persist) |
| `src/rules/` | Modular AML typology rules (structuring, dormant, flow_through, rapid_withdraw, high_risk_country, low_buyer_diversity) |
| `src/rule_engine.py` | Canonical rule orchestrator - imports from `src/rules/` |
| `src/risk_engine.py` | Log-odds meta-risk scoring (ML prob + segment + country + rule severity priors) |
| `src/risk_governance.py` | Distribution control, uncertainty penalty, baseline confidence |
| `src/scoring.py` | Full ML training + inference pipeline (behavioral/structural/temporal models) |
| `src/features.py` | Feature engineering (behavioral baselines, winsorization, one-hot segment) |
| `src/ml/` | LightGBM training, time-based split, calibration, metrics, active learning |
| `src/evaluation_service.py` | Production-grade metrics (analyst disposition labels, PR-AUC, temporal holdout) |
| `src/suppression.py` | Vectorized alert suppression (signature dedup, per-user daily cap) |
| `src/hard_constraints.py` | Sanctions / mandatory overrides (never suppressed) |
| `src/governance/` | Performance monitoring, PSI drift detection |
| `src/storage.py` | SQLite/Postgres abstraction |
| `src/ai_summary.py` | AI narrative generation (Gemini) for case summaries |

---

## ML Pipeline

### Training (scripts/train_model.py)
1. Time-based split: train -> validation -> test (calendar month windows, never random)
2. Labels: `compute_labels()` maps analyst dispositions to `y_sar` and `y_escalated`
3. Model: LightGBM with monotonic constraints and imbalance weighting
4. Calibration: Isotonic regression fitted on **validation set only**
5. Metrics: PR-AUC (primary), TP retention @ suppression rate, suppression @ 98% TP retention
6. Config: `backend/config/ml.yaml`

### Inference
`scoring.py -> risk_engine.compute_risk() -> risk_governance.apply_risk_governance()`

---

## Critical Rules (Never Break These)

- **All config values come from `src/config.py`** - never hardcode weights, thresholds, or flags
- **Risk weights must sum to 1.0** - enforced by assertion in config.py
- **No debug file writes in production code** - use `logging.getLogger(__name__)` only
- **No random train/test splits** - always use `src/ml/split.py` time-based split
- **Evaluation labels** - never use `synthetic_true_suspicious` as ground truth for production metrics
- **Hard constraints override everything** - sanctions hits must never be suppressed
- **Suppression is vectorized** - never use Python for-loops over user/signature combinations

---

## Key Config Constants

```python
# src/config.py - canonical values
RISK_RULE_WEIGHT        = 0.25
RISK_BEHAVIORAL_WEIGHT  = 0.35
RISK_STRUCTURAL_WEIGHT  = 0.15
RISK_TEMPORAL_ML_WEIGHT = 0.25
# Sum must equal 1.0 (enforced by assertion)

OVERLAY_MODE = True   # Run as post-detection overlay (no raw transaction ML)
RISK_BAND_T1 = 40     # LOW/MEDIUM threshold
RISK_BAND_T2 = 70     # MEDIUM/HIGH threshold
RISK_BAND_T3 = 90     # HIGH/CRITICAL threshold
```

---

## Running the System

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload

# Train ML model
python scripts/train_model.py --data data/bank_alerts_1000.csv

# Run tests
python -m pytest tests/ -v

# Frontend
cd frontend
npm install && npm run dev
```

---

## Testing

```bash
# All tests
cd backend && python -m pytest tests/ -v

# Specific suites
python -m pytest tests/test_rules_module.py -v          # Rule engine
python -m pytest tests/test_technical_review_fixes.py -v # Pipeline regression
python -m pytest tests/ml/ -v                            # ML pipeline
```

---

## Grounding Instruction

Before writing any code, read:
1. `backend/src/config.py` - all constants and their purpose
2. The relevant module(s) listed in the module map above
3. The existing tests in `backend/tests/` for the area you are changing

Do not modify `scoring.py` or `features.py` feature columns without also updating the test suite.
Do not add new rule logic to `services/rule_engine.py` - add it to `src/rules/` instead.
Do not add new weight constants to config.py without updating the sum assertion.
