# ALTHEA Pilot Readiness

## What ALTHEA Can Demonstrate

ALTHEA is a post-detection AML investigation layer. It ingests alerts from an upstream monitoring system, ranks them for review, explains why they matter in analyst language, and supports investigation workflow.

For pilot readiness, ALTHEA can now demonstrate:

- Ranking quality against simple operational baselines.
- Human-readable analyst explanations that use AML-native language.
- An operational alert queue and structured alert detail response.
- A lightweight pilot value summary for operators and sponsors.

## Ranking Evaluation

ALTHEA now supports a reproducible ranking evaluation flow through the evaluation service and API.

Compared rankings:

- ALTHEA ranking
- Chronological baseline
- Amount-descending baseline
- Random baseline
- Simple heuristic baseline when the required fields exist

Primary metrics:

- `Recall@Top10%`
- `Recall@Top20%`
- `Recall@Top30%`
- `Precision@Top10%`
- `Precision@Top20%`
- `SAR capture at top 10/20/30%`
- `Workload reduction at target recall`
- `PR-AUC` when labels are valid
- `Lift over random baseline`

The evaluation output is designed to be pilot-readable and investor-readable. Example:

`ALTHEA captured 78% of suspicious alerts in the top 20% of the queue, outperforming the amount desc baseline by 1.90x.`

## Invalid Evaluation Safeguards

ALTHEA does not report misleading ranking metrics when the dataset is unsuitable.

Evaluation is marked invalid when:

- no labels are present
- all labels are positive
- all labels are negative

In those cases the system returns:

`Evaluation requires both positive and negative labeled alerts.`

Functional scoring and pipeline behavior still continue. The system only suppresses misleading ranking-quality claims.

## Explainability Behavior

ALTHEA now separates:

- analyst-facing explanation
- technical explanation payload

The analyst-facing explanation includes:

- `summary_text`
- `key_risk_drivers`
- `aml_patterns`
- `analyst_next_steps`
- `confidence_level`

The technical payload is still preserved for audit, model review, and debugging, but it is no longer the primary explanation shown to analysts.

## What Is Hidden From Normal Analyst UI

By default, analyst queue and detail responses do not expose:

- `evaluation_label_is_sar`
- raw internal evaluation labels
- debug-first explanation payloads as the primary explanation

These can still exist in admin, governance, or technical fields where appropriate.

## Pilot Workflow Readiness

The queue response now supports:

- default priority sorting
- pagination
- risk-band filtering
- status filtering
- alert/account search where available
- empty-state handling
- clear invalid-filter errors

The alert detail response now supports:

- structured risk section
- investigation summary
- human explanation
- entities and transactions
- sorted timeline
- workflow state
- separated technical details

## Pilot Value Summary

The pilot summary can now report:

- total ingested alerts
- priority-band distribution
- average risk score
- alerts with explanations
- ingestion warnings
- evaluation summary when valid labels exist

When labels are not suitable, the summary states:

`Evaluation labels unavailable or not suitable for ranking validation.`

## Remaining Real-Bank Validation

The following still require real-bank validation before production claims:

- label quality and consistency across institutions
- analyst workflow fit in live case handling
- calibration of thresholds by bank segment and typology
- jurisdiction-specific policy tuning
- measurable downstream impact on investigation throughput and SAR conversion

ALTHEA supports pilot evidence generation. It does not by itself prove production effectiveness across all environments.
