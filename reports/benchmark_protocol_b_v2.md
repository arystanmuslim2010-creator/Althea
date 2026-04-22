# ALTHEA IBM AML Benchmark Protocol B v2

This run keeps Protocol B strict:
- past-only feature construction
- future-only labels
- chronological split
- no pattern-derived shortcut features in the primary score

## Dataset

- Source alerts: `data\processed\ibm_aml_alerts\hi_small_alerts.jsonl`
- Reused base feature cache: `data\processed\ibm_aml_alerts\benchmark_features\protocol_b_source_account_24h.features.csv`
- Extra strict v2 feature cache: `data\processed\ibm_aml_alerts\benchmark_features\protocol_b_source_account_24h.v2_extra.features.csv`
- Total alerts: `1307771`
- Positive alerts: `1333`
- Negative alerts: `1306438`

## Current Champion Diagnosis

- Current Protocol B v1 Recall@Top 10%: `0.5224`
- Current Protocol B v1 Precision@Top 10%: `0.0067`
- Positives captured by weighted heuristic but missed by current champion in top decile: `24`
- Current top-decile false negatives: `160`

## Model Comparison

| Candidate | Kind | Family | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protocol_b_v2_sparse_logistic_behavior_shift | model | sparse_logistic_regression | 0.5433 | 0.6537 | 0.0070 | 0.0042 | 0.6093 | 0.1368 |
| protocol_b_v2_sparse_logistic_recent_entropy | model | sparse_logistic_regression | 0.5433 | 0.6448 | 0.0070 | 0.0041 | 0.5982 | 0.1335 |
| protocol_b_v2_sparse_logistic_graph_entropy | model | sparse_logistic_regression | 0.5403 | 0.6537 | 0.0069 | 0.0042 | 0.5749 | 0.1431 |
| protocol_b_v2_sparse_logistic_base | model | sparse_logistic_regression | 0.5343 | 0.6478 | 0.0068 | 0.0041 | 0.6016 | 0.1431 |
| althea_protocol_b_logistic_regression_reference | model | - | 0.5224 | 0.6299 | 0.0067 | 0.0040 | 0.5290 | 0.1289 |
| protocol_b_v2_logistic_regression_hardneg | model | logistic_regression | 0.4328 | 0.5134 | 0.0055 | 0.0033 | 0.4240 | 0.1066 |
| protocol_b_v2_logistic_regression | model | logistic_regression | 0.4179 | 0.5433 | 0.0054 | 0.0035 | 0.4965 | 0.1098 |
| protocol_b_v2_lightgbm_hardneg | model | lightgbm | 0.4030 | 0.4209 | 0.0052 | 0.0027 | 0.2070 | 0.0030 |
| distinct_counterparties_descending | baseline | - | 0.3522 | 0.4776 | 0.0045 | 0.0031 | 0.3534 | 0.0554 |
| weighted_signal_heuristic | heuristic | - | 0.3343 | 0.4925 | 0.0043 | 0.0032 | 0.5982 | 0.0554 |
| transaction_count_descending | baseline | - | 0.2925 | 0.4597 | 0.0037 | 0.0029 | 0.4382 | 0.0557 |
| amount_descending | baseline | - | 0.2806 | 0.5403 | 0.0036 | 0.0035 | 0.5591 | 0.0080 |
| protocol_b_v2_lightgbm | model | lightgbm | 0.1851 | 0.3104 | 0.0024 | 0.0020 | 0.2751 | 0.0024 |
| chronological_queue | baseline | - | 0.1164 | 0.2179 | 0.0015 | 0.0014 | 0.0016 | 0.0012 |

## Champion

- Selected champion: `protocol_b_v2_sparse_logistic_graph_entropy`
- Recall@Top 10%: `0.5403`
- Recall@Top 20%: `0.6537`
- Precision@Top 10%: `0.0069`
- Precision@Top 20%: `0.0042`
- PR-AUC: `0.1431`
- Improved beyond current Protocol B reference `0.5224`: `True`

## Ablation Safety Check

| Ablation | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Delta vs champion |
| --- | ---: | ---: | ---: | ---: |
| ablation_amount_features_v2 | 0.5403 | 0.6448 | 0.0069 | 0.0000 |
| ablation_history_temporal_features_v2 | 0.5403 | 0.6537 | 0.0069 | 0.0000 |
| ablation_payment_currency_mix_features_v2 | 0.4119 | 0.5701 | 0.0053 | -0.1284 |
| ablation_graph_network_features_v2 | 0.5403 | 0.6478 | 0.0069 | 0.0000 |

## Strictness Guardrails

- Benchmark convenience reduced further: `True`
- Future information added to features: `False`
- Pattern-derived shortcuts restored into primary score: `False`
- Test data used for tuning: `False`

## Notes

- This remains a synthetic IBM-derived benchmark, not live bank validation.
- Stronger numbers here should still be treated as internal benchmark evidence only.
