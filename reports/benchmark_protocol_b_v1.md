# ALTHEA IBM AML Benchmark Protocol B

This benchmark intentionally makes the task harder and cleaner:
- features come only from the past 24h observation window `[T-24h, T]`
- labels come only from the future 24h outcome window `(T, T+24h]`
- no train-only label-rate encodings are used
- pattern-derived shortcut features are excluded from the primary model

## Dataset

- Source artifact reused: `data\processed\ibm_aml_alerts\hi_small_alerts.jsonl`
- Feature cache: `data\processed\ibm_aml_alerts\benchmark_features\protocol_b_source_account_24h.features.csv`
- Total protocol-B alerts: `1307771`
- Positive alerts: `1333`
- Negative alerts: `1306438`
- Positive rate: `0.0010`

## Strict Chronological Split

- Train alerts: `784662`
- Validation alerts: `261554`
- Test alerts: `261555`

## Benchmark Table

| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | baseline | 0.1164 | 0.2179 | 0.0015 | 0.0014 | 0.0016 | 0.0012 |
| amount_descending | baseline | 0.2806 | 0.5403 | 0.0036 | 0.0035 | 0.5591 | 0.0080 |
| weighted_signal_heuristic | heuristic | 0.3343 | 0.4925 | 0.0043 | 0.0032 | 0.5982 | 0.0554 |
| althea_protocol_b_logistic_regression | model | 0.5224 | 0.6299 | 0.0067 | 0.0040 | 0.5290 | 0.1289 |

## Feature Ablations

| Run | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Note |
| --- | ---: | ---: | ---: | --- |
| althea_protocol_b_logistic_regression | 0.5224 | 0.6299 | 0.0067 | Protocol B primary candidate: past-24h observation features only, future-24h labels only, no train-only label encodings, no pattern-derived shortcut features. |
| ablation_amount_features | 0.5254 | 0.6239 | 0.0067 | Removed amount and normalized-amount features. |
| ablation_history_features | 0.5284 | 0.6358 | 0.0068 | Removed account-history features derived from prior protocol-B anchor windows. |
| ablation_payment_currency_mix_features | 0.4358 | 0.5612 | 0.0056 | Removed payment-format, currency-mix, and temporal mix features. |
| pattern_derived_control | 0.5582 | 0.6239 | 0.0071 | Control run that reintroduces past-window pattern-derived features. Excluded from the primary benchmark because they remain leakage-like shortcuts relative to real bank operations. |

## Readout

- Primary ALTHEA protocol-B model beats chronological queue at Recall@Top 10%: `True`
- Primary ALTHEA protocol-B model beats amount baseline at Recall@Top 10%: `True`
- Primary ALTHEA protocol-B model beats weighted heuristic at Recall@Top 10%: `True`
- Benchmark convenience reduced: `True`
  Feature windows and label windows are now temporally decoupled.
- Leakage reduced: `True`
  Primary model excludes train-only label encodings and excludes pattern-derived shortcut features.
- Old convenience benchmark reference: `reports\benchmark_sanity_v1.json`
- Old Recall@Top 10%: `0.8467`
- New Recall@Top 10%: `0.5224`
- New benchmark more trustworthy than old benchmark: `True`
