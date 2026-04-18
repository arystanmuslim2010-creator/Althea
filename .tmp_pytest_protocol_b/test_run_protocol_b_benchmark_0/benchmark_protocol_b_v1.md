# ALTHEA IBM AML Benchmark Protocol B

This benchmark intentionally makes the task harder and cleaner:
- features come only from the past 24h observation window `[T-24h, T]`
- labels come only from the future 24h outcome window `(T, T+24h]`
- no train-only label-rate encodings are used
- pattern-derived shortcut features are excluded from the primary model

## Dataset

- Source artifact reused: `.tmp_pytest_protocol_b\test_run_protocol_b_benchmark_0\alerts.jsonl`
- Feature cache: `.tmp_pytest_protocol_b\test_run_protocol_b_benchmark_0\protocol_b.features.csv`
- Total protocol-B alerts: `80`
- Positive alerts: `16`
- Negative alerts: `64`
- Positive rate: `0.2000`

## Strict Chronological Split

- Train alerts: `48`
- Validation alerts: `16`
- Test alerts: `16`

## Benchmark Table

| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | baseline | 0.0000 | 0.3333 | 0.0000 | 0.2500 | 0.0000 | 0.2342 |
| amount_descending | baseline | 0.6667 | 1.0000 | 1.0000 | 0.7500 | 0.8125 | 1.0000 |
| weighted_signal_heuristic | heuristic | 0.6667 | 1.0000 | 1.0000 | 0.7500 | 0.8125 | 1.0000 |
| althea_protocol_b_logistic_regression | model | 0.6667 | 1.0000 | 1.0000 | 0.7500 | 0.8125 | 1.0000 |

## Feature Ablations

| Run | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Note |
| --- | ---: | ---: | ---: | --- |
| althea_protocol_b_logistic_regression | 0.6667 | 1.0000 | 1.0000 | Protocol B primary candidate: past-24h observation features only, future-24h labels only, no train-only label encodings, no pattern-derived shortcut features. |
| ablation_amount_features | 0.6667 | 1.0000 | 1.0000 | Removed amount and normalized-amount features. |
| ablation_history_features | 0.6667 | 1.0000 | 1.0000 | Removed account-history features derived from prior protocol-B anchor windows. |
| ablation_payment_currency_mix_features | 0.6667 | 1.0000 | 1.0000 | Removed payment-format, currency-mix, and temporal mix features. |
| pattern_derived_control | 0.6667 | 1.0000 | 1.0000 | Control run that reintroduces past-window pattern-derived features. Excluded from the primary benchmark because they remain leakage-like shortcuts relative to real bank operations. |

## Readout

- Primary ALTHEA protocol-B model beats chronological queue at Recall@Top 10%: `True`
- Primary ALTHEA protocol-B model beats amount baseline at Recall@Top 10%: `False`
- Primary ALTHEA protocol-B model beats weighted heuristic at Recall@Top 10%: `False`
- Benchmark convenience reduced: `True`
  Feature windows and label windows are now temporally decoupled.
- Leakage reduced: `True`
  Primary model excludes train-only label encodings and excludes pattern-derived shortcut features.
- Old convenience benchmark reference: `reports\benchmark_sanity_v1.json`
- Old Recall@Top 10%: `0.8467`
- New Recall@Top 10%: `0.6667`
- New benchmark more trustworthy than old benchmark: `True`
