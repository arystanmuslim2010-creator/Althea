# ALTHEA IBM LI-Small Transfer Benchmark

This report adds only the next LI-Small transfer result. It reuses existing HI benchmark artifacts and does not rerun the full HI benchmark or the prior sanity suite.

## Artifact Reuse

- Reused HI benchmark summary: `reports\benchmark_v2.json`
- Reused HI feature cache: `data\processed\ibm_aml_alerts\benchmark_features\source_account_24h.features.csv`
- Reused LI alert JSONL: `False`
- Reused LI feature CSV: `False`

## LI Dataset

- Alert grouping: `source_account_24h`
- LI alerts: `1785721`
- LI positive alerts: `2629`
- LI negative alerts: `1783092`
- LI positive rate: `0.0015`
- LI average transactions per alert: `3.88`

## Schema Compatibility

- LI transaction header matches HI parser: `True`
- LI feature columns exactly match HI feature columns: `True`
- LI accounts CSV present but unused by the current protocol: `True`

## LI Benchmark Comparison

| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | baseline | 0.1261 | 0.2107 | 0.0024 | 0.0020 | 0.0556 | 0.0018 |
| amount_descending | baseline | 0.1172 | 0.2181 | 0.0022 | 0.0021 | 0.4844 | 0.0047 |
| weighted_signal_heuristic | heuristic | 0.1246 | 0.2151 | 0.0024 | 0.0020 | 0.4217 | 0.0387 |
| hi_trained_logistic_regression_raw_signals_on_li | transfer | 0.7374 | 0.9110 | 0.0139 | 0.0086 | 0.8785 | 0.0996 |
| li_native_logistic_regression_raw_signals | model | 0.7226 | 0.9362 | 0.0136 | 0.0088 | 0.8718 | 0.0764 |

## Readout

- HI-trained champion recall@top10 on LI: `0.7374`
- Amount baseline recall@top10 on LI: `0.1172`
- Transfer verdict vs amount baseline: `generalized_above_amount_baseline`
- External-claim verdict: synthetic benchmark evidence only. This still is not sufficient for customer-facing or investor-facing performance claims without stronger out-of-distribution validation.
