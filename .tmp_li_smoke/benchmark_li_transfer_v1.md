# ALTHEA IBM LI-Small Transfer Benchmark

This report adds only the next LI-Small transfer result. It reuses existing HI benchmark artifacts and does not rerun the full HI benchmark or the prior sanity suite.

## Artifact Reuse

- Reused HI benchmark summary: `.tmp_li_smoke\benchmark_v2.json`
- Reused HI feature cache: `.tmp_li_smoke\hi.features.csv`
- Reused LI alert JSONL: `True`
- Reused LI feature CSV: `True`

## LI Dataset

- Alert grouping: `source_account_24h`
- LI alerts: `80`
- LI positive alerts: `20`
- LI negative alerts: `60`
- LI positive rate: `0.2500`
- LI average transactions per alert: `1.75`

## Schema Compatibility

- LI transaction header matches HI parser: `True`
- LI feature columns exactly match HI feature columns: `True`
- LI accounts CSV present but unused by the current protocol: `True`

## LI Benchmark Comparison

| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | baseline | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.3750 | 0.1556 |
| amount_descending | baseline | 1.0000 | 1.0000 | 1.0000 | 0.5000 | 0.8750 | 1.0000 |
| weighted_signal_heuristic | heuristic | 1.0000 | 1.0000 | 1.0000 | 0.5000 | 0.8750 | 1.0000 |
| hi_trained_logistic_regression_raw_signals_on_li | transfer | 1.0000 | 1.0000 | 1.0000 | 0.5000 | 0.8750 | 1.0000 |

## Readout

- HI-trained champion recall@top10 on LI: `1.0000`
- Amount baseline recall@top10 on LI: `1.0000`
- Transfer verdict vs amount baseline: `generalized_above_amount_baseline`
- External-claim verdict: synthetic benchmark evidence only. This still is not sufficient for customer-facing or investor-facing performance claims without stronger out-of-distribution validation.
