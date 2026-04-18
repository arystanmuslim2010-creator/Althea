# ALTHEA IBM Alert Benchmark Protocol

## Locked Primary Benchmark

- Dataset source: IBM AML-Data `HI-Small` transaction CSV plus pattern file.
- Alert construction: `source_account_24h` anchored windows.
- Primary split: chronological `60/20/20` by alert `created_at` ascending.
- Primary model family: `logistic_regression_raw_signals`.
- Explicit exclusion: `logistic_regression_full` is not the primary externally-citable number because it includes train-only label/pattern-rate encodings.
- Required baselines:
  - chronological_queue
  - amount_descending
  - amount_usd_descending
  - transaction_count_descending
  - distinct_counterparties_descending
  - weighted_signal_heuristic
- Required metrics:
  - Recall@Top 10%
  - Recall@Top 20%
  - Precision@Top 10%
  - Precision@Top 20%
  - review reduction at fixed recall
  - PR-AUC
- Required sanity checks:
  - feature ablation suite
  - label/future leakage audit
  - alternative chronological splits
  - cross-grouping transfer
  - shuffled-label control
  - decile uplift table
  - subgroup sensitivity tables

## Current Protocol Notes

- Proxy label: `evaluation_label_is_sar = 1` if any transaction in the grouped alert window has `Is Laundering = 1`.
- Pattern file remains enrichment only, but even `pattern_assigned` should be treated as leakage-like convenience and reported explicitly.
- Chronology-safe history features are allowed only if they are derived from prior rows after chronological sorting and never from future alerts.
- Any future benchmark number must be reported together with the corresponding sanity report JSON/MD outputs.

- Current sanity summary path: `reports\benchmark_sanity_v1.json`
- Current sanity report path: `reports\benchmark_sanity_v1.md`
