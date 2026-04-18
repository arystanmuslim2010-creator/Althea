# IBM AML-Data Benchmark

This document describes the first ALTHEA benchmark pipeline built for IBM AML-Data `HI-Small`.

## Scope

- Input files:
  - `HI-Small_Trans.csv`
  - `HI-Small_Patterns.txt`
- Output:
  - alert-level JSONL for ALTHEA ingestion and evaluation
  - a benchmark summary JSON
  - a markdown report

This benchmark is synthetic validation only. It does not represent live bank deployment, real SAR outcomes, or production ROI proof.

## Alert Construction

- One synthetic alert is created for each source account inside an anchored 24-hour window.
- Source account key is `"<From Bank>:<Account>"`.
- A new alert window starts when the next transaction for that source account is at least 24 hours after the current alert start time.
- The alert label is a proxy:
  - `evaluation_label_is_sar = 1` if any transaction in the alert has `Is Laundering = 1`
  - otherwise `0`

## Typology Enrichment

- `HI-Small_Patterns.txt` is parsed as block-scoped laundering attempts.
- Supported pattern families observed in `HI-Small` include:
  - `FAN-OUT`
  - `FAN-IN`
  - `CYCLE`
  - `STACK`
  - `GATHER-SCATTER`
  - `SCATTER-GATHER`
  - `BIPARTITE`
  - `RANDOM`
- Transactions are matched back to the main CSV by exact normalized row signature.
- An alert gets a typology only when all matched laundering transactions in that alert resolve to one typology.
- Otherwise the alert typology is `unknown`.

## Outputs

- Converted alerts:
  - `data/processed/ibm_aml_alerts/hi_small_alerts.jsonl`
- Conversion summary:
  - `data/processed/ibm_aml_alerts/hi_small_alerts.summary.json`
- Benchmark report:
  - `reports/benchmark_v1.md`
- Benchmark summary JSON:
  - `reports/benchmark_v1.json`
- Improvement benchmark report:
  - `reports/benchmark_v2.md`
- Improvement benchmark summary JSON:
  - `reports/benchmark_v2.json`

Generated processed artifacts under `data/processed/ibm_aml_alerts/` are ignored by git on purpose.

## How To Run

From the repo root:

```powershell
python backend/scripts/aml_data_to_alert_jsonl.py `
  --transactions "C:\path\to\HI-Small_Trans.csv" `
  --patterns "C:\path\to\HI-Small_Patterns.txt"
```

Then run the benchmark:

```powershell
python backend/scripts/run_aml_benchmark.py `
  --alerts "data\processed\ibm_aml_alerts\hi_small_alerts.jsonl"
```

If you want to skip the ALTHEA score baseline:

```powershell
python backend/scripts/run_aml_benchmark.py `
  --alerts "data\processed\ibm_aml_alerts\hi_small_alerts.jsonl" `
  --disable-althea-baseline
```

## Improvement Sprint Benchmark

The v2 path adds:

- compact alert-level feature extraction caches
- stronger baselines:
  - `amount_usd_descending`
  - `transaction_count_descending`
  - `distinct_counterparties_descending`
  - `weighted_signal_heuristic`
- focused diagnostics against the current ALTHEA production-path model
- practical alert-level candidate models:
  - `logistic_regression_raw_signals`
  - `logistic_regression_full`
  - `lightgbm_balanced`
  - `lightgbm_top_recall`
- grouping sensitivity runs for:
  - `source_account_24h`
  - `source_account_6h`
  - `source_destination_24h`

Run the improvement benchmark from the repo root:

```powershell
python backend/scripts/run_aml_benchmark_improvement.py `
  --alerts "data\processed\ibm_aml_alerts\hi_small_alerts.jsonl" `
  --transactions "C:\path\to\HI-Small_Trans.csv" `
  --patterns "C:\path\to\HI-Small_Patterns.txt"
```

If you only want the default source-account 24h benchmark and want to skip grouping sensitivity:

```powershell
python backend/scripts/run_aml_benchmark_improvement.py `
  --alerts "data\processed\ibm_aml_alerts\hi_small_alerts.jsonl" `
  --skip-grouping-variants
```

If you only want to rebuild cached compact features:

```powershell
python backend/scripts/run_aml_benchmark_improvement.py `
  --alerts "data\processed\ibm_aml_alerts\hi_small_alerts.jsonl" `
  --skip-grouping-variants `
  --skip-althea-diagnosis `
  --force-rebuild-features
```

## Baselines

- `chronological_queue`
  - alerts ranked by `created_at` ascending
- `amount_descending`
  - alerts ranked by total transaction amount descending
- `althea_score`
  - local ALTHEA model ranking if a resolvable approved model exists in the local runtime database and object storage

## Metrics

Reported metrics focus on alert prioritization:

- dataset stats
  - total alerts
  - positive alerts
  - negative alerts
  - average transactions per alert
  - alerts with typology assigned
- ranking metrics
  - Recall@Top 10%
  - Recall@Top 20%
  - Precision@Top 10%
  - Precision@Top 20%
  - review reduction at 80% recall
  - PR-AUC

## Chronological Split

- Train = first 60%
- Validation = next 20%
- Test = final 20%

Split order is based on alert `created_at` after chronological sorting. No random shuffling is used.

## Known Limitations

- Labels are transaction-derived laundering proxies, not true analyst or SAR dispositions.
- Amount-based heuristics are currency-naive in v1 because the dataset does not provide a robust benchmark FX normalization path.
- v2 adds static FX normalization to reduce cross-currency distortion, but it remains an approximate benchmark-only scaffold rather than a production FX service.
- Typology enrichment depends on exact row-level pattern matches and leaves some alerts as `unknown`.
- The v2 champion is benchmark-optimized for IBM-derived alerts; it should not be treated as a production-approved ALTHEA model artifact without separate governance and registration work.
- The observed time horizon in `HI-Small` is short, so temporal realism is limited.
