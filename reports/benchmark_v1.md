# ALTHEA Benchmark v1

This report summarizes a first-pass synthetic benchmark built from IBM AML-Data `HI-Small`.

Important scope note: this is synthetic benchmark validation only. It is not live bank validation, not production ROI proof, and not evidence of deployment readiness on real customer data.

## Dataset

- Dataset: `IBM AML-Data HI-Small`
- Transactions CSV: `HI-Small_Trans.csv`
- Patterns file: `HI-Small_Patterns.txt`
- Alert JSONL: `data\processed\ibm_aml_alerts\hi_small_alerts.jsonl`

## Alert Construction

- Grouping rule: one synthetic alert = all transactions from the same source account within an anchored 24-hour window.
- Label rule: `evaluation_label_is_sar = 1` if any transaction in the alert has `Is Laundering = 1`; otherwise `0`.
- Label semantics: this is a synthetic proxy label derived from IBM AML-Data transactions, not a real SAR outcome.
- Typology enrichment: pattern-file transaction matches are lifted to alert level only when a single typology maps cleanly to the alert; otherwise typology is `unknown`.

## Chronology

- Min alert timestamp: `2022-09-01T00:00:00Z`
- Max alert timestamp: `2022-09-18T16:18:00Z`
- Observed span: `17.679166666666667` days
- Unique calendar days: `18`

## Dataset Stats

- Total alerts: `1307771`
- Positive alerts: `3878`
- Negative alerts: `1303893`
- Average transactions per alert: `3.88`
- Alerts with typology assigned: `2358`

## Split Logic

- Chronological split by alert `created_at` after sorting ascending.
- Train = first 60%, validation = next 20%, test = final 20%.

- Train alerts: `784662`
- Validation alerts: `261554`
- Test alerts: `261555`

## Baselines

- Baseline A: chronological queue (`created_at` ascending).
- Baseline B: simple amount heuristic (`total transaction amount` descending).
- Baseline C: ALTHEA score-based ranking using local model `model-50fd74dbbda8`.

## Validation Metrics

| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | 0.1099 | 0.1862 | 0.0050 | 0.0042 | 0.1961 |
| amount_descending | 0.1602 | 0.2601 | 0.0073 | 0.0059 | 0.4007 |
| althea_score | 0.0235 | 0.0898 | 0.0011 | 0.0020 | 0.1327 |

## Test Metrics

| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| chronological_queue | 0.1027 | 0.1517 | 0.0051 | 0.0038 | 0.0010 |
| amount_descending | 0.2092 | 0.3931 | 0.0104 | 0.0098 | 0.5873 |
| althea_score | 0.0383 | 0.0874 | 0.0019 | 0.0022 | 0.0893 |

## Known Limitations

- The benchmark label is a transaction-derived proxy, not a true case or SAR disposition.
- Alert grouping is synthetic and anchored to 24-hour source-account windows; it does not replicate a bank production alerting rule stack.
- Amount-based comparisons are currency-naive in v1 because IBM AML-Data does not provide a stable FX-normalized benchmark amount.
- The ALTHEA score baseline uses the currently registered local model as-is; it was not retrained or recalibrated on IBM-derived alert labels for this benchmark.
- Typology enrichment depends on exact transaction matches against the pattern file, so some laundering alerts remain `unknown` at alert level.
- The observed chronology spans a short synthetic period, so seasonal and operational drift are underrepresented.

## Chronology Warnings

- Converter observed 2418736 out-of-order transaction rows.
