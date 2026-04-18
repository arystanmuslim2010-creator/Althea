# ALTHEA IBM AML Benchmark Improvement Sprint

This report summarizes a focused alert-ranking improvement pass on IBM AML-Data `HI-Small` synthetic alerts.

Important scope note: this is synthetic benchmark validation only. It is not live bank validation, not a production ROI proof, and not evidence of bank deployment readiness.

## Default Benchmark Dataset

- Grouping variant: `source_account_24h`
- Total alerts: `1307771`
- Positive alerts: `3878`
- Negative alerts: `1303893`
- Average transactions per alert: `3.88`
- Typology assignment rate: `0.0018`

## Chronological Split

- Train = first 60%, validation = next 20%, test = final 20%.
- Train alerts: `784662`
- Validation alerts: `261554`
- Test alerts: `261555`

## Diagnosis: Why Current ALTHEA Loses

- Current ALTHEA scoring is using a bootstrap demo RandomForest model that was never trained on IBM-derived alerts.
- Feature alignment is weak: 9 shared columns, 7 expected legacy columns imputed, 41 current bundle columns dropped.
- Amount-heavy positives are materially under-captured by the current ALTHEA ranking relative to the simple amount queue.
- The active model therefore behaves like a schema-mismatched fallback, not a benchmark-calibrated alert prioritizer.

- Amount baseline positives captured in top 10%: `273`
- Current ALTHEA positives captured in top 10%: `39`
- Positives captured by amount but missed by ALTHEA top 10%: `273`
- Median total amount (USD proxy) of amount-only positives: `50051.02`
- Median tx count of amount-only positives: `3.00`
- Shared current/expected model features: `9`
- Legacy schema columns imputed at inference: `7`
- Rich current bundle columns dropped at inference: `41`

## Benchmark Table

| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| logistic_regression_raw_signals | model | 0.8467 | 0.9632 | 0.0422 | 0.0240 | 0.9230 | 0.1967 |
| lightgbm_top_recall | model | 0.7816 | 0.7824 | 0.0390 | 0.0195 | 0.6541 | 0.3213 |
| logistic_regression_full | model | 0.6889 | 0.8184 | 0.0344 | 0.0204 | 0.8213 | 0.1285 |
| lightgbm_balanced | model | 0.4805 | 0.9272 | 0.0240 | 0.0231 | 0.8818 | 0.0365 |
| amount_descending | baseline | 0.2092 | 0.3931 | 0.0104 | 0.0098 | 0.5873 | 0.0108 |
| distinct_counterparties_descending | baseline | 0.1916 | 0.3134 | 0.0096 | 0.0078 | 0.1076 | 0.0257 |
| weighted_signal_heuristic | heuristic | 0.1326 | 0.2544 | 0.0066 | 0.0063 | 0.5340 | 0.0235 |
| amount_usd_descending | baseline | 0.1119 | 0.4391 | 0.0056 | 0.0110 | 0.6280 | 0.0130 |
| chronological_queue | baseline | 0.1027 | 0.1517 | 0.0051 | 0.0038 | 0.0010 | 0.0037 |
| transaction_count_descending | baseline | 0.0935 | 0.1739 | 0.0047 | 0.0043 | 0.0471 | 0.0217 |

## Champion

- Selected by validation priority metric: `logistic_regression_raw_signals`
- Test Recall@Top 10%: `0.8467`
- Test Recall@Top 20%: `0.9632`
- Test Precision@Top 10%: `0.0422`
- Test PR-AUC: `0.1967`
- Notes: Standardized logistic regression on direct alert signals plus chronology-safe history features, excluding train-only label/pattern rate encodings.

## Feature Changes

- Added explicit amount-aware alert features: raw total/max/mean/min/std/range and static-FX USD-normalized equivalents.
- Added alert structure features: transaction count, distinct counterparties, destination-bank breadth, counterparty concentration, time span, gap metrics, payment-format mix, same-bank ratio, mixed-currency flags, round-amount ratio, night/weekend activity ratios.
- Added label-free chronology-safe history features: prior alert counts, time since prior alert, prior rolling amount totals and averages by source account and source bank.
- Added train-only category encodings for source bank, dominant destination bank, dominant currency, and payment format; added train-only pattern-rate priors without using direct exact-match test annotations as model inputs.

## Grouping Sensitivity

- `source_account_6h`: positive_rate=0.0021, avg_tx_per_alert=2.50, amount_baseline_recall@10=0.2059, weighted_heuristic_recall@10=0.1236, champion_recall@10=0.9575
- `source_destination_24h`: positive_rate=0.0023, avg_tx_per_alert=2.34, amount_baseline_recall@10=0.1156, weighted_heuristic_recall@10=0.0396, champion_recall@10=0.9012

## Limitations

- Labels remain synthetic transaction-derived proxies, not true SAR or case outcomes.
- Static FX normalization is benchmark-only scaffolding; it is approximate and intended only to reduce obvious cross-currency distortion in IBM AML-Data.
- Pattern file enrichment is used conservatively through train-only priors; direct exact-match test annotations were not used as primary model features.
- Grouping sensitivity outside the default source-account 24h variant still depends on synthetic grouping rules, not a real bank rules engine.

## Champion Top Features

- `log_total_amount_usd`: `6.9666`
- `log_transaction_count`: `-6.5263`
- `log_max_amount_usd`: `-5.8747`
- `log_unique_destination_accounts`: `5.0219`
- `source_bank_prior_avg_amount_usd`: `-4.6988`
- `std_amount_usd`: `4.4942`
- `unique_destination_accounts`: `3.6551`
- `min_amount_usd`: `3.6045`
- `max_amount`: `-2.7900`
- `amount_range_usd`: `-2.7858`
