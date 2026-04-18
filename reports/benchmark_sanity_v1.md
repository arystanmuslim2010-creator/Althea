# ALTHEA IBM Benchmark Sanity Report

This report is intentionally skeptical. It treats the current high benchmark score as unproven until it survives attempts to break it.

## Primary Setup

- Dataset: `IBM AML-Data HI-Small synthetic alert benchmark`
- Primary grouping: `source_account_24h`
- Primary model under test: `logistic_regression_raw_signals`
- Primary test Recall@Top 10%: `0.8467`
- Primary test Recall@Top 20%: `0.9632`
- Primary test Precision@Top 10%: `0.0422`
- Primary test PR-AUC: `0.1967`

## Leakage Audit

- [high] Train-only positive-rate encodings are directly derived from labels and are not acceptable as the primary externally-cited benchmark feature set.
- [medium] Train-only pattern-rate encodings are derived from pattern annotations that originate from the laundering-attempt file and should be treated as benchmark convenience rather than bank-realistic signal.
- [low] Chronology-safe history features are generated via `cumcount`, shifted cumulative totals, and previous timestamps on data sorted by alert `created_at`; this uses only prior rows in the ordered stream, not future alerts.
- [high] Alert labels are defined as `1 if any transaction in the grouped window is laundering`. The same grouped window also determines amount, breadth, and cadence features, so the task is structurally easier than real alert adjudication and likely benefits from synthetic grouping convenience.
- [low] Many alerts share identical timestamps. Prior-alert history among same-timestamp rows therefore depends on deterministic row ordering rather than true event latency. This is not future leakage, but it is a synthetic artifact.

## Feature Ablations

| Ablation | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Delta Recall@Top 10% |
| --- | ---: | ---: | ---: | ---: |
| full_feature_champion | 0.8467 | 0.9632 | 0.0422 | n/a |
| ablation_amount_related | 0.8421 | 0.9609 | 0.0420 | -0.0046 |
| ablation_normalized_amount | 0.8268 | 0.9525 | 0.0413 | -0.0199 |
| ablation_history_chronology | 0.8651 | 0.9556 | 0.0432 | 0.0184 |
| ablation_counterparty_breadth | 0.7962 | 0.9464 | 0.0397 | -0.0506 |
| ablation_pattern_derived | skipped | skipped | skipped | skipped |
| ablation_time_cadence | 0.8452 | 0.9448 | 0.0422 | -0.0015 |
| ablation_payment_currency_mix | 0.6904 | 0.8061 | 0.0344 | -0.1563 |

## Split Robustness

| Split | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Test Alerts |
| --- | ---: | ---: | ---: | ---: |
| chronological_60_20_20 | 0.8467 | 0.9632 | 0.0422 | 261555 |
| chronological_50_25_25 | 0.6987 | 0.8547 | 0.0338 | 326943 |
| chronological_70_15_15 | 0.9046 | 0.9787 | 0.0498 | 196166 |
| chronological_60_10_30_later_only | 0.7834 | 0.9368 | 0.0376 | 392332 |

## Cross-Grouping Transfer

| Transfer | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Common Features |
| --- | ---: | ---: | ---: | ---: |
| source_account_24h_to_source_account_6h | 0.7671 | 0.9125 | 0.0294 | 70 |
| source_account_24h_to_source_destination_24h | 0.1496 | 0.5812 | 0.0062 | 70 |
| source_account_6h_to_source_account_24h | 0.8100 | 0.9556 | 0.0404 | 70 |
| source_destination_24h_to_source_account_24h | 0.5448 | 0.8720 | 0.0272 | 70 |

## Randomized-Label Control

- Shuffled-label Recall@Top 10%: `0.1870`
- Shuffled-label Recall@Top 20%: `0.3050`
- Shuffled-label Precision@Top 10%: `0.0093`

## Baseline Comparison

| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | PR-AUC |
| --- | ---: | ---: | ---: | ---: |
| chronological_queue | 0.1027 | 0.1517 | 0.0051 | 0.0037 |
| amount_descending | 0.2092 | 0.3931 | 0.0104 | 0.0108 |
| amount_usd_descending | 0.1119 | 0.4391 | 0.0056 | 0.0130 |
| transaction_count_descending | 0.0935 | 0.1739 | 0.0047 | 0.0217 |
| distinct_counterparties_descending | 0.1916 | 0.3134 | 0.0096 | 0.0257 |
| weighted_signal_heuristic | 0.1326 | 0.2544 | 0.0066 | 0.0235 |

## Decile Analysis

- Global positive rate: `0.0050`

| Champion Decile | Positive Rate | Uplift vs Base | Amount Decile Positive Rate | Amount Decile Uplift |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.0422 | 8.47 | 0.0104 | 2.09 |
| 2 | 0.0058 | 1.16 | 0.0092 | 1.84 |
| 3 | 0.0014 | 0.28 | 0.0115 | 2.30 |
| 4 | 0.0003 | 0.06 | 0.0078 | 1.56 |
| 5 | 0.0001 | 0.02 | 0.0055 | 1.10 |
| 6 | 0.0000 | 0.00 | 0.0029 | 0.57 |
| 7 | 0.0000 | 0.01 | 0.0015 | 0.29 |
| 8 | 0.0000 | 0.00 | 0.0004 | 0.08 |
| 9 | 0.0000 | 0.01 | 0.0004 | 0.08 |
| 10 | 0.0000 | 0.00 | 0.0005 | 0.09 |

## Sensitivity Checks

### Typed Vs Unknown

- typed: alerts=950, positives=950, top10_recall=0.8800, top10_precision=1.0000
- unknown: alerts=260605, positives=355, top10_recall=0.7577, top10_precision=0.0106

### Typology

- BIPARTITE: alerts=61, positives=61, top10_recall=0.8361, top10_precision=1.0000
- CYCLE: alerts=97, positives=97, top10_recall=0.8557, top10_precision=1.0000
- FAN-IN: alerts=131, positives=131, top10_recall=0.9008, top10_precision=1.0000
- FAN-OUT: alerts=47, positives=47, top10_recall=1.0000, top10_precision=1.0000
- GATHER-SCATTER: alerts=233, positives=233, top10_recall=0.9142, top10_precision=1.0000
- RANDOM: alerts=80, positives=80, top10_recall=0.9500, top10_precision=1.0000
- SCATTER-GATHER: alerts=171, positives=171, top10_recall=0.8713, top10_precision=1.0000
- STACK: alerts=130, positives=130, top10_recall=0.7615, top10_precision=1.0000

### Alert Size Bucket

- 1: alerts=113645, positives=453, top10_recall=0.7704, top10_precision=0.0178
- 2-3: alerts=68126, positives=548, top10_recall=0.9197, top10_precision=0.1857
- 4-6: alerts=50889, positives=165, top10_recall=0.8970, top10_precision=0.0966
- 7+: alerts=28895, positives=139, top10_recall=0.7482, top10_precision=0.0460

### Amount Bucket

- q1_low: alerts=65389, positives=22, top10_recall=0.6818, top10_precision=0.0076
- q2_mid_low: alerts=65389, positives=87, top10_recall=0.7931, top10_precision=0.0110
- q3_mid_high: alerts=65388, positives=400, top10_recall=0.8325, top10_precision=0.0273
- q4_high: alerts=65389, positives=796, top10_recall=0.8643, top10_precision=0.1209

### Currency Complexity

- mixed_currency: alerts=6249, positives=630, top10_recall=0.9524, top10_precision=0.2419
- single_currency: alerts=255306, positives=675, top10_recall=0.7481, top10_precision=0.0213

### Cross Bank Breadth

- 1_bank: alerts=194034, positives=503, top10_recall=0.7654, top10_precision=0.0191
- 2_3_banks: alerts=54969, positives=630, top10_recall=0.9079, top10_precision=0.1435
- 4plus_banks: alerts=12552, positives=172, top10_recall=0.8605, top10_precision=0.0728

## Product Realism Check

- Champion top-10%% positive rate: `0.0422`
- Amount top-10%% positive rate: `0.0104`
- Champion top-10%% uplift: `8.47`x base rate
- Amount top-10%% uplift: `2.09`x base rate

Champion-only top-ranked samples:

- IBMHI-1C79E943267A: label=1, total_amount_usd=1635005184.00, tx_count=1728, breadth=864, top_contributions=unique_destination_accounts(116.31), log_unique_destination_accounts(85.05), transaction_count(69.87)
- IBMHI-A1BE965EB77D: label=0, total_amount_usd=40588460.00, tx_count=561, breadth=292, top_contributions=log_unique_destination_accounts(69.34), unique_destination_accounts(39.15), amount_std_to_mean_usd(31.10)
- IBMHI-51B4423B855E: label=1, total_amount_usd=36677.24, tx_count=6, breadth=5, top_contributions=log_unique_destination_accounts(12.89), log_total_amount_usd(4.28), ach_ratio(3.11)
- IBMHI-127905D8830C: label=1, total_amount_usd=56889.96, tx_count=6, breadth=5, top_contributions=log_unique_destination_accounts(12.89), log_total_amount_usd(5.20), ach_ratio(3.85)
- IBMHI-1B571160C206: label=1, total_amount_usd=89341.12, tx_count=7, breadth=6, top_contributions=log_unique_destination_accounts(15.12), log_total_amount_usd(6.14), ach_ratio(3.85)

Amount-only top-ranked samples:

- IBMHI-8BA817ECEBFB: label=0, total_amount_usd=21436352512.00, tx_count=14, breadth=6, dominant_payment_format=cheque
- IBMHI-7B0CCE395E35: label=0, total_amount_usd=4421294080.00, tx_count=6, breadth=2, dominant_payment_format=ach
- IBMHI-A56FD846540B: label=0, total_amount_usd=1580279168.00, tx_count=4, breadth=3, dominant_payment_format=credit_card
- IBMHI-D7C6C49EF9E0: label=0, total_amount_usd=1424088576.00, tx_count=8, breadth=2, dominant_payment_format=ach
- IBMHI-3E311C2AD871: label=0, total_amount_usd=1174577280.00, tx_count=15, breadth=4, dominant_payment_format=ach

## HI / LI Generalization

- Status: `unavailable`
- Detail: LI-Small files are not present in the local IBM dataset directory; cross-illicit-ratio transfer could not be run in this repo-only pass.

## Verdict

- Trustworthiness verdict: `conditionally_trustworthy_under_narrow_synthetic_assumptions`
- Internal citation verdict: `allowed_with_full_sanity-report caveats`
- External citation verdict: `not_recommended_without_strong_synthetic-only caveat and no customer-performance claims`
- The primary raw-signal champion remains far above the amount baseline under the default chronological split.
- Shuffled-label control collapses performance and argues against an implementation bug that directly leaks labels through the supervision path.
- There is still benchmark convenience: the proxy label is defined on the same grouped transaction window that also determines the strongest ranking features.
- Pattern-derived exact-match information remains a leakage-like convenience signal and should stay flagged even when its marginal impact is small.
- The primary score is strong enough for internal synthetic benchmarking discussions, but not strong enough to cite externally as evidence of real bank AML ranking performance.
