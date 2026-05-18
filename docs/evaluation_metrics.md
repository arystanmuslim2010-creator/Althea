# Evaluation Metrics

## Why Accuracy Is Not Emphasized

Alert-ranking systems are not judged mainly by overall accuracy. In AML investigation workflows, analysts care about whether the most suspicious alerts rise to the top of the queue so limited review capacity is spent well.

For that reason ALTHEA emphasizes ranking metrics rather than plain accuracy.

## Recall@TopK

`Recall@TopK` measures how many labeled suspicious alerts are captured within the top portion of the ranked queue.

Examples:

- `Recall@Top10%`
- `Recall@Top20%`
- `Recall@Top30%`

Interpretation:

- high recall at low queue depth means analysts reach more important alerts earlier

## Precision@TopK

`Precision@TopK` measures how many alerts in the top portion of the queue are labeled suspicious.

Examples:

- `Precision@Top10%`
- `Precision@Top20%`

Interpretation:

- higher precision means the front of the queue contains a denser concentration of useful analyst work

## SAR Capture

In ALTHEA evaluation, `SAR capture` is the share of labeled suspicious alerts captured within a defined queue segment.

Examples:

- SAR capture at top 10%
- SAR capture at top 20%
- SAR capture at top 30%

If the evaluation label represents suspicious or SAR-worthy alerts, SAR capture is operationally equivalent to recall over that label set.

## Workload Reduction

`Workload reduction at target recall` measures how much of the queue can be deferred while still achieving a target recall level.

Interpretation:

- if ALTHEA reaches 80% recall after reviewing 40% of alerts, workload reduction is 60%

This is useful for pilot discussions because it connects model ranking quality to analyst capacity.

## PR-AUC

`PR-AUC` is the area under the precision-recall curve.

This is useful when positive alerts are rare, which is common in AML settings. It is more informative than ROC-focused summaries when class imbalance is strong.

ALTHEA only reports `PR-AUC` when both positive and negative labels are present.

## Lift Over Baseline

`Lift over baseline` compares ALTHEA against a simpler ranking strategy.

Examples:

- lift over random baseline
- lift over best simple baseline

Interpretation:

- `1.90x` lift means ALTHEA captures suspicious alerts 1.9 times as effectively at the selected queue depth

## Baselines Used

ALTHEA compares its ranking against:

- chronological ordering
- amount-descending ordering
- random ordering
- simple heuristic ordering when the needed fields exist

These baselines are intentionally simple. The point is to test whether ALTHEA produces a materially better investigation queue than naive alternatives.

## Invalid Evaluation Datasets

ALTHEA does not produce misleading ranking metrics when:

- labels are missing
- all labels are positive
- all labels are negative

In those cases the system returns:

`Evaluation requires both positive and negative labeled alerts.`
