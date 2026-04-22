# ALTHEA IBM AML Benchmark Protocol B v3

This run keeps the strict Protocol B benchmark unchanged and evaluates a richer past-only stack.

## Final Outcome

- Validation-safe champion: `protocol_b_v3_fused_sparse_logistic`
- Champion test Recall@Top 10%: `0.5522`
- Reference test Recall@Top 10%: `0.5403`
- Improvement vs reference: `+0.0119`

## Strongest Component

- Strongest ablation drop: `ablation_remove_horizon_layer`
- Delta Recall@Top 10% vs fused sparse candidate: `+0.0000`

## Benchmark Safety

- Past-only features: `True`
- Future-only labels: `True`
- Chronological split: `True`
- Pattern shortcuts in primary score: `False`
- Test-based model selection: `False`
