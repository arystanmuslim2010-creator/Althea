# Explainability Contract

## Analyst-Facing Fields

The primary analyst explanation uses AML-native language and returns:

```json
{
  "summary_text": "This alert is prioritized because funds appear to be received from multiple counterparties and moved onward rapidly, which is consistent with a possible layering pattern.",
  "key_risk_drivers": [
    "Multiple incoming transfers from different counterparties",
    "Rapid outgoing movement after receipt of funds",
    "Transaction pattern differs from expected account behavior"
  ],
  "aml_patterns": [
    "Fan-in",
    "Rapid fund movement",
    "Potential layering"
  ],
  "analyst_next_steps": [
    "Review whether incoming funds were redistributed quickly",
    "Check whether counterparties are connected or repeated",
    "Compare the activity against the customer's expected profile"
  ],
  "confidence_level": "Medium"
}
```

These fields are intended for analysts, investigators, pilot reviewers, and workflow screens.

## Technical Details Fields

The system also preserves technical payloads under:

- `technical_details`
- `risk_explanation`
- `human_explanation`
- technical or admin detail sections in structured responses

These may include:

- raw feature values
- normalized contributions
- model version
- explanation method
- explanation status
- raw explainability payloads
- score metadata

These fields are secondary. They are not the primary explanation contract for analysts.

## Compliance-Safe Wording

Allowed wording:

- `may indicate`
- `is consistent with`
- `suggests`
- `warrants review`

Disallowed primary wording:

- `confirmed laundering`
- `money laundering confirmed`
- automatic statements that a SAR must be filed

ALTHEA supports investigation. It does not make a final compliance determination.

## Signal Mapping

The interpretation layer maps technical signals into AML concepts such as:

- `fan_in_ratio` or many senders -> `Fan-in`
- `fan_out_ratio` or many receivers -> `Fan-out`
- short time gaps or high velocity -> `Rapid fund movement`
- repeated small amounts -> `Possible structuring`
- incoming then outgoing sequence -> `Potential layering`
- high counterparty concentration -> `Counterparty concentration risk`
- deviation from baseline -> `Activity differs from expected account behavior`
- cycles or circular transfers -> `Circular fund movement`

## Good Explanation Example

Good:

`This alert is prioritized because funds appear to be received from multiple counterparties and moved onward rapidly, which is consistent with a potential layering pattern and warrants review.`

Why it is good:

- human-readable
- AML-native
- compliance-safe
- explains why the alert is prioritized
- suggests concrete next review actions

## Bad Explanation Example

Bad:

`Top SHAP features: time_gap=-0.82, fan_in_ratio=0.66, amount_log1p=0.44`

Why it is bad:

- technical-first
- not useful as primary analyst wording
- does not explain the AML meaning
- exposes internal model detail without interpretation

## Backward Compatibility

For compatibility, ALTHEA may still return alias fields such as:

- `key_reasons`
- `analyst_focus_points`
- `confidence_score`

New consumers should prefer:

- `key_risk_drivers`
- `analyst_next_steps`
- `confidence_level`
