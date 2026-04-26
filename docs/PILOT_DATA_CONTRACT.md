# ALTHEA Pilot Data Contract

ALTHEA is a read-only investigation support layer. It consumes bank-generated AML alerts and limited contextual data for prioritization evaluation. It does not replace transaction monitoring, rule engines, case-management decisions, or SAR/STR filing judgment.

## Required Batch Shape

`PilotBatch`
- `tenant_id`
- `batch_id`
- `alerts`: list of `PilotAlertRecord`
- `transactions`: list of `PilotTransactionRecord`
- `outcomes`: optional list of `PilotOutcomeRecord`

`PilotAlertRecord`
- `alert_id`
- `alert_timestamp`
- Optional: `customer_id_hash`, `account_id_hash`, `scenario_name`, `original_risk_score`, `alert_status`, `rule_name`, `source_system`

`PilotTransactionRecord`
- `transaction_id`
- `timestamp`
- `amount`
- `currency`
- Optional hashed account/counterparty fields, direction, channel, country

`PilotOutcomeRecord`
- `alert_id`
- Optional: outcome/disposition, false-positive flag, escalation flag, SAR/STR filed flag, decision timestamp, closure category

## Anonymization Expectations

Provide hashed or tokenized IDs only. Do not send names, passport numbers, IIN/SSN/tax IDs, raw documents, phone numbers, email addresses, residential addresses, or full customer profiles.

Minimum lookback is 30 days of alert and transaction context. A 60-90 day lookback is preferred for stable behavior and peer-history features.

## Use Limitation

Pilot data is used to evaluate prioritization, explanation quality, analyst workflow fit, and governance readiness. Any ROI or workload reduction figures remain modeled until validated in a live bank pilot with agreed measurement controls.
