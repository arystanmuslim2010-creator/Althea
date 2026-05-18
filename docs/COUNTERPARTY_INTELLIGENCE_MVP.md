# Counterparty Intelligence MVP

## What It Does

The Counterparty Intelligence MVP adds bank-data-only counterparty context inside alert investigations. It helps analysts review:

- New vs recurring counterparties in the recent alert window
- Top counterparties by transaction count and volume
- Counterparty concentration
- Shared counterparties across similar alerts
- Links to previous escalated or high-suspicion cases
- Fan-in and fan-out behavior
- Concise linked-pattern signals and an analyst takeaway

This is investigation support for human review. It does not make a final alert outcome, SAR, or STR decision.

## Required Data

The MVP works best when bank-provided alert payloads include:

- `alert_id`, `tenant_id`, `timestamp` or `alert_timestamp`
- `account_id`, `customer_id`, `entity_id`, or equivalent hashed account/customer identifiers
- `transactions` with `transaction_id`, `timestamp`, `amount`, `currency`, `direction`
- Counterparty fields such as `counterparty_id`, `counterparty_account`, `beneficiary_id`, `beneficiary_account`
- Prior alert status, outcome, and priority score
- Case history with status and outcome

When enrichment account events are available, the service also uses them as historical account activity. Missing optional fields produce partial intelligence instead of a hard failure.

## What It Does Not Do

- Not external sanctions, PEP, adverse media, or ownership screening
- Not a beneficial ownership database
- Not external data scraping
- Not a global counterparty intelligence platform
- Not criminal network detection
- Not a SAR/STR decision engine
- Not automated filing or final case disposition

## Safe Wording

Preferred wording:

- Counterparty context
- Linked-pattern signals
- Similar alerts
- Shared counterparties
- New vs recurring counterparties
- Investigation support
- Human review required

Avoid wording that implies automated decisions, confirmed laundering, criminality, or filing requirements.

## Limitations

- Depends on bank-provided hashed IDs and transaction fields
- Counterparty matching quality depends on consistent source identifiers
- Recent vs historical classification uses configurable time windows and may be limited by payload coverage
- Linked escalated cases are based on existing internal outcomes and case labels
- No external sanctions/PEP/adverse media or ownership provider data is included
- Output supports human investigation only

## Roadmap

Phase 1:

- Counterparty context inside alert investigations

Phase 2:

- Counterparty profile pages
- Linked alerts and case history

Phase 3:

- Entity resolution
- Graph risk scoring
- Case clustering

Phase 4:

- External screening integrations
- Sanctions/PEP/adverse media/ownership providers
