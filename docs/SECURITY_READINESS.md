# Security Readiness

ALTHEA is designed as a read-only AI investigation layer for AML teams. It ingests bank-generated alerts and contextual data, ranks investigative priority, explains risk factors, and supports human analysts. It does not replace core banking, transaction monitoring, AML rule engines, or authorized SAR/STR filing decisions.

## Current Controls

- Tenant isolation is enforced through authenticated tenant context and repository queries.
- Alert access is object-level: analysts see assigned alerts, accessible case-linked alerts, or team alerts when they have team-queue permission.
- Managers/admins may see broader tenant queues.
- User DTOs are sanitized before API return; password hashes, tokens, secrets, and refresh tokens are excluded.
- Governance and training metadata is restricted to manager/governance/admin views, with write operations limited to governance/admin permissions.
- Public error responses avoid raw internal exceptions outside demo mode.
- Runtime modes separate demo, pilot, and production behavior.
- Model artifact loading supports optional SHA256 integrity verification before deserialization.

## Roles

- `analyst`: assigned-alert and case workflow access.
- `investigator`: assigned/team queue investigation workflows.
- `manager`: broader queue visibility and safe governance summaries.
- `governance`: model-governance detail and write workflows.
- `admin`: tenant administrative superuser.

## Known Limitations

- Bank-specific SSO, SIEM, DLP, key management, and infrastructure hardening still require deployment review.
- Model artifact SHA256 is optional unless the registry stores `artifact_sha256`; production onboarding should require it.
- Pilot metrics are modeled/observed pilot metrics, not production savings claims.
- Benchmark evidence is internal and synthetic unless explicitly validated by a bank pilot.

Status: pilot-ready after bank-specific integration and security review; not production certified.

## Pre-Production Checklist

- Enforce PostgreSQL RLS in target environment.
- Require signed or hash-verified model artifacts.
- Validate SSO/RBAC mappings with the bank.
- Run penetration test and privacy review.
- Confirm logs do not contain raw PII or secrets.
- Confirm SAR/STR workflows remain human decisioning only.
