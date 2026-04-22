from __future__ import annotations

from services.enrichment_connectors.base import BaseEnrichmentConnector


class KYCConnector(BaseEnrichmentConnector):
    source_name = "kyc"
    env_prefix = "ALTHEA_KYC"
