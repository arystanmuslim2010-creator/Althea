from __future__ import annotations

from services.enrichment_connectors.base import BaseEnrichmentConnector


class DeviceConnector(BaseEnrichmentConnector):
    source_name = "device"
    env_prefix = "ALTHEA_DEVICE"
