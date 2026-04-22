from __future__ import annotations

from services.enrichment_connectors.base import BaseEnrichmentConnector


class ChannelConnector(BaseEnrichmentConnector):
    source_name = "channel"
    env_prefix = "ALTHEA_CHANNEL"
