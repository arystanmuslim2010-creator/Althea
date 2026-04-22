from __future__ import annotations

from services.enrichment_connectors.base import BaseEnrichmentConnector


class WatchlistConnector(BaseEnrichmentConnector):
    source_name = "watchlist"
    env_prefix = "ALTHEA_WATCHLIST"
