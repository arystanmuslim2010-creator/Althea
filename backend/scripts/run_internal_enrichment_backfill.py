from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[1]


if str(_backend_root()) not in sys.path:
    sys.path.insert(0, str(_backend_root()))


from core.dependencies import get_enrichment_health_service, get_enrichment_sync_service, get_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed canonical enrichment tables from internal ALTHEA case/outcome history.",
    )
    parser.add_argument(
        "--tenant-id",
        default="",
        help="Tenant to backfill. Defaults to ALTHEA_DEFAULT_TENANT_ID when omitted.",
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        default=["case_actions", "alert_outcomes"],
        choices=["case_actions", "alert_outcomes"],
        help="Internal enrichment targets to backfill into canonical tables.",
    )
    parser.add_argument(
        "--actor-id",
        default="cli-backfill",
        help="Audit actor id recorded for the backfill run.",
    )
    parser.add_argument(
        "--skip-health-rebuild",
        action="store_true",
        help="Skip post-backfill health/coverage rebuild.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = get_settings()
    tenant_id = str(args.tenant_id or settings.default_tenant_id).strip()
    if not tenant_id:
        parser.error("tenant-id is required either via --tenant-id or ALTHEA_DEFAULT_TENANT_ID")

    sync_service = get_enrichment_sync_service()
    health_service = get_enrichment_health_service()
    result = sync_service.backfill_internal_targets(
        tenant_id=tenant_id,
        targets=list(args.targets),
        actor_id=str(args.actor_id or "cli-backfill"),
    )
    if not args.skip_health_rebuild:
        result["health"] = health_service.rebuild(tenant_id=tenant_id)
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
