from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_timestamp(value: Any) -> float:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return 0.0
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _heuristic_score(record: dict[str, Any]) -> float:
    score = 0.0
    amount = _safe_float(record.get("amount"), default=0.0)
    if amount >= 10000:
        score += 0.35
    elif amount >= 3000:
        score += 0.2

    typology = str(record.get("typology") or "").strip().lower()
    if typology in {"structuring", "flow_through", "cross_border", "sanctions"}:
        score += 0.2

    fan_in = _safe_float(record.get("fan_in_ratio"), default=0.0)
    fan_out = _safe_float(record.get("fan_out_ratio"), default=0.0)
    if fan_in >= 2.0:
        score += 0.15
    if fan_out >= 2.0:
        score += 0.15

    if bool(record.get("has_incoming_and_outgoing_sequence")):
        score += 0.15

    concentration = _safe_float(record.get("counterparty_concentration"), default=0.0)
    if concentration >= 0.7:
        score += 0.15
    return round(min(score, 1.0), 4)


def build_baseline_rankings(
    records: list[dict[str, Any]],
    *,
    althea_score_field: str,
    dataset_name: str,
) -> dict[str, list[dict[str, Any]]]:
    base_rows = [dict(row or {}) for row in records]
    random_seed = int(hashlib.sha256(dataset_name.encode("utf-8")).hexdigest()[:8], 16)
    rng = random.Random(random_seed)

    indexed = []
    for index, row in enumerate(base_rows):
        enriched = dict(row)
        enriched["_source_index"] = index
        indexed.append(enriched)

    random_rows = list(indexed)
    rng.shuffle(random_rows)

    ranked = {
        "althea": sorted(
            indexed,
            key=lambda row: (_safe_float(row.get(althea_score_field)), -int(row["_source_index"])),
            reverse=True,
        ),
        "chronological": sorted(
            indexed,
            key=lambda row: (_parse_timestamp(row.get("created_at") or row.get("timestamp")), -int(row["_source_index"])),
            reverse=True,
        ),
        "amount_desc": sorted(
            indexed,
            key=lambda row: (_safe_float(row.get("amount")), -int(row["_source_index"])),
            reverse=True,
        ),
        "random": random_rows,
    }

    heuristic_available = any(
        any(field in row for field in ("amount", "typology", "fan_in_ratio", "fan_out_ratio", "counterparty_concentration"))
        for row in indexed
    )
    if heuristic_available:
        ranked["heuristic"] = sorted(
            indexed,
            key=lambda row: (_heuristic_score(row), -int(row["_source_index"])),
            reverse=True,
        )

    result: dict[str, list[dict[str, Any]]] = {}
    for name, rows in ranked.items():
        decorated: list[dict[str, Any]] = []
        for order, row in enumerate(rows, start=1):
            ranking_score = {
                "althea": _safe_float(row.get(althea_score_field)),
                "chronological": _parse_timestamp(row.get("created_at") or row.get("timestamp")),
                "amount_desc": _safe_float(row.get("amount")),
                "random": float(len(rows) - order),
                "heuristic": _heuristic_score(row),
            }.get(name, 0.0)
            cleaned = {key: value for key, value in row.items() if key != "_source_index"}
            cleaned["ranking_score"] = ranking_score
            cleaned["priority_rank"] = order
            decorated.append(cleaned)
        result[name] = decorated
    return result
