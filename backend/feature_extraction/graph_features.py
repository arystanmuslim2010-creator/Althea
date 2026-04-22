from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


GRAPH_FEATURE_COLUMNS = [
    "graph_hist_out_degree",
    "graph_hist_in_degree",
    "graph_hist_unique_banks",
    "graph_hist_counterparty_concentration",
    "graph_hist_top_counterparty_share",
    "graph_new_counterparty_share",
    "graph_seen_counterparty_share",
    "graph_reactivated_counterparty_share",
    "graph_repeated_interaction_intensity",
    "graph_reciprocal_counterparty_share",
    "graph_fan_in_indicator",
    "graph_fan_out_indicator",
    "graph_scatter_ratio",
    "graph_gather_ratio",
    "graph_relay_candidate_share",
    "graph_recent_edge_reuse_24h",
    "graph_recent_path_count_24h",
]


@dataclass(slots=True)
class _GraphTransaction:
    alert_id: str
    created_at: datetime
    source_account_key: str
    source_bank: str
    destination_account_key: str
    destination_bank: str


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _parse_iso_utc(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def _iter_alert_rows(alert_jsonl_path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(alert_jsonl_path).open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            rows.append(payload)
    rows.sort(key=lambda item: (_parse_iso_utc(item.get("created_at")), str(item.get("alert_id") or "")))
    return rows


def extract_graph_feature_csv_from_alert_jsonl(
    alert_jsonl_path: str | Path,
    output_csv_path: str | Path,
    *,
    force_rebuild: bool = False,
) -> tuple[Path, dict[str, Any]]:
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path, {"reused_existing_graph_feature_csv": True}

    alerts = _iter_alert_rows(alert_jsonl_path)
    rows_written = 0
    global_out_neighbors: defaultdict[str, Counter[str]] = defaultdict(Counter)
    global_in_neighbors: defaultdict[str, Counter[str]] = defaultdict(Counter)
    global_bank_partners: defaultdict[str, Counter[str]] = defaultdict(Counter)
    last_seen_edge: dict[tuple[str, str], datetime] = {}
    recent_out_edges: defaultdict[str, list[tuple[datetime, str]]] = defaultdict(list)
    recent_in_edges: defaultdict[str, list[tuple[datetime, str]]] = defaultdict(list)

    _ensure_parent_dir(output_path)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["alert_id", *GRAPH_FEATURE_COLUMNS])
        writer.writeheader()
        for alert in alerts:
            alert_id = str(alert.get("alert_id") or "")
            created_at = _parse_iso_utc(alert.get("created_at"))
            source_account_key = str(alert.get("source_account_key") or "")
            transactions = alert.get("transactions") or []
            current_counterparties = [
                str(tx.get("receiver") or tx.get("destination_account_key") or "")
                for tx in transactions
            ]
            current_counterparties = [value for value in current_counterparties if value]
            current_banks = [
                str(((tx.get("optional_fields") or {}).get("to_bank")) or "")
                for tx in transactions
            ]
            current_banks = [value for value in current_banks if value]
            prior_out = global_out_neighbors[source_account_key]
            prior_in = global_in_neighbors[source_account_key]
            prior_banks = global_bank_partners[source_account_key]
            total_prior_out = float(sum(prior_out.values()))
            top_counterparty_count = float(max(prior_out.values())) if prior_out else 0.0
            seen_counterparties = [counterparty for counterparty in current_counterparties if prior_out[counterparty] > 0]
            new_counterparties = [counterparty for counterparty in current_counterparties if prior_out[counterparty] <= 0]
            reactivated_counterparties = [
                counterparty
                for counterparty in current_counterparties
                if prior_out[counterparty] > 0
                and (created_at - last_seen_edge.get((source_account_key, counterparty), created_at)).total_seconds() / 3600.0 > 24.0
            ]
            reciprocal_counterparties = [
                counterparty for counterparty in current_counterparties if global_out_neighbors[counterparty][source_account_key] > 0
            ]
            relay_counterparties = [
                counterparty for counterparty in current_counterparties if len(global_out_neighbors[counterparty]) > 0
            ]

            cutoff = created_at - timedelta(hours=24)
            recent_out_edges[source_account_key] = [
                item for item in recent_out_edges[source_account_key] if item[0] >= cutoff
            ]
            recent_in_edges[source_account_key] = [
                item for item in recent_in_edges[source_account_key] if item[0] >= cutoff
            ]
            recent_edge_reuse = sum(1 for _, counterparty in recent_out_edges[source_account_key] if counterparty in current_counterparties)
            recent_path_count = sum(
                1
                for counterparty in current_counterparties
                if any(source_account_key != src for _, src in recent_in_edges[counterparty] if _ >= cutoff)
            )
            record = {
                "alert_id": alert_id,
                "graph_hist_out_degree": float(len(prior_out)),
                "graph_hist_in_degree": float(len(prior_in)),
                "graph_hist_unique_banks": float(len(prior_banks)),
                "graph_hist_counterparty_concentration": float(
                    sum((count / total_prior_out) ** 2 for count in prior_out.values()) if total_prior_out > 0 else 0.0
                ),
                "graph_hist_top_counterparty_share": _safe_ratio(top_counterparty_count, total_prior_out),
                "graph_new_counterparty_share": _safe_ratio(float(len(new_counterparties)), float(len(current_counterparties))),
                "graph_seen_counterparty_share": _safe_ratio(float(len(seen_counterparties)), float(len(current_counterparties))),
                "graph_reactivated_counterparty_share": _safe_ratio(float(len(reactivated_counterparties)), float(len(current_counterparties))),
                "graph_repeated_interaction_intensity": _safe_ratio(
                    float(sum(prior_out[counterparty] for counterparty in current_counterparties)),
                    float(len(current_counterparties)),
                ),
                "graph_reciprocal_counterparty_share": _safe_ratio(
                    float(len(reciprocal_counterparties)),
                    float(len(current_counterparties)),
                ),
                "graph_fan_in_indicator": float(len(prior_in)),
                "graph_fan_out_indicator": float(len(set(current_counterparties))),
                "graph_scatter_ratio": _safe_ratio(float(len(set(current_counterparties))), float(len(current_counterparties))),
                "graph_gather_ratio": _safe_ratio(float(len(prior_in)), float(len(prior_in) + len(prior_out))),
                "graph_relay_candidate_share": _safe_ratio(float(len(relay_counterparties)), float(len(current_counterparties))),
                "graph_recent_edge_reuse_24h": float(recent_edge_reuse),
                "graph_recent_path_count_24h": float(recent_path_count),
            }
            writer.writerow(record)
            rows_written += 1

            for tx in transactions:
                destination_account_key = str(tx.get("receiver") or tx.get("destination_account_key") or "")
                destination_bank = str(((tx.get("optional_fields") or {}).get("to_bank")) or "")
                source_bank = str(((tx.get("optional_fields") or {}).get("from_bank")) or "")
                if not destination_account_key:
                    continue
                global_out_neighbors[source_account_key][destination_account_key] += 1
                global_in_neighbors[destination_account_key][source_account_key] += 1
                if destination_bank:
                    global_bank_partners[source_account_key][destination_bank] += 1
                if source_bank:
                    global_bank_partners[destination_account_key][source_bank] += 1
                last_seen_edge[(source_account_key, destination_account_key)] = created_at
                recent_out_edges[source_account_key].append((created_at, destination_account_key))
                recent_in_edges[destination_account_key].append((created_at, source_account_key))

    return output_path, {
        "reused_existing_graph_feature_csv": False,
        "rows_written": int(rows_written),
        "feature_columns": list(GRAPH_FEATURE_COLUMNS),
    }


def load_graph_feature_frame(base_frame: pd.DataFrame, graph_feature_csv_path: str | Path) -> pd.DataFrame:
    graph_frame = pd.read_csv(graph_feature_csv_path)
    graph_frame["alert_id"] = graph_frame["alert_id"].astype(str)
    for column in GRAPH_FEATURE_COLUMNS:
        graph_frame[column] = pd.to_numeric(graph_frame[column], errors="coerce").fillna(0.0).astype(np.float32)
    merged = base_frame.merge(graph_frame, on="alert_id", how="left", validate="one_to_one")
    for column in GRAPH_FEATURE_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0.0)
    return merged
