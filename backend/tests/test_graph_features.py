from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from feature_extraction.graph_features import (
    GRAPH_FEATURE_COLUMNS,
    extract_graph_feature_csv_from_alert_jsonl,
    load_graph_feature_frame,
)


def _write_alerts(path: Path) -> Path:
    alerts = [
        {
            "alert_id": "A1",
            "created_at": "2022-01-01T00:00:00Z",
            "source_account_key": "acct-1",
            "transactions": [
                {"receiver": "acct-2", "optional_fields": {"from_bank": "b1", "to_bank": "b2"}},
                {"receiver": "acct-3", "optional_fields": {"from_bank": "b1", "to_bank": "b3"}},
            ],
        },
        {
            "alert_id": "A2",
            "created_at": "2022-01-01T02:00:00Z",
            "source_account_key": "acct-2",
            "transactions": [
                {"receiver": "acct-1", "optional_fields": {"from_bank": "b2", "to_bank": "b1"}},
            ],
        },
        {
            "alert_id": "A3",
            "created_at": "2022-01-02T03:00:00Z",
            "source_account_key": "acct-1",
            "transactions": [
                {"receiver": "acct-2", "optional_fields": {"from_bank": "b1", "to_bank": "b2"}},
                {"receiver": "acct-4", "optional_fields": {"from_bank": "b1", "to_bank": "b4"}},
            ],
        },
    ]
    path.write_text("\n".join(json.dumps(item) for item in alerts) + "\n", encoding="utf-8")
    return path


def test_graph_features_are_past_only(tmp_path: Path) -> None:
    alert_path = _write_alerts(tmp_path / "alerts.jsonl")
    graph_path, summary = extract_graph_feature_csv_from_alert_jsonl(alert_path, tmp_path / "graph.csv", force_rebuild=True)
    frame = pd.read_csv(graph_path)
    first = frame.loc[frame["alert_id"] == "A1"].iloc[0]
    third = frame.loc[frame["alert_id"] == "A3"].iloc[0]

    assert summary["rows_written"] == 3
    assert first["graph_hist_out_degree"] == 0.0
    assert third["graph_seen_counterparty_share"] > 0.0
    assert third["graph_reciprocal_counterparty_share"] > 0.0


def test_graph_feature_merge(tmp_path: Path) -> None:
    alert_path = _write_alerts(tmp_path / "alerts.jsonl")
    graph_path, _ = extract_graph_feature_csv_from_alert_jsonl(alert_path, tmp_path / "graph.csv", force_rebuild=True)
    base = pd.DataFrame(
        [
            {"alert_id": "A1", "created_at": "2022-01-01T00:00:00Z"},
            {"alert_id": "A2", "created_at": "2022-01-01T02:00:00Z"},
            {"alert_id": "A3", "created_at": "2022-01-02T03:00:00Z"},
        ]
    )
    merged = load_graph_feature_frame(base, graph_path)
    assert set(GRAPH_FEATURE_COLUMNS).issubset(merged.columns)
