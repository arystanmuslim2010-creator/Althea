from __future__ import annotations

import json
from pathlib import Path

from benchmarks.ibm_aml_improvement import extract_feature_csv_from_alert_jsonl
from benchmarks.ibm_aml_li_transfer import run_li_transfer_benchmark


def _write_alert_jsonl(path: Path, alerts: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")
    return path


def _build_alerts(prefix: str, total: int, amount_scale: float) -> list[dict]:
    alerts: list[dict] = []
    for index in range(total):
        is_positive = 1 if index % 4 == 0 else 0
        tx_count = 4 if is_positive else 1
        transactions = []
        for offset in range(tx_count):
            transactions.append(
                {
                    "transaction_id": f"{prefix}-TX-{index}-{offset}",
                    "amount": float((index + 1) * amount_scale * (offset + 1) * (20 if is_positive else 1)),
                    "timestamp": f"2022-09-{(index % 28) + 1:02d}T0{offset}:00:00Z",
                    "sender": f"{prefix}-SRC-{index // 3}",
                    "receiver": f"{prefix}-DST-{index}-{offset}",
                    "currency": "US Dollar" if offset % 2 == 0 else "Euro",
                    "channel": "Wire" if is_positive else "ACH",
                    "optional_fields": {
                        "from_bank": f"{100 + (index % 7)}",
                        "to_bank": f"{300 + offset}",
                        "is_laundering": is_positive if offset == 0 else 0,
                    },
                }
            )
        alerts.append(
            {
                "alert_id": f"{prefix}-A{index:04d}",
                "created_at": f"2022-09-{(index % 28) + 1:02d}T00:00:00Z",
                "source_system": "ibm_amlsim",
                "source_account_key": f"{100 + (index % 7)}:{prefix}-SRC-{index // 3}",
                "typology": "unknown",
                "evaluation_label_is_sar": is_positive,
                "accounts": [{"account_id": f"{100 + (index % 7)}:{prefix}-SRC-{index // 3}"}],
                "transactions": transactions,
                "metadata": {"dataset_name": prefix},
            }
        )
    return alerts


def test_run_li_transfer_benchmark_reuses_cached_artifacts_and_writes_report(tmp_path: Path) -> None:
    hi_alerts = _build_alerts("HI", total=80, amount_scale=50.0)
    li_alerts = _build_alerts("LI", total=80, amount_scale=60.0)

    hi_alert_path = _write_alert_jsonl(tmp_path / "hi_alerts.jsonl", hi_alerts)
    li_alert_path = _write_alert_jsonl(tmp_path / "li_alerts.jsonl", li_alerts)
    hi_feature_path = tmp_path / "hi.features.csv"
    li_feature_path = tmp_path / "li.features.csv"
    extract_feature_csv_from_alert_jsonl(hi_alert_path, hi_feature_path, grouping_variant="source_account_24h", force_rebuild=True)
    extract_feature_csv_from_alert_jsonl(li_alert_path, li_feature_path, grouping_variant="source_account_24h", force_rebuild=True)

    hi_summary_path = tmp_path / "benchmark_v2.json"
    hi_summary_path.write_text(
        json.dumps({"champion": {"name": "logistic_regression_raw_signals"}}, ensure_ascii=True),
        encoding="utf-8",
    )

    li_transactions_path = tmp_path / "LI-Small_Trans.csv"
    li_transactions_path.write_text(
        "\n".join(
            [
                "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering",
                "2022/09/01 00:00,010,SRC1,011,DST1,100.00,US Dollar,100.00,US Dollar,ACH,0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    li_patterns_path = tmp_path / "LI-Small_patterns.txt"
    li_patterns_path.write_text("", encoding="utf-8")
    li_accounts_path = tmp_path / "LI-Small_accounts.csv"
    li_accounts_path.write_text("Account,Bank\nSRC1,010\n", encoding="utf-8")

    result = run_li_transfer_benchmark(
        li_transactions_path=li_transactions_path,
        li_patterns_path=li_patterns_path,
        li_accounts_path=li_accounts_path,
        report_path=tmp_path / "benchmark_li_transfer_v1.md",
        summary_path=tmp_path / "benchmark_li_transfer_v1.json",
        hi_summary_path=hi_summary_path,
        hi_feature_path=hi_feature_path,
        li_alert_path=li_alert_path,
        li_feature_path=li_feature_path,
        include_li_native_model=False,
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert result.report_path.exists()
    assert summary["reused_artifacts"]["li_alerts_reused"] is True
    assert summary["reused_artifacts"]["li_features_reused"] is True
    assert summary["feature_schema_match"]["exact_match"] is True
    assert summary["hi_transfer_result"]["test_metrics"]["recall_at_top_10pct"] >= 0.0
