from __future__ import annotations

import json
from pathlib import Path

from benchmarks.ibm_aml_data import convert_transactions_to_alert_jsonl, run_benchmark


def _write_transactions_csv(path: Path, rows: list[str]) -> Path:
    header = (
        "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
        "Amount Paid,Payment Currency,Payment Format,Is Laundering\n"
    )
    path.write_text(header + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_converter_groups_transactions_into_alerts_with_proxy_labels_and_typology(tmp_path: Path) -> None:
    transactions_path = _write_transactions_csv(
        tmp_path / "HI-Small_Trans.csv",
        [
            "2022/09/01 00:00,010,SRC1,011,DST1,100.00,US Dollar,100.00,US Dollar,ACH,0",
            "2022/09/01 10:00,010,SRC1,012,DST2,200.00,US Dollar,200.00,US Dollar,ACH,1",
            "2022/09/02 00:00,020,SRC2,021,DST3,300.00,Euro,300.00,Euro,Wire,1",
            "2022/09/02 01:30,010,SRC1,013,DST4,50.00,US Dollar,50.00,US Dollar,ACH,0",
            "broken,row",
        ],
    )
    patterns_path = tmp_path / "HI-Small_Patterns.txt"
    patterns_path.write_text(
        "\n".join(
            [
                "BEGIN LAUNDERING ATTEMPT - FAN-OUT",
                "2022/09/01 10:00,010,SRC1,012,DST2,200.00,US Dollar,200.00,US Dollar,ACH,1",
                "END LAUNDERING ATTEMPT - FAN-OUT",
                "",
                "BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 2 hops",
                "2022/09/02 00:00,020,SRC2,021,DST3,300.00,Euro,300.00,Euro,Wire,1",
                "END LAUNDERING ATTEMPT - CYCLE",
            ]
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "alerts.jsonl"

    summary = convert_transactions_to_alert_jsonl(
        transactions_path=transactions_path,
        patterns_path=patterns_path,
        output_path=output_path,
    )

    alerts = _load_jsonl(output_path)
    assert summary["alerts_written"] == 3
    assert summary["invalid_rows"] == 1
    assert len(alerts) == 3

    src1_alerts = sorted(
        [item for item in alerts if item["source_account_key"] == "010:SRC1"],
        key=lambda item: item["created_at"],
    )
    assert len(src1_alerts) == 2
    assert src1_alerts[0]["evaluation_label_is_sar"] == 1
    assert src1_alerts[0]["typology"] == "FAN-OUT"
    assert len(src1_alerts[0]["transactions"]) == 2
    assert src1_alerts[1]["evaluation_label_is_sar"] == 0
    assert src1_alerts[1]["typology"] == "unknown"
    assert len(src1_alerts[1]["transactions"]) == 1

    src2_alert = next(item for item in alerts if item["source_account_key"] == "020:SRC2")
    assert src2_alert["evaluation_label_is_sar"] == 1
    assert src2_alert["typology"] == "CYCLE"


def test_converter_uses_li_dataset_prefix_for_alert_ids(tmp_path: Path) -> None:
    transactions_path = _write_transactions_csv(
        tmp_path / "LI-Small_Trans.csv",
        [
            "2022/09/01 00:00,010,SRC1,011,DST1,100.00,US Dollar,100.00,US Dollar,ACH,0",
        ],
    )
    patterns_path = tmp_path / "LI-Small_patterns.txt"
    patterns_path.write_text("", encoding="utf-8")
    output_path = tmp_path / "li_alerts.jsonl"

    convert_transactions_to_alert_jsonl(
        transactions_path=transactions_path,
        patterns_path=patterns_path,
        output_path=output_path,
        dataset_name="IBM AML-Data LI-Small",
    )

    alerts = _load_jsonl(output_path)
    assert len(alerts) == 1
    assert alerts[0]["alert_id"].startswith("IBMLI-")


def test_benchmark_runner_computes_chronological_and_amount_baselines(tmp_path: Path) -> None:
    alert_path = tmp_path / "alerts.jsonl"
    alerts: list[dict] = []
    for index in range(10):
        alerts.append(
            {
                "alert_id": f"A{index + 1}",
                "created_at": f"2022-09-{index + 1:02d}T00:00:00Z",
                "source_system": "ibm_amlsim",
                "source_account_key": f"SRC-{index + 1}",
                "typology": "unknown",
                "evaluation_label_is_sar": 0,
                "accounts": [{"account_id": f"SRC-{index + 1}"}],
                "transactions": [
                    {
                        "transaction_id": f"TX-{index + 1}",
                        "amount": float(index + 1),
                        "timestamp": f"2022-09-{index + 1:02d}T00:00:00Z",
                        "sender": f"SRC-{index + 1}",
                        "receiver": f"DST-{index + 1}",
                    }
                ],
                "metadata": {"source_system": "ibm_amlsim"},
            }
        )

    # Test split will contain A9 and A10. Make only A10 positive and highest amount.
    alerts[8]["transactions"][0]["amount"] = 5.0
    alerts[9]["transactions"][0]["amount"] = 5000.0
    alerts[9]["evaluation_label_is_sar"] = 1

    alert_path.write_text("\n".join(json.dumps(item, ensure_ascii=True) for item in alerts) + "\n", encoding="utf-8")

    summary_path = tmp_path / "benchmark.json"
    report_path = tmp_path / "benchmark.md"
    result = run_benchmark(
        alert_jsonl_path=alert_path,
        report_path=report_path,
        summary_path=summary_path,
        include_althea_baseline=False,
    )

    assert result.dataset_stats["total_alerts"] == 10
    assert result.split_stats["train"]["total_alerts"] == 6
    assert result.split_stats["validation"]["total_alerts"] == 2
    assert result.split_stats["test"]["total_alerts"] == 2
    assert result.althea_baseline_status["status"] == "skipped"
    assert report_path.exists()
    assert summary_path.exists()

    chronological = result.ranking_metrics["test"]["chronological_queue"]
    amount = result.ranking_metrics["test"]["amount_descending"]
    assert chronological["precision_at_top_20pct"] == 0.0
    assert amount["precision_at_top_20pct"] == 1.0
