import json
from pathlib import Path

from lakehouse.quality.gates import (
    QualityGateFailed,
    build_quality_gate_report,
    emit_quality_alert,
    enforce_quality_gate,
    format_quality_gate_summary,
)
from tests.support import build_test_settings


def test_build_quality_gate_report_uses_latest_batch_and_summarizes_failures():
    settings = build_test_settings("test_quality_gate_behavior_latest")
    settings.ensure_directories()
    report_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "20260317T120000Z.json").write_text(
        json.dumps([{"rule_name": "old_rule", "status": "failed", "failed_records": 1}]),
        encoding="utf-8",
    )
    (report_dir / "20260318T120000Z.json").write_text(
        json.dumps(
            [
                {"rule_name": "not_null_email", "dataset_name": "silver.customers", "status": "failed", "failed_records": 2},
                {"rule_name": "positive_quantity", "dataset_name": "silver.order_items", "status": "passed", "failed_records": 0},
            ]
        ),
        encoding="utf-8",
    )

    report = build_quality_gate_report(settings)

    assert report.batch_id == "20260318T120000Z"
    assert report.total_rules == 2
    assert report.failed_rules == 1
    assert report.failed_records == 2
    assert report.failed_results[0]["rule_name"] == "not_null_email"


def test_enforce_quality_gate_raises_with_actionable_summary_when_failures_exist():
    settings = build_test_settings("test_quality_gate_failure")
    settings.ensure_directories()
    report_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "20260318T130000Z.json").write_text(
        json.dumps(
            [
                {
                    "dataset_name": "silver.orders",
                    "rule_name": "order_customer_exists",
                    "status": "failed",
                    "failed_records": 3,
                }
            ]
        ),
        encoding="utf-8",
    )

    report = build_quality_gate_report(settings)

    try:
        enforce_quality_gate(report)
    except QualityGateFailed as exc:
        message = str(exc)
    else:
        raise AssertionError("Era esperado que o quality gate falhasse.")

    assert "20260318T130000Z" in message
    assert "order_customer_exists" in message
    assert "silver.orders" in message


def test_emit_quality_alert_returns_failure_summary_without_raising():
    settings = build_test_settings("test_quality_gate_alert")
    settings.ensure_directories()
    report_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "20260318T140000Z.json").write_text(
        json.dumps(
            [
                {
                    "dataset_name": "silver.customers",
                    "rule_name": "not_null_email",
                    "status": "failed",
                    "failed_records": 1,
                }
            ]
        ),
        encoding="utf-8",
    )

    message = emit_quality_alert(settings)

    assert "ALERTA" in message
    assert "not_null_email" in message
    assert "20260318T140000Z" in message


def test_format_quality_gate_summary_reports_successful_batches():
    settings = build_test_settings("test_quality_gate_success")
    settings.ensure_directories()
    report_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "20260318T150000Z.json").write_text(
        json.dumps(
            [
                {
                    "dataset_name": "silver.customers",
                    "rule_name": "not_null_email",
                    "status": "passed",
                    "failed_records": 0,
                }
            ]
        ),
        encoding="utf-8",
    )

    report = build_quality_gate_report(settings)

    assert format_quality_gate_summary(report) == "Quality gate aprovado para o batch 20260318T150000Z. Regras avaliadas: 1."
