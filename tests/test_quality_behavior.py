from datetime import datetime, timezone

from lakehouse.quality.rules import validate_not_null, validate_unique


def test_validate_not_null_reports_failed_records_and_failure_rate():
    result = validate_not_null(
        records=[
            {"customer_id": 10, "email": "ana@example.com"},
            {"customer_id": 20, "email": None},
        ],
        field_name="email",
        dataset_name="silver.customers",
        layer="silver",
        pipeline_run_id="quality-test",
        execution_timestamp=datetime(2026, 3, 17, 15, 0, tzinfo=timezone.utc),
    )

    assert result.status == "failed"
    assert result.failed_records == 1
    assert result.total_records == 2
    assert result.failure_rate == 0.5
    assert result.rule_name == "not_null_email"


def test_validate_unique_passes_when_dataset_is_empty():
    result = validate_unique(
        records=[],
        field_names=["customer_id"],
        dataset_name="silver.customers",
        layer="silver",
        pipeline_run_id="quality-test",
    )

    assert result.status == "passed"
    assert result.failed_records == 0
    assert result.total_records == 0
    assert result.failure_rate == 0.0
