import json

from lakehouse.silver.sales import SilverBuildResult
from lakehouse.silver.service import persist_silver_result
from tests.support import build_test_settings


def test_persist_silver_result_writes_curated_datasets_and_quality_results():
    settings = build_test_settings("test_silver_persistence")
    settings.ensure_directories()
    silver_result = SilverBuildResult(
        source_name="postgres",
        datasets={
            "customers": [{"customer_id": 10, "email": "ana@example.com"}],
            "products": [{"product_id": 100, "product_name": "Notebook Pro"}],
            "orders": [{"order_id": 1, "customer_id": 10}],
            "order_items": [{"order_id": 1, "line_number": 1, "product_id": 100}],
        },
        quality_results=[
            {
                "pipeline_run_id": "silver-run",
                "dataset_name": "silver.customers",
                "layer": "silver",
                "rule_name": "not_null_email",
                "rule_type": "content",
                "status": "passed",
                "failed_records": 0,
                "total_records": 1,
                "failure_rate": 0.0,
                "execution_timestamp": "2026-03-17T16:00:00+00:00",
            }
        ],
    )

    persisted = persist_silver_result(silver_result, settings, batch_id="20260317T160000Z")

    assert persisted.dataset_counts["customers"] == 1
    assert persisted.quality_results_count == 1
    assert persisted.dataset_paths["customers"].exists()
    assert json.loads(persisted.quality_results_path.read_text(encoding="utf-8"))[0]["rule_name"] == "not_null_email"
