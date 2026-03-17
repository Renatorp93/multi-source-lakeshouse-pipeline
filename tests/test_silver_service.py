from pathlib import Path

from lakehouse.silver.service import SilverPersistedResult, build_and_persist_sales_silver
from tests.support import build_test_settings


def test_build_and_persist_sales_silver_orchestrates_bronze_build_silver_and_persistence():
    settings = build_test_settings("test_silver_service")
    captured = {}

    def fake_bronze_builder(_settings):
        captured["bronze_called"] = True
        return "20260317T160000Z", {"postgres": {"customers": [], "products": [], "orders": [], "order_items": []}}

    def fake_silver_builder(bronze_datasets, pipeline_run_id, preferred_source="postgres"):
        captured["silver_input"] = bronze_datasets
        captured["pipeline_run_id"] = pipeline_run_id
        captured["preferred_source"] = preferred_source

        class _Result:
            source_name = "postgres"
            datasets = {"customers": [], "products": [], "orders": [], "order_items": []}
            quality_results = []

        return _Result()

    def fake_persister(silver_result, _settings, batch_id):
        captured["persist_batch_id"] = batch_id
        return SilverPersistedResult(
            source_name=silver_result.source_name,
            batch_id=batch_id,
            dataset_paths={"customers": Path("customers.json")},
            quality_results_path=Path("quality_results.json"),
            dataset_counts={"customers": 0},
            quality_results_count=0,
        )

    result = build_and_persist_sales_silver(
        settings,
        bronze_builder=fake_bronze_builder,
        silver_builder=fake_silver_builder,
        persister=fake_persister,
    )

    assert captured["bronze_called"] is True
    assert captured["pipeline_run_id"] == settings.pipeline_run_id
    assert captured["persist_batch_id"] == "20260317T160000Z"
    assert result.source_name == "postgres"
