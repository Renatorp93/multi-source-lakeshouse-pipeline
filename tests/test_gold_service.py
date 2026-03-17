from pathlib import Path

from lakehouse.gold.sales import GoldBuildResult
from lakehouse.gold.service import GoldPersistedResult, build_and_persist_sales_gold
from lakehouse.silver.service import SilverPersistedResult
from tests.support import build_test_settings


def test_build_and_persist_sales_gold_orchestrates_silver_loading_gold_build_and_persistence():
    settings = build_test_settings("test_gold_service")
    captured = {}

    def fake_silver_pipeline(_settings):
        captured["silver_called"] = True
        return SilverPersistedResult(
            source_name="api",
            batch_id="20260317T170000Z",
            dataset_paths={
                "customers": Path("customers.json"),
                "products": Path("products.json"),
                "orders": Path("orders.json"),
                "order_items": Path("order_items.json"),
            },
            quality_results_path=Path("quality.json"),
            dataset_counts={"customers": 1},
            quality_results_count=1,
        )

    def fake_loader(dataset_paths):
        captured["loaded_paths"] = dataset_paths
        return {
            "customers": [],
            "products": [],
            "orders": [],
            "order_items": [],
        }

    def fake_gold_builder(silver_datasets):
        captured["gold_input"] = silver_datasets
        return GoldBuildResult(
            marts={
                "daily_sales": [],
                "customer_sales": [],
                "product_sales": [],
            }
        )

    def fake_persister(gold_result, _settings, batch_id, source_name):
        captured["persist_batch_id"] = batch_id
        captured["persist_source"] = source_name
        return GoldPersistedResult(
            source_name=source_name,
            batch_id=batch_id,
            mart_paths={"daily_sales": Path("daily_sales.json")},
            mart_counts={"daily_sales": 0},
        )

    result = build_and_persist_sales_gold(
        settings,
        silver_pipeline=fake_silver_pipeline,
        silver_loader=fake_loader,
        gold_builder=fake_gold_builder,
        persister=fake_persister,
    )

    assert captured["silver_called"] is True
    assert captured["gold_input"] == {
        "customers": [],
        "products": [],
        "orders": [],
        "order_items": [],
    }
    assert captured["persist_batch_id"] == "20260317T170000Z"
    assert captured["persist_source"] == "api"
    assert result.source_name == "api"
