import json
from pathlib import Path

from lakehouse.gold.sales import GoldBuildResult
from lakehouse.gold.service import (
    GoldPersistedResult,
    build_and_persist_sales_gold,
    load_persisted_silver_result,
)
from lakehouse.silver.service import SilverPersistedResult
from tests.support import build_test_settings


def test_load_persisted_silver_result_uses_latest_quality_batch_and_discovers_source_files():
    settings = build_test_settings("test_gold_service_loader")
    settings.ensure_directories()

    quality_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    quality_dir.mkdir(parents=True, exist_ok=True)
    (quality_dir / "20260317T160000Z.json").write_text(
        json.dumps([{"rule_name": "old_rule", "status": "passed"}]),
        encoding="utf-8",
    )
    (quality_dir / "20260317T170000Z.json").write_text(
        json.dumps([{"rule_name": "not_null_email", "status": "passed"}]),
        encoding="utf-8",
    )

    for entity in ("customers", "products", "orders", "order_items"):
        target_dir = Path(settings.silver_root) / "postgres" / entity
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / "20260317T170000Z.json").write_text(json.dumps([]), encoding="utf-8")

    result = load_persisted_silver_result(settings)

    assert result.source_name == "postgres"
    assert result.batch_id == "20260317T170000Z"
    assert result.quality_results_count == 1
    assert set(result.dataset_paths) == {"customers", "products", "orders", "order_items"}


def test_build_and_persist_sales_gold_orchestrates_persisted_silver_loading_gold_build_and_persistence():
    settings = build_test_settings("test_gold_service")
    captured = {}

    def fake_silver_result_loader(_settings, batch_id=None):
        captured["silver_loader_called"] = True
        captured["requested_batch_id"] = batch_id
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
        silver_result_loader=fake_silver_result_loader,
        silver_loader=fake_loader,
        gold_builder=fake_gold_builder,
        persister=fake_persister,
    )

    assert captured["silver_loader_called"] is True
    assert captured["requested_batch_id"] is None
    assert captured["loaded_paths"] == {
        "customers": Path("customers.json"),
        "products": Path("products.json"),
        "orders": Path("orders.json"),
        "order_items": Path("order_items.json"),
    }
    assert captured["gold_input"] == {
        "customers": [],
        "products": [],
        "orders": [],
        "order_items": [],
    }
    assert captured["persist_batch_id"] == "20260317T170000Z"
    assert captured["persist_source"] == "api"
    assert result.source_name == "api"
