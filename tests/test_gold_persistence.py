import json

from lakehouse.gold.sales import GoldBuildResult
from lakehouse.gold.service import persist_gold_result
from tests.support import build_test_settings


def test_persist_gold_result_writes_all_marts_to_storage():
    settings = build_test_settings("test_gold_persistence")
    gold_result = GoldBuildResult(
        marts={
            "daily_sales": [{"order_date": "2026-03-10", "order_count": 1}],
            "customer_sales": [{"customer_id": 10, "order_count": 1}],
            "product_sales": [{"product_id": 100, "units_sold": 2}],
        }
    )

    result = persist_gold_result(
        gold_result,
        settings=settings,
        batch_id="20260317T170000Z",
        source_name="api",
    )

    assert result.source_name == "api"
    assert result.mart_counts == {
        "daily_sales": 1,
        "customer_sales": 1,
        "product_sales": 1,
    }
    assert json.loads(result.mart_paths["daily_sales"].read_text(encoding="utf-8")) == [
        {"order_date": "2026-03-10", "order_count": 1}
    ]


