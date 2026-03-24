from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from tests.support import fixed_timestamp, sample_raw_payloads


def test_build_sales_snapshot_normalizes_entities():
    snapshot = build_sales_snapshot(
        sample_raw_payloads(),
        pipeline_run_id="test-run",
        extracted_at=fixed_timestamp(),
    )

    assert snapshot.batch_id == "20260317T120000Z"
    assert snapshot.customers[0]["customer_id"] == 10
    assert snapshot.products[0]["product_id"] == 100
    assert snapshot.orders[0]["order_date"] == "2026-03-12"
    assert snapshot.order_items[0]["line_number"] == 1
    assert snapshot.order_items[0]["pipeline_run_id"] == "test-run"
