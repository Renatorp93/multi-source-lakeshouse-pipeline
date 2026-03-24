from datetime import datetime, timezone

from lakehouse.bronze.sales import build_bronze_datasets, enrich_bronze_records
from lakehouse.ingestion.sales.exports import write_raw_api_payloads, write_sales_csv_exports
from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from tests.support import build_test_settings, sample_raw_payloads


def test_enrich_bronze_records_adds_processing_metadata_without_losing_business_fields():
    records = [
        {
            "customer_id": 10,
            "first_name": "Ana",
            "last_name": "Silva",
            "email": "ana@example.com",
            "phone": "1111-1111",
            "city": "Sao Paulo",
            "state": "SP",
            "country": "Brasil",
            "source_system": "dummyjson_api",
            "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
            "batch_id": "20260317T120000Z",
            "load_date": "2026-03-17",
            "pipeline_run_id": "test-run",
        }
    ]

    enriched = enrich_bronze_records(
        records,
        "customers",
        processing_timestamp=datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc),
    )

    assert enriched[0]["customer_id"] == 10
    assert enriched[0]["processing_timestamp"] == "2026-03-17T13:00:00+00:00"
    assert len(enriched[0]["record_hash"]) == 64


def test_build_bronze_datasets_uses_the_same_latest_batch_for_all_sources():
    settings = build_test_settings("test_bronze_behavior")
    settings.ensure_directories()

    snapshot = build_sales_snapshot(
        sample_raw_payloads(),
        pipeline_run_id=settings.pipeline_run_id,
        extracted_at=datetime(2026, 3, 17, 15, 0, tzinfo=timezone.utc),
    )
    write_raw_api_payloads(snapshot, settings)
    write_sales_csv_exports(snapshot, settings)

    postgres_rows = {
        "customers": snapshot.customers,
        "products": snapshot.products,
        "orders": snapshot.orders,
        "order_items": snapshot.order_items,
    }

    batch_id, datasets = build_bronze_datasets(
        settings,
        postgres_fetcher=lambda _settings, entity: postgres_rows[entity],
    )

    assert batch_id == snapshot.batch_id
    assert datasets["api"]["customers"][0]["customer_id"] == 10
    assert datasets["csv"]["customers"][0]["customer_id"] == "10"
    assert datasets["postgres"]["orders"][0]["order_id"] == 5
    assert "record_hash" in datasets["api"]["customers"][0]
