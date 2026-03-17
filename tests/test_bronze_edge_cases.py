import pytest
from datetime import datetime, timezone

from lakehouse.bronze.sales import build_bronze_datasets, enrich_bronze_records
from lakehouse.ingestion.sales.exports import write_raw_api_payloads, write_sales_csv_exports
from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from tests.support import build_test_settings
from tests.support import fixed_timestamp, sample_raw_payloads


def test_enrich_bronze_records_fills_missing_fields_and_keeps_hash_stable():
    records = [{"customer_id": 10, "first_name": "Ana"}]
    processed_at = datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc)

    first = enrich_bronze_records(records, "customers", processing_timestamp=processed_at)
    second = enrich_bronze_records(records, "customers", processing_timestamp=processed_at)

    assert first[0]["last_name"] is None
    assert first[0]["record_hash"] == second[0]["record_hash"]
    assert first[0]["processing_timestamp"] == "2026-03-17T13:00:00+00:00"


def test_build_bronze_datasets_fails_fast_when_landing_is_missing():
    settings = build_test_settings("test_bronze_missing_landing")
    settings.ensure_directories()

    with pytest.raises(FileNotFoundError):
        build_bronze_datasets(settings, postgres_fetcher=lambda *_args: [])


def test_build_bronze_datasets_can_skip_postgres_when_source_error_is_allowed():
    settings = build_test_settings("test_bronze_skip_postgres")
    settings.ensure_directories()
    snapshot = build_sales_snapshot(
        sample_raw_payloads(),
        pipeline_run_id=settings.pipeline_run_id,
        extracted_at=fixed_timestamp(),
    )
    write_raw_api_payloads(snapshot, settings)
    write_sales_csv_exports(snapshot, settings)

    batch_id, datasets = build_bronze_datasets(
        settings,
        postgres_fetcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("postgres indisponivel")),
        on_source_error="skip",
    )

    assert batch_id == snapshot.batch_id
    assert "postgres" not in datasets
    assert "api" in datasets
    assert "csv" in datasets
