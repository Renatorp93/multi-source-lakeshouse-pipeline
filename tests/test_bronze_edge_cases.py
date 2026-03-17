import pytest
from datetime import datetime, timezone

from lakehouse.bronze.sales import build_bronze_datasets, enrich_bronze_records
from tests.support import build_test_settings


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
