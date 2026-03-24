import shutil
from pathlib import Path
import pytest

from lakehouse.ingestion.sales.batches import find_latest_batch
from lakehouse.ingestion.sales.dummyjson import DummyJsonClient
from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from lakehouse.ingestion.sales.service import sync_sales_sources
from tests.support import build_test_settings, fixed_timestamp, sample_raw_payloads


class EmptySalesClient:
    def fetch_sales_payloads(self) -> dict[str, list[dict]]:
        return {"users": [], "products": [], "carts": []}


def test_build_sales_snapshot_handles_payload_without_orders():
    raw_payloads = sample_raw_payloads()
    raw_payloads["carts"] = []

    snapshot = build_sales_snapshot(
        raw_payloads,
        pipeline_run_id="edge-run",
        extracted_at=fixed_timestamp(),
    )

    assert snapshot.customers
    assert snapshot.products
    assert snapshot.orders == []
    assert snapshot.order_items == []


def test_build_sales_snapshot_preserves_none_for_missing_optional_fields():
    snapshot = build_sales_snapshot(
        {
            "users": [{"id": 1, "firstName": "Ana", "lastName": "Silva"}],
            "products": [{"id": 2, "title": "Mouse"}],
            "carts": [{"id": 3, "userId": 1, "products": [{}]}],
        },
        pipeline_run_id="edge-run",
        extracted_at=fixed_timestamp(),
    )

    assert snapshot.customers[0]["city"] is None
    assert snapshot.products[0]["brand"] is None
    assert snapshot.order_items[0]["product_id"] is None


def test_sync_sales_sources_returns_zero_counts_for_empty_payloads():
    settings = build_test_settings("test_sync_sales_empty")
    settings.ensure_directories()
    captured = {}

    def fake_seeder(snapshot, _settings):
        captured["snapshot"] = snapshot

    result = sync_sales_sources(settings, client=EmptySalesClient(), seeder=fake_seeder)

    assert result.customers == 0
    assert result.products == 0
    assert result.orders == 0
    assert result.order_items == 0
    assert result.api_dir.exists()
    assert result.csv_dir.exists()
    assert captured["snapshot"].raw_payloads["users"] == []


def test_find_latest_batch_raises_when_no_common_batch_exists():
    test_root = build_test_settings("test_no_common_batch").logs_root
    root_path = Path(test_root).parent / "batches"
    if root_path.exists():
        shutil.rmtree(root_path)

    api_root = root_path / "api"
    csv_root = root_path / "csv"
    api_root.mkdir(parents=True)
    csv_root.mkdir(parents=True)
    (api_root / "20260317T120000Z").mkdir()
    (csv_root / "20260317T130000Z").mkdir()

    with pytest.raises(FileNotFoundError):
        find_latest_batch(api_root, csv_root)


def test_dummyjson_client_returns_empty_collection_when_total_is_zero(monkeypatch):
    monkeypatch.setattr(
        DummyJsonClient,
        "_get_json",
        lambda self, _resource, _params: {"users": [], "total": 0},
    )
    client = DummyJsonClient("https://dummyjson.com")

    items = client.fetch_collection("users", "users", page_size=100)

    assert items == []
