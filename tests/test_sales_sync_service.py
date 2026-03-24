import json

from lakehouse.ingestion.sales.service import sync_sales_sources
from tests.support import build_test_settings, sample_raw_payloads


class FakeSalesClient:
    def fetch_sales_payloads(self) -> dict[str, list[dict]]:
        return sample_raw_payloads()


def test_sync_sales_sources_persists_sales_snapshot_in_all_outputs():
    settings = build_test_settings("test_sync_sales_service")
    settings.ensure_directories()
    captured = {}

    def fake_seeder(snapshot, _settings):
        captured["snapshot"] = snapshot

    result = sync_sales_sources(settings, client=FakeSalesClient(), seeder=fake_seeder)

    assert result.customers == 1
    assert result.products == 1
    assert result.orders == 1
    assert result.order_items == 1
    assert (result.api_dir / "users.json").exists()
    assert (result.csv_dir / "customers.csv").exists()
    assert json.loads((result.api_dir / "users.json").read_text(encoding="utf-8"))["records"][0]["id"] == 10
    assert captured["snapshot"].customers[0]["customer_id"] == 10
    assert captured["snapshot"].orders[0]["gross_amount"] == 9999.9
