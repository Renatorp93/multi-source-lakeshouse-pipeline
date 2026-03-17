import json
from datetime import datetime, timezone

from lakehouse.config.settings import get_settings
from lakehouse.ingestion.sales.exports import write_raw_api_payloads, write_sales_csv_exports
from lakehouse.ingestion.sales.normalize import build_sales_snapshot


def test_sales_exports_write_expected_files():
    settings = get_settings()
    snapshot = build_sales_snapshot(
        {
            "users": [{"id": 1, "firstName": "Joao", "lastName": "Pereira", "address": {}}],
            "products": [{"id": 2, "title": "Mouse", "price": 99.9}],
            "carts": [
                {
                    "id": 3,
                    "userId": 1,
                    "total": 99.9,
                    "discountedTotal": 89.9,
                    "totalProducts": 1,
                    "totalQuantity": 1,
                    "products": [
                        {
                            "id": 2,
                            "title": "Mouse",
                            "price": 99.9,
                            "quantity": 1,
                            "total": 99.9,
                            "discountPercentage": 10,
                            "discountedTotal": 89.9,
                        }
                    ],
                }
            ],
        },
        pipeline_run_id="test-export",
        extracted_at=datetime(2026, 3, 17, 15, 0, tzinfo=timezone.utc),
    )

    api_dir = write_raw_api_payloads(snapshot, settings)
    csv_dir = write_sales_csv_exports(snapshot, settings)

    assert (api_dir / "users.json").exists()
    assert (csv_dir / "customers.csv").exists()
    assert json.loads((api_dir / "users.json").read_text(encoding="utf-8"))["resource"] == "users"
