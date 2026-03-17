from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.config.settings import Settings, load_defaults


def build_test_settings(test_name: str) -> Settings:
    test_root = Path("./logs") / test_name
    if test_root.exists():
        shutil.rmtree(test_root)

    storage_root = (test_root / "storage").resolve()
    logs_root = (test_root / "logs").resolve()
    settings = Settings.from_sources(load_defaults())

    return settings.model_copy(
        update={
            "environment": "test",
            "pipeline_run_id": f"{test_name}_run",
            "storage_root": str(storage_root),
            "landing_root": str(storage_root / "landing"),
            "bronze_root": str(storage_root / "bronze"),
            "silver_root": str(storage_root / "silver"),
            "gold_root": str(storage_root / "gold"),
            "checkpoint_root": str(storage_root / "checkpoints"),
            "logs_root": str(logs_root),
            "spark_warehouse_dir": str(storage_root / "warehouse"),
        }
    )


def sample_raw_payloads() -> dict[str, list[dict]]:
    return {
        "users": [
            {
                "id": 10,
                "firstName": "Ana",
                "lastName": "Silva",
                "email": "ana@example.com",
                "phone": "1111-1111",
                "address": {"city": "Sao Paulo", "state": "SP", "country": "Brasil"},
            }
        ],
        "products": [
            {
                "id": 100,
                "sku": "SKU-100",
                "title": "Notebook Pro",
                "category": "laptops",
                "brand": "Tech",
                "price": 7999.9,
                "stock": 12,
                "rating": 4.8,
                "availabilityStatus": "In Stock",
            }
        ],
        "carts": [
            {
                "id": 5,
                "userId": 10,
                "total": 9999.9,
                "discountedTotal": 9499.9,
                "totalProducts": 1,
                "totalQuantity": 2,
                "products": [
                    {
                        "id": 100,
                        "title": "Notebook Pro",
                        "price": 4999.95,
                        "quantity": 2,
                        "total": 9999.9,
                        "discountPercentage": 5.0,
                        "discountedTotal": 9499.9,
                    }
                ],
            }
        ],
    }


def fixed_timestamp() -> datetime:
    return datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
