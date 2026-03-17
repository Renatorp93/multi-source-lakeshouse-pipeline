from datetime import datetime, timezone

from lakehouse.ingestion.sales.normalize import build_sales_snapshot


def test_build_sales_snapshot_normalizes_entities():
    raw_payloads = {
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

    snapshot = build_sales_snapshot(
        raw_payloads,
        pipeline_run_id="test-run",
        extracted_at=datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc),
    )

    assert snapshot.batch_id == "20260317T120000Z"
    assert snapshot.customers[0]["customer_id"] == 10
    assert snapshot.products[0]["product_id"] == 100
    assert snapshot.orders[0]["order_date"] == "2026-03-12"
    assert snapshot.order_items[0]["line_number"] == 1
    assert snapshot.order_items[0]["pipeline_run_id"] == "test-run"
