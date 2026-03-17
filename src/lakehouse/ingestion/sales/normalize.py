from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class SalesSnapshot:
    batch_id: str
    extraction_timestamp: str
    customers: list[dict[str, Any]]
    products: list[dict[str, Any]]
    orders: list[dict[str, Any]]
    order_items: list[dict[str, Any]]
    raw_payloads: dict[str, list[dict[str, Any]]]


def build_sales_snapshot(
    raw_payloads: dict[str, list[dict[str, Any]]],
    pipeline_run_id: str,
    extracted_at: datetime | None = None,
) -> SalesSnapshot:
    extracted_at = extracted_at or datetime.now(timezone.utc)
    extraction_timestamp = extracted_at.isoformat()
    batch_id = extracted_at.strftime("%Y%m%dT%H%M%SZ")
    load_date = extracted_at.date().isoformat()

    customers = [
        {
            "customer_id": user["id"],
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
            "email": user.get("email"),
            "phone": user.get("phone"),
            "city": (user.get("address") or {}).get("city"),
            "state": (user.get("address") or {}).get("state"),
            "country": (user.get("address") or {}).get("country"),
            "source_system": "dummyjson_api",
            "ingestion_timestamp": extraction_timestamp,
            "batch_id": batch_id,
            "load_date": load_date,
            "pipeline_run_id": pipeline_run_id,
        }
        for user in raw_payloads["users"]
    ]

    products = [
        {
            "product_id": product["id"],
            "sku": product.get("sku"),
            "product_name": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "price": product.get("price"),
            "stock": product.get("stock"),
            "rating": product.get("rating"),
            "availability_status": product.get("availabilityStatus"),
            "source_system": "dummyjson_api",
            "ingestion_timestamp": extraction_timestamp,
            "batch_id": batch_id,
            "load_date": load_date,
            "pipeline_run_id": pipeline_run_id,
        }
        for product in raw_payloads["products"]
    ]

    orders: list[dict[str, Any]] = []
    order_items: list[dict[str, Any]] = []

    for cart in raw_payloads["carts"]:
        order_date = _derive_order_date(cart["id"], extracted_at)
        orders.append(
            {
                "order_id": cart["id"],
                "customer_id": cart.get("userId"),
                "order_date": order_date,
                "gross_amount": cart.get("total"),
                "discounted_amount": cart.get("discountedTotal"),
                "total_products": cart.get("totalProducts"),
                "total_quantity": cart.get("totalQuantity"),
                "source_system": "dummyjson_api",
                "ingestion_timestamp": extraction_timestamp,
                "batch_id": batch_id,
                "load_date": load_date,
                "pipeline_run_id": pipeline_run_id,
            }
        )

        for line_number, item in enumerate(cart.get("products", []), start=1):
            order_items.append(
                {
                    "order_id": cart["id"],
                    "line_number": line_number,
                    "product_id": item.get("id"),
                    "product_name": item.get("title"),
                    "quantity": item.get("quantity"),
                    "unit_price": item.get("price"),
                    "gross_amount": item.get("total"),
                    "discount_percentage": item.get("discountPercentage"),
                    "discounted_amount": item.get("discountedTotal"),
                    "source_system": "dummyjson_api",
                    "ingestion_timestamp": extraction_timestamp,
                    "batch_id": batch_id,
                    "load_date": load_date,
                    "pipeline_run_id": pipeline_run_id,
                }
            )

    return SalesSnapshot(
        batch_id=batch_id,
        extraction_timestamp=extraction_timestamp,
        customers=customers,
        products=products,
        orders=orders,
        order_items=order_items,
        raw_payloads=raw_payloads,
    )


def _derive_order_date(cart_id: int, extracted_at: datetime) -> str:
    synthetic_date = extracted_at.date() - timedelta(days=cart_id % 45)
    return synthetic_date.isoformat()
