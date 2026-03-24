from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lakehouse.quality.rules import (
    quality_result_to_dict,
    validate_allowed_reference,
    validate_discount_not_greater_than_gross,
    validate_not_null,
    validate_positive,
    validate_unique,
)


@dataclass(frozen=True)
class SilverBuildResult:
    source_name: str
    datasets: dict[str, list[dict[str, Any]]]
    quality_results: list[dict[str, Any]]


def build_silver_sales_datasets(
    bronze_datasets: dict[str, dict[str, list[dict[str, Any]]]],
    pipeline_run_id: str,
    preferred_source: str = "postgres",
    execution_timestamp: datetime | None = None,
) -> SilverBuildResult:
    source_name = select_silver_source(bronze_datasets, preferred_source)
    source_datasets = bronze_datasets[source_name]
    execution_timestamp = execution_timestamp or datetime.now(timezone.utc)

    quality_results = evaluate_sales_quality(
        source_datasets,
        pipeline_run_id=pipeline_run_id,
        execution_timestamp=execution_timestamp,
    )

    customers = clean_customers(source_datasets["customers"])
    products = clean_products(source_datasets["products"])
    orders = clean_orders(source_datasets["orders"], customers)
    order_items = clean_order_items(source_datasets["order_items"], orders, products)

    return SilverBuildResult(
        source_name=source_name,
        datasets={
            "customers": customers,
            "products": products,
            "orders": orders,
            "order_items": order_items,
        },
        quality_results=quality_results,
    )


def select_silver_source(
    bronze_datasets: dict[str, dict[str, list[dict[str, Any]]]],
    preferred_source: str,
) -> str:
    if preferred_source in bronze_datasets:
        return preferred_source
    if not bronze_datasets:
        raise ValueError("Nenhuma fonte Bronze disponivel para construir a Silver.")
    return next(iter(bronze_datasets))


def evaluate_sales_quality(
    datasets: dict[str, list[dict[str, Any]]],
    pipeline_run_id: str,
    execution_timestamp: datetime,
) -> list[dict[str, Any]]:
    customers = datasets["customers"]
    products = datasets["products"]
    orders = datasets["orders"]
    order_items = datasets["order_items"]

    results = [
        validate_not_null(customers, "customer_id", "silver.customers", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(customers, "email", "silver.customers", "silver", pipeline_run_id, execution_timestamp),
        validate_unique(customers, ["customer_id"], "silver.customers", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(products, "product_id", "silver.products", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(products, "product_name", "silver.products", "silver", pipeline_run_id, execution_timestamp),
        validate_unique(products, ["product_id"], "silver.products", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(orders, "order_id", "silver.orders", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(orders, "customer_id", "silver.orders", "silver", pipeline_run_id, execution_timestamp),
        validate_unique(orders, ["order_id"], "silver.orders", "silver", pipeline_run_id, execution_timestamp),
        validate_allowed_reference(
            orders,
            "customer_id",
            {customer.get("customer_id") for customer in customers if customer.get("customer_id") is not None},
            "silver.orders",
            "silver",
            pipeline_run_id,
            "order_customer_exists",
            execution_timestamp,
        ),
        validate_discount_not_greater_than_gross(
            orders,
            "silver.orders",
            "silver",
            pipeline_run_id,
            execution_timestamp,
        ),
        validate_not_null(order_items, "order_id", "silver.order_items", "silver", pipeline_run_id, execution_timestamp),
        validate_not_null(order_items, "product_id", "silver.order_items", "silver", pipeline_run_id, execution_timestamp),
        validate_positive(
            order_items,
            "quantity",
            "silver.order_items",
            "silver",
            pipeline_run_id,
            "positive_quantity",
            execution_timestamp,
        ),
        validate_allowed_reference(
            order_items,
            "order_id",
            {order.get("order_id") for order in orders if order.get("order_id") is not None},
            "silver.order_items",
            "silver",
            pipeline_run_id,
            "order_item_order_exists",
            execution_timestamp,
        ),
    ]

    return [quality_result_to_dict(result) for result in results]


def clean_customers(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid_records = [
        record
        for record in records
        if record.get("customer_id") is not None and _normalize_nullable_string(record.get("email")) is not None
    ]
    deduplicated = _deduplicate(valid_records, ["customer_id"])

    cleaned: list[dict[str, Any]] = []
    for record in deduplicated:
        cleaned.append(
            {
                **record,
                "email": _normalize_nullable_string(record.get("email")),
                "city": _title_or_none(record.get("city")),
                "state": _upper_or_none(record.get("state")),
                "country": _title_or_none(record.get("country")),
            }
        )
    return cleaned


def clean_products(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid_records = [
        record
        for record in records
        if record.get("product_id") is not None and _normalize_nullable_string(record.get("product_name")) is not None
    ]
    deduplicated = _deduplicate(valid_records, ["product_id"])

    cleaned: list[dict[str, Any]] = []
    for record in deduplicated:
        cleaned.append(
            {
                **record,
                "product_name": _normalize_nullable_string(record.get("product_name")).title(),
                "category": _lower_or_none(record.get("category")),
                "brand": _title_or_none(record.get("brand")),
            }
        )
    return cleaned


def clean_orders(records: list[dict[str, Any]], customers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowed_customer_ids = {customer["customer_id"] for customer in customers}
    valid_records = []

    for record in records:
        if record.get("order_id") is None or record.get("customer_id") is None:
            continue
        if record.get("customer_id") not in allowed_customer_ids:
            continue
        gross_amount = record.get("gross_amount")
        discounted_amount = record.get("discounted_amount")
        if gross_amount is not None and discounted_amount is not None and float(discounted_amount) > float(gross_amount):
            continue
        valid_records.append(record)

    return _deduplicate(valid_records, ["order_id"])


def clean_order_items(
    records: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    allowed_order_ids = {order["order_id"] for order in orders}
    allowed_product_ids = {product["product_id"] for product in products}
    valid_records = []

    for record in records:
        if record.get("order_id") is None or record.get("product_id") is None:
            continue
        if record.get("order_id") not in allowed_order_ids:
            continue
        if record.get("product_id") not in allowed_product_ids:
            continue
        if record.get("quantity") is None or float(record.get("quantity")) <= 0:
            continue
        valid_records.append(record)

    return _deduplicate(valid_records, ["order_id", "line_number"])


def _deduplicate(records: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    deduplicated: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for record in records:
        key = tuple(record.get(field_name) for field_name in key_fields)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(record)

    return deduplicated


def _normalize_nullable_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized.lower() if normalized else None


def _title_or_none(value: Any) -> str | None:
    normalized = _normalize_nullable_string(value)
    return normalized.title() if normalized is not None else None


def _upper_or_none(value: Any) -> str | None:
    normalized = _normalize_nullable_string(value)
    return normalized.upper() if normalized is not None else None


def _lower_or_none(value: Any) -> str | None:
    return _normalize_nullable_string(value)
