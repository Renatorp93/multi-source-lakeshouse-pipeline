from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GoldBuildResult:
    marts: dict[str, list[dict[str, Any]]]


def build_gold_sales_marts(silver_datasets: dict[str, list[dict[str, Any]]]) -> GoldBuildResult:
    customers = silver_datasets.get("customers", [])
    products = silver_datasets.get("products", [])
    orders = silver_datasets.get("orders", [])
    order_items = silver_datasets.get("order_items", [])
    total_customers = len({record.get("customer_id") for record in customers if record.get("customer_id") is not None})

    customer_index = {record.get("customer_id"): record for record in customers}
    product_index = {record.get("product_id"): record for record in products}
    items_by_order = _group_items_by_order(order_items)

    daily_sales = _build_daily_sales(orders, items_by_order, total_customers=total_customers)
    monthly_sales = _build_monthly_sales(orders, items_by_order, total_customers=total_customers)
    customer_sales = _build_customer_sales(orders, items_by_order, customer_index, total_customers=total_customers)
    product_sales = _build_product_sales(order_items, product_index)

    return GoldBuildResult(
        marts={
            "daily_sales": daily_sales,
            "monthly_sales": monthly_sales,
            "customer_sales": customer_sales,
            "product_sales": product_sales,
        }
    )


def _build_daily_sales(
    orders: list[dict[str, Any]],
    items_by_order: dict[Any, list[dict[str, Any]]],
    total_customers: int,
) -> list[dict[str, Any]]:
    aggregations: dict[str, dict[str, Any]] = {}

    for order in orders:
        order_id = order.get("order_id")
        order_date = order.get("order_date")
        customer_id = order.get("customer_id")

        if order_id is None or order_date is None:
            continue

        entry = aggregations.setdefault(
            order_date,
            {
                "order_date": order_date,
                "order_count": 0,
                "customer_ids": set(),
                "items_sold": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
            },
        )
        entry["order_count"] += 1
        entry["gross_revenue"] += _to_float(order.get("gross_amount"))
        entry["net_revenue"] += _to_float(order.get("discounted_amount"))
        if customer_id is not None:
            entry["customer_ids"].add(customer_id)
        entry["items_sold"] += sum(_to_int(item.get("quantity")) for item in items_by_order.get(order_id, []))

    results = []
    for order_date in sorted(aggregations):
        entry = aggregations[order_date]
        results.append(
            {
                "order_date": order_date,
                "order_count": entry["order_count"],
                "customer_count": len(entry["customer_ids"]),
                "items_sold": entry["items_sold"],
                "gross_revenue": _round_amount(entry["gross_revenue"]),
                "net_revenue": _round_amount(entry["net_revenue"]),
                "discount_amount": _round_amount(entry["gross_revenue"] - entry["net_revenue"]),
                "discount_rate": _safe_rate(entry["gross_revenue"] - entry["net_revenue"], entry["gross_revenue"]),
                "average_ticket": _round_amount(
                    0.0 if entry["order_count"] == 0 else entry["net_revenue"] / entry["order_count"]
                ),
                "average_items_per_order": _safe_rate(entry["items_sold"], entry["order_count"]),
                "buyer_conversion_rate": _safe_rate(len(entry["customer_ids"]), total_customers),
            }
        )
    return results


def _build_monthly_sales(
    orders: list[dict[str, Any]],
    items_by_order: dict[Any, list[dict[str, Any]]],
    total_customers: int,
) -> list[dict[str, Any]]:
    aggregations: dict[str, dict[str, Any]] = {}

    for order in orders:
        order_id = order.get("order_id")
        order_date = order.get("order_date")
        customer_id = order.get("customer_id")

        if order_id is None or order_date is None:
            continue

        order_month = str(order_date)[:7]
        entry = aggregations.setdefault(
            order_month,
            {
                "order_month": order_month,
                "order_count": 0,
                "customer_ids": set(),
                "items_sold": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
            },
        )
        entry["order_count"] += 1
        entry["gross_revenue"] += _to_float(order.get("gross_amount"))
        entry["net_revenue"] += _to_float(order.get("discounted_amount"))
        if customer_id is not None:
            entry["customer_ids"].add(customer_id)
        entry["items_sold"] += sum(_to_int(item.get("quantity")) for item in items_by_order.get(order_id, []))

    results = []
    for order_month in sorted(aggregations):
        entry = aggregations[order_month]
        results.append(
            {
                "order_month": order_month,
                "order_count": entry["order_count"],
                "customer_count": len(entry["customer_ids"]),
                "items_sold": entry["items_sold"],
                "gross_revenue": _round_amount(entry["gross_revenue"]),
                "net_revenue": _round_amount(entry["net_revenue"]),
                "discount_amount": _round_amount(entry["gross_revenue"] - entry["net_revenue"]),
                "discount_rate": _safe_rate(entry["gross_revenue"] - entry["net_revenue"], entry["gross_revenue"]),
                "average_ticket": _round_amount(
                    0.0 if entry["order_count"] == 0 else entry["net_revenue"] / entry["order_count"]
                ),
                "average_items_per_order": _safe_rate(entry["items_sold"], entry["order_count"]),
                "buyer_conversion_rate": _safe_rate(len(entry["customer_ids"]), total_customers),
            }
        )
    return results


def _build_customer_sales(
    orders: list[dict[str, Any]],
    items_by_order: dict[Any, list[dict[str, Any]]],
    customer_index: dict[Any, dict[str, Any]],
    total_customers: int,
) -> list[dict[str, Any]]:
    aggregations: dict[Any, dict[str, Any]] = {}

    for order in orders:
        order_id = order.get("order_id")
        customer_id = order.get("customer_id")
        order_date = order.get("order_date")

        if order_id is None or customer_id is None:
            continue

        customer = customer_index.get(customer_id, {})
        entry = aggregations.setdefault(
            customer_id,
            {
                "customer_id": customer_id,
                "customer_name": _build_customer_name(customer),
                "email": customer.get("email"),
                "order_count": 0,
                "items_sold": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
                "last_order_date": order_date,
            },
        )
        entry["order_count"] += 1
        entry["items_sold"] += sum(_to_int(item.get("quantity")) for item in items_by_order.get(order_id, []))
        entry["gross_revenue"] += _to_float(order.get("gross_amount"))
        entry["net_revenue"] += _to_float(order.get("discounted_amount"))
        entry["last_order_date"] = _max_nullable(entry["last_order_date"], order_date)

    results = []
    for customer_id in sorted(aggregations):
        entry = aggregations[customer_id]
        results.append(
            {
                "customer_id": entry["customer_id"],
                "customer_name": entry["customer_name"],
                "email": entry["email"],
                "order_count": entry["order_count"],
                "items_sold": entry["items_sold"],
                "gross_revenue": _round_amount(entry["gross_revenue"]),
                "net_revenue": _round_amount(entry["net_revenue"]),
                "discount_amount": _round_amount(entry["gross_revenue"] - entry["net_revenue"]),
                "average_ticket": _round_amount(
                    0.0 if entry["order_count"] == 0 else entry["net_revenue"] / entry["order_count"]
                ),
                "buyer_conversion_rate": _safe_rate(1, total_customers) if total_customers else 0.0,
                "last_order_date": entry["last_order_date"],
            }
        )
    return results


def _build_product_sales(
    order_items: list[dict[str, Any]],
    product_index: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    aggregations: dict[Any, dict[str, Any]] = {}

    for item in order_items:
        product_id = item.get("product_id")
        order_id = item.get("order_id")

        if product_id is None or order_id is None:
            continue

        product = product_index.get(product_id, {})
        entry = aggregations.setdefault(
            product_id,
            {
                "product_id": product_id,
                "product_name": product.get("product_name") or item.get("product_name"),
                "category": product.get("category"),
                "order_ids": set(),
                "units_sold": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
            },
        )
        entry["order_ids"].add(order_id)
        entry["units_sold"] += _to_int(item.get("quantity"))
        entry["gross_revenue"] += _to_float(item.get("gross_amount"))
        entry["net_revenue"] += _to_float(item.get("discounted_amount"))

    results = []
    for entry in aggregations.values():
        results.append(
            {
                "product_id": entry["product_id"],
                "product_name": entry["product_name"],
                "category": entry["category"],
                "order_count": len(entry["order_ids"]),
                "units_sold": entry["units_sold"],
                "gross_revenue": _round_amount(entry["gross_revenue"]),
                "net_revenue": _round_amount(entry["net_revenue"]),
                "discount_amount": _round_amount(entry["gross_revenue"] - entry["net_revenue"]),
                "discount_rate": _safe_rate(entry["gross_revenue"] - entry["net_revenue"], entry["gross_revenue"]),
            }
        )

    return sorted(
        results,
        key=lambda record: (-record["net_revenue"], -record["units_sold"], record["product_id"]),
    )


def _group_items_by_order(order_items: list[dict[str, Any]]) -> dict[Any, list[dict[str, Any]]]:
    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for item in order_items:
        grouped[item.get("order_id")].append(item)
    return dict(grouped)


def _build_customer_name(customer: dict[str, Any]) -> str | None:
    first_name = str(customer.get("first_name", "")).strip()
    last_name = str(customer.get("last_name", "")).strip()
    full_name = " ".join(part for part in (first_name, last_name) if part)
    return full_name or None


def _max_nullable(current: str | None, candidate: str | None) -> str | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def _round_amount(value: float) -> float:
    return round(value, 2)


def _safe_rate(numerator: float | int, denominator: float | int, precision: int = 4) -> float:
    if denominator in (0, 0.0):
        return 0.0
    return round(float(numerator) / float(denominator), precision)


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)
