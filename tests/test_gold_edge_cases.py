from lakehouse.gold.sales import build_gold_sales_marts


def test_build_gold_sales_marts_returns_empty_views_when_silver_is_empty():
    result = build_gold_sales_marts(
        {
            "customers": [],
            "products": [],
            "orders": [],
            "order_items": [],
        }
    )

    assert result.marts == {
        "daily_sales": [],
        "customer_sales": [],
        "product_sales": [],
    }


def test_build_gold_sales_marts_keeps_order_metrics_even_without_order_items():
    silver_datasets = {
        "customers": [
            {"customer_id": 10, "first_name": "Ana", "last_name": "Silva", "email": "ana@example.com"},
        ],
        "products": [],
        "orders": [
            {"order_id": 1, "customer_id": 10, "order_date": "2026-03-10", "gross_amount": 100.0, "discounted_amount": 90.0},
        ],
        "order_items": [],
    }

    result = build_gold_sales_marts(silver_datasets)

    assert result.marts["daily_sales"] == [
        {
            "order_date": "2026-03-10",
            "order_count": 1,
            "customer_count": 1,
            "items_sold": 0,
            "gross_revenue": 100.0,
            "net_revenue": 90.0,
            "average_ticket": 90.0,
        }
    ]
    assert result.marts["customer_sales"] == [
        {
            "customer_id": 10,
            "customer_name": "Ana Silva",
            "email": "ana@example.com",
            "order_count": 1,
            "items_sold": 0,
            "gross_revenue": 100.0,
            "net_revenue": 90.0,
            "last_order_date": "2026-03-10",
        }
    ]
    assert result.marts["product_sales"] == []


def test_build_gold_sales_marts_uses_item_information_when_product_dimension_is_missing():
    silver_datasets = {
        "customers": [
            {"customer_id": 10, "first_name": "Ana", "last_name": "Silva", "email": "ana@example.com"},
        ],
        "products": [],
        "orders": [
            {"order_id": 1, "customer_id": 10, "order_date": "2026-03-10", "gross_amount": 100.0, "discounted_amount": 95.0},
        ],
        "order_items": [
            {"order_id": 1, "line_number": 1, "product_id": 999, "product_name": "Produto Orfao", "quantity": 2, "gross_amount": 100.0, "discounted_amount": 95.0},
        ],
    }

    result = build_gold_sales_marts(silver_datasets)

    assert result.marts["product_sales"] == [
        {
            "product_id": 999,
            "product_name": "Produto Orfao",
            "category": None,
            "order_count": 1,
            "units_sold": 2,
            "gross_revenue": 100.0,
            "net_revenue": 95.0,
        }
    ]
