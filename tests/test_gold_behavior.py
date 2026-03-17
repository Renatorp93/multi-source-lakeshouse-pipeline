from lakehouse.gold.sales import build_gold_sales_marts


def test_build_gold_sales_marts_aggregates_daily_customer_and_product_views():
    silver_datasets = {
        "customers": [
            {"customer_id": 10, "first_name": "Ana", "last_name": "Silva", "email": "ana@example.com"},
            {"customer_id": 11, "first_name": "Bruno", "last_name": "Souza", "email": "bruno@example.com"},
        ],
        "products": [
            {"product_id": 100, "product_name": "Notebook Pro", "category": "laptops"},
            {"product_id": 101, "product_name": "Mouse Sem Fio", "category": "accessories"},
            {"product_id": 102, "product_name": "Headset Pro", "category": "accessories"},
        ],
        "orders": [
            {"order_id": 1, "customer_id": 10, "order_date": "2026-03-10", "gross_amount": 100.0, "discounted_amount": 90.0},
            {"order_id": 2, "customer_id": 10, "order_date": "2026-03-10", "gross_amount": 50.0, "discounted_amount": 50.0},
            {"order_id": 3, "customer_id": 11, "order_date": "2026-03-11", "gross_amount": 200.0, "discounted_amount": 180.0},
        ],
        "order_items": [
            {"order_id": 1, "line_number": 1, "product_id": 100, "product_name": "Notebook Pro", "quantity": 2, "gross_amount": 100.0, "discounted_amount": 90.0},
            {"order_id": 2, "line_number": 1, "product_id": 101, "product_name": "Mouse Sem Fio", "quantity": 1, "gross_amount": 50.0, "discounted_amount": 50.0},
            {"order_id": 3, "line_number": 1, "product_id": 100, "product_name": "Notebook Pro", "quantity": 1, "gross_amount": 120.0, "discounted_amount": 110.0},
            {"order_id": 3, "line_number": 2, "product_id": 102, "product_name": "Headset Pro", "quantity": 2, "gross_amount": 80.0, "discounted_amount": 70.0},
        ],
    }

    result = build_gold_sales_marts(silver_datasets)

    assert result.marts["daily_sales"] == [
        {
            "order_date": "2026-03-10",
            "order_count": 2,
            "customer_count": 1,
            "items_sold": 3,
            "gross_revenue": 150.0,
            "net_revenue": 140.0,
            "average_ticket": 70.0,
        },
        {
            "order_date": "2026-03-11",
            "order_count": 1,
            "customer_count": 1,
            "items_sold": 3,
            "gross_revenue": 200.0,
            "net_revenue": 180.0,
            "average_ticket": 180.0,
        },
    ]
    assert result.marts["customer_sales"] == [
        {
            "customer_id": 10,
            "customer_name": "Ana Silva",
            "email": "ana@example.com",
            "order_count": 2,
            "items_sold": 3,
            "gross_revenue": 150.0,
            "net_revenue": 140.0,
            "last_order_date": "2026-03-10",
        },
        {
            "customer_id": 11,
            "customer_name": "Bruno Souza",
            "email": "bruno@example.com",
            "order_count": 1,
            "items_sold": 3,
            "gross_revenue": 200.0,
            "net_revenue": 180.0,
            "last_order_date": "2026-03-11",
        },
    ]
    assert result.marts["product_sales"] == [
        {
            "product_id": 100,
            "product_name": "Notebook Pro",
            "category": "laptops",
            "order_count": 2,
            "units_sold": 3,
            "gross_revenue": 220.0,
            "net_revenue": 200.0,
        },
        {
            "product_id": 102,
            "product_name": "Headset Pro",
            "category": "accessories",
            "order_count": 1,
            "units_sold": 2,
            "gross_revenue": 80.0,
            "net_revenue": 70.0,
        },
        {
            "product_id": 101,
            "product_name": "Mouse Sem Fio",
            "category": "accessories",
            "order_count": 1,
            "units_sold": 1,
            "gross_revenue": 50.0,
            "net_revenue": 50.0,
        },
    ]
