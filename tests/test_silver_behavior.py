from lakehouse.silver.sales import build_silver_sales_datasets


def test_build_silver_sales_datasets_normalizes_and_filters_invalid_sales_records():
    bronze_datasets = {
        "postgres": {
            "customers": [
                {
                    "customer_id": 10,
                    "first_name": "Ana",
                    "last_name": "Silva",
                    "email": " ANA@EXAMPLE.COM ",
                    "phone": "1111-1111",
                    "city": "sao paulo",
                    "state": "sp",
                    "country": "brasil",
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "hash-1",
                },
                {
                    "customer_id": 10,
                    "first_name": "Ana",
                    "last_name": "Silva",
                    "email": "ana@example.com",
                    "phone": "1111-1111",
                    "city": "sao paulo",
                    "state": "sp",
                    "country": "brasil",
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "hash-2",
                },
                {
                    "customer_id": 20,
                    "first_name": "Cliente",
                    "last_name": "SemEmail",
                    "email": None,
                    "phone": None,
                    "city": "rio de janeiro",
                    "state": "rj",
                    "country": "brasil",
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "hash-3",
                },
            ],
            "products": [
                {
                    "product_id": 100,
                    "sku": "SKU-100",
                    "product_name": "Notebook Pro",
                    "category": "LAPTOPS",
                    "brand": "tech",
                    "price": 7999.9,
                    "stock": 12,
                    "rating": 4.8,
                    "availability_status": "In Stock",
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "product-hash-1",
                },
                {
                    "product_id": None,
                    "sku": "SKU-X",
                    "product_name": None,
                    "category": "UNKNOWN",
                    "brand": None,
                    "price": 1.0,
                    "stock": 1,
                    "rating": 1.0,
                    "availability_status": "In Stock",
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "product-hash-2",
                },
            ],
            "orders": [
                {
                    "order_id": 1,
                    "customer_id": 10,
                    "order_date": "2026-03-10",
                    "gross_amount": 100.0,
                    "discounted_amount": 90.0,
                    "total_products": 1,
                    "total_quantity": 2,
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "order-hash-1",
                },
                {
                    "order_id": 2,
                    "customer_id": 999,
                    "order_date": "2026-03-11",
                    "gross_amount": 50.0,
                    "discounted_amount": 55.0,
                    "total_products": 1,
                    "total_quantity": 1,
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "order-hash-2",
                },
            ],
            "order_items": [
                {
                    "order_id": 1,
                    "line_number": 1,
                    "product_id": 100,
                    "product_name": "Notebook Pro",
                    "quantity": 2,
                    "unit_price": 50.0,
                    "gross_amount": 100.0,
                    "discount_percentage": 10.0,
                    "discounted_amount": 90.0,
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "item-hash-1",
                },
                {
                    "order_id": 2,
                    "line_number": 1,
                    "product_id": 100,
                    "product_name": "Notebook Pro",
                    "quantity": 1,
                    "unit_price": 50.0,
                    "gross_amount": 50.0,
                    "discount_percentage": 0.0,
                    "discounted_amount": 50.0,
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "item-hash-2",
                },
                {
                    "order_id": 1,
                    "line_number": 2,
                    "product_id": 100,
                    "product_name": "Notebook Pro",
                    "quantity": 0,
                    "unit_price": 10.0,
                    "gross_amount": 0.0,
                    "discount_percentage": 0.0,
                    "discounted_amount": 0.0,
                    "source_system": "postgres",
                    "ingestion_timestamp": "2026-03-17T12:00:00+00:00",
                    "batch_id": "20260317T120000Z",
                    "load_date": "2026-03-17",
                    "pipeline_run_id": "bronze-run",
                    "processing_timestamp": "2026-03-17T12:05:00+00:00",
                    "record_hash": "item-hash-3",
                },
            ],
        }
    }

    result = build_silver_sales_datasets(bronze_datasets, pipeline_run_id="silver-run")

    assert result.source_name == "postgres"
    assert len(result.datasets["customers"]) == 1
    assert result.datasets["customers"][0]["email"] == "ana@example.com"
    assert result.datasets["customers"][0]["city"] == "Sao Paulo"
    assert result.datasets["customers"][0]["state"] == "SP"
    assert len(result.datasets["products"]) == 1
    assert result.datasets["products"][0]["category"] == "laptops"
    assert result.datasets["products"][0]["brand"] == "Tech"
    assert len(result.datasets["orders"]) == 1
    assert len(result.datasets["order_items"]) == 1
    assert any(item["rule_name"] == "unique_customer_id" and item["status"] == "failed" for item in result.quality_results)
    assert any(item["rule_name"] == "not_null_email" and item["status"] == "failed" for item in result.quality_results)
    assert any(item["rule_name"] == "order_customer_exists" and item["status"] == "failed" for item in result.quality_results)


def test_build_silver_sales_datasets_falls_back_to_first_available_source():
    bronze_datasets = {
        "api": {
            "customers": [],
            "products": [],
            "orders": [],
            "order_items": [],
        }
    }

    result = build_silver_sales_datasets(bronze_datasets, pipeline_run_id="silver-run")

    assert result.source_name == "api"
    assert result.datasets["customers"] == []
