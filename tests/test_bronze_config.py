from lakehouse.bronze.sales import ENTITY_SCHEMAS


def test_customers_bronze_schema_contains_metadata_fields():
    customers_schema = ENTITY_SCHEMAS["customers"]

    assert customers_schema["customer_id"] == "bigint"
    assert customers_schema["ingestion_timestamp"] == "timestamp"
    assert customers_schema["pipeline_run_id"] == "string"
