from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config.settings import Settings, resolve_path
from lakehouse.ingestion.sales.batches import find_latest_sales_batch, load_raw_api_payloads, parse_batch_id
from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from lakehouse.ingestion.sales.postgres import fetch_sales_rows


ENTITY_SCHEMAS = {
    "customers": {
        "customer_id": "bigint",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "phone": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "source_system": "string",
        "ingestion_timestamp": "timestamp",
        "batch_id": "string",
        "load_date": "date",
        "pipeline_run_id": "string",
    },
    "products": {
        "product_id": "bigint",
        "sku": "string",
        "product_name": "string",
        "category": "string",
        "brand": "string",
        "price": "double",
        "stock": "int",
        "rating": "double",
        "availability_status": "string",
        "source_system": "string",
        "ingestion_timestamp": "timestamp",
        "batch_id": "string",
        "load_date": "date",
        "pipeline_run_id": "string",
    },
    "orders": {
        "order_id": "bigint",
        "customer_id": "bigint",
        "order_date": "date",
        "gross_amount": "double",
        "discounted_amount": "double",
        "total_products": "int",
        "total_quantity": "int",
        "source_system": "string",
        "ingestion_timestamp": "timestamp",
        "batch_id": "string",
        "load_date": "date",
        "pipeline_run_id": "string",
    },
    "order_items": {
        "order_id": "bigint",
        "line_number": "int",
        "product_id": "bigint",
        "product_name": "string",
        "quantity": "int",
        "unit_price": "double",
        "gross_amount": "double",
        "discount_percentage": "double",
        "discounted_amount": "double",
        "source_system": "string",
        "ingestion_timestamp": "timestamp",
        "batch_id": "string",
        "load_date": "date",
        "pipeline_run_id": "string",
    },
}


def load_latest_sales_batch_to_bronze(spark: SparkSession, settings: Settings) -> dict[str, dict[str, int]]:
    batch = find_latest_sales_batch(settings)
    batch_timestamp = parse_batch_id(batch.batch_id)

    raw_api_payloads = load_raw_api_payloads(batch)
    api_snapshot = build_sales_snapshot(
        raw_api_payloads,
        pipeline_run_id=settings.pipeline_run_id,
        extracted_at=batch_timestamp,
    )

    summary: dict[str, dict[str, int]] = {"api": {}, "csv": {}, "postgres": {}}

    for entity in ENTITY_SCHEMAS:
        api_records = getattr(api_snapshot, entity)
        api_df = standardize_bronze_dataframe(spark.createDataFrame(api_records), entity)
        write_bronze_table(api_df, settings, "api", entity)
        summary["api"][entity] = api_df.count()

        csv_path = batch.csv_dir / f"{entity}.csv"
        csv_df = spark.read.option("header", True).csv(str(csv_path))
        csv_df = standardize_bronze_dataframe(csv_df, entity)
        write_bronze_table(csv_df, settings, "csv", entity)
        summary["csv"][entity] = csv_df.count()

        postgres_records = fetch_sales_rows(settings, entity)
        postgres_df = standardize_bronze_dataframe(spark.createDataFrame(postgres_records), entity)
        write_bronze_table(postgres_df, settings, "postgres", entity)
        summary["postgres"][entity] = postgres_df.count()

    return summary


def standardize_bronze_dataframe(df: DataFrame, entity: str) -> DataFrame:
    schema = ENTITY_SCHEMAS[entity]

    projected_columns = []
    for column_name, column_type in schema.items():
        if column_name in df.columns:
            projected_columns.append(F.col(column_name).cast(column_type).alias(column_name))
        else:
            projected_columns.append(F.lit(None).cast(column_type).alias(column_name))

    standardized_df = df.select(*projected_columns)
    hash_columns = [
        F.coalesce(F.col(column_name).cast("string"), F.lit(""))
        for column_name in schema
    ]

    return standardized_df.withColumn("processing_timestamp", F.current_timestamp()).withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *hash_columns), 256),
    )


def write_bronze_table(df: DataFrame, settings: Settings, source: str, entity: str) -> Path:
    target_path = resolve_path(settings.bronze_root) / source / entity
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path))
    )
    return target_path
