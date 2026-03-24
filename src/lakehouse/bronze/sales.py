from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def build_bronze_datasets(
    settings: Settings,
    postgres_fetcher: Any = fetch_sales_rows,
    on_source_error: str = "raise",
) -> tuple[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    batch = find_latest_sales_batch(settings)
    batch_timestamp = parse_batch_id(batch.batch_id)

    raw_api_payloads = load_raw_api_payloads(batch)
    api_snapshot = build_sales_snapshot(
        raw_api_payloads,
        pipeline_run_id=settings.pipeline_run_id,
        extracted_at=batch_timestamp,
    )

    datasets: dict[str, dict[str, list[dict[str, Any]]]] = {"api": {}, "csv": {}}

    for entity in ENTITY_SCHEMAS:
        datasets["api"][entity] = enrich_bronze_records(
            getattr(api_snapshot, entity),
            entity,
        )
        datasets["csv"][entity] = enrich_bronze_records(
            load_csv_records(batch.csv_dir / f"{entity}.csv"),
            entity,
        )

    try:
        postgres_datasets: dict[str, list[dict[str, Any]]] = {}
        for entity in ENTITY_SCHEMAS:
            postgres_datasets[entity] = enrich_bronze_records(
                postgres_fetcher(settings, entity),
                entity,
            )
        datasets["postgres"] = postgres_datasets
    except Exception:
        if on_source_error != "skip":
            raise

    return batch.batch_id, datasets


def load_latest_sales_batch_to_bronze(spark: SparkSession, settings: Settings) -> dict[str, dict[str, int]]:
    _, datasets = build_bronze_datasets(settings)

    summary: dict[str, dict[str, int]] = {"api": {}, "csv": {}, "postgres": {}}

    for source, entities in datasets.items():
        for entity, records in entities.items():
            dataframe = cast_bronze_dataframe(spark.createDataFrame(records), entity)
            write_bronze_table(dataframe, settings, source, entity)
            summary[source][entity] = len(records)

    return summary


def load_csv_records(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        return list(reader)


def enrich_bronze_records(
    records: list[dict[str, Any]],
    entity: str,
    processing_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    processing_timestamp = processing_timestamp or datetime.now(timezone.utc)
    processing_value = processing_timestamp.isoformat()
    schema = ENTITY_SCHEMAS[entity]
    enriched_records: list[dict[str, Any]] = []

    for record in records:
        normalized_record = {column_name: record.get(column_name) for column_name in schema}
        record_hash = hashlib.sha256(
            "||".join("" if normalized_record[column] is None else str(normalized_record[column]) for column in schema).encode("utf-8")
        ).hexdigest()
        normalized_record["processing_timestamp"] = processing_value
        normalized_record["record_hash"] = record_hash
        enriched_records.append(normalized_record)

    return enriched_records


def cast_bronze_dataframe(df: DataFrame, entity: str) -> DataFrame:
    schema = ENTITY_SCHEMAS[entity]

    projected_columns = []
    for column_name, column_type in schema.items():
        if column_name in df.columns:
            projected_columns.append(F.col(column_name).cast(column_type).alias(column_name))
        else:
            projected_columns.append(F.lit(None).cast(column_type).alias(column_name))

    projected_columns.append(F.col("processing_timestamp").cast("timestamp").alias("processing_timestamp"))
    projected_columns.append(F.col("record_hash").cast("string").alias("record_hash"))

    return df.select(*projected_columns)


def write_bronze_table(df: DataFrame, settings: Settings, source: str, entity: str) -> Path:
    target_path = resolve_path(settings.bronze_root) / source / entity
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path))
    )
    return target_path
