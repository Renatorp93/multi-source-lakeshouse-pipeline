from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from lakehouse.config.settings import Settings
from lakehouse.ingestion.sales.normalize import SalesSnapshot


DDL_STATEMENTS = [
    """
    CREATE SCHEMA IF NOT EXISTS {schema};
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.customers (
        customer_id BIGINT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        source_system TEXT NOT NULL,
        ingestion_timestamp TIMESTAMPTZ NOT NULL,
        batch_id TEXT NOT NULL,
        load_date DATE NOT NULL,
        pipeline_run_id TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.products (
        product_id BIGINT PRIMARY KEY,
        sku TEXT,
        product_name TEXT,
        category TEXT,
        brand TEXT,
        price NUMERIC(12, 2),
        stock INTEGER,
        rating NUMERIC(5, 2),
        availability_status TEXT,
        source_system TEXT NOT NULL,
        ingestion_timestamp TIMESTAMPTZ NOT NULL,
        batch_id TEXT NOT NULL,
        load_date DATE NOT NULL,
        pipeline_run_id TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.orders (
        order_id BIGINT PRIMARY KEY,
        customer_id BIGINT REFERENCES {schema}.customers(customer_id),
        order_date DATE NOT NULL,
        gross_amount NUMERIC(12, 2),
        discounted_amount NUMERIC(12, 2),
        total_products INTEGER,
        total_quantity INTEGER,
        source_system TEXT NOT NULL,
        ingestion_timestamp TIMESTAMPTZ NOT NULL,
        batch_id TEXT NOT NULL,
        load_date DATE NOT NULL,
        pipeline_run_id TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.order_items (
        order_id BIGINT REFERENCES {schema}.orders(order_id),
        line_number INTEGER,
        product_id BIGINT REFERENCES {schema}.products(product_id),
        product_name TEXT,
        quantity INTEGER,
        unit_price NUMERIC(12, 2),
        gross_amount NUMERIC(12, 2),
        discount_percentage NUMERIC(6, 2),
        discounted_amount NUMERIC(12, 2),
        source_system TEXT NOT NULL,
        ingestion_timestamp TIMESTAMPTZ NOT NULL,
        batch_id TEXT NOT NULL,
        load_date DATE NOT NULL,
        pipeline_run_id TEXT NOT NULL,
        PRIMARY KEY (order_id, line_number)
    );
    """,
]


def seed_sales_snapshot(snapshot: SalesSnapshot, settings: Settings) -> None:
    schema = settings.postgres.schema_name
    with psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.database,
        user=settings.postgres.user,
        password=settings.postgres.password,
    ) as connection:
        with connection.cursor() as cursor:
            for statement in DDL_STATEMENTS:
                cursor.execute(statement.format(schema=schema))

            _upsert(cursor, f"{schema}.customers", snapshot.customers, ["customer_id"])
            _upsert(cursor, f"{schema}.products", snapshot.products, ["product_id"])
            _upsert(cursor, f"{schema}.orders", snapshot.orders, ["order_id"])
            _upsert(cursor, f"{schema}.order_items", snapshot.order_items, ["order_id", "line_number"])

        connection.commit()


def fetch_sales_rows(settings: Settings, entity: str) -> list[dict[str, Any]]:
    schema = settings.postgres.schema_name
    with psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.database,
        user=settings.postgres.user,
        password=settings.postgres.password,
        cursor_factory=RealDictCursor,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {schema}.{entity}")
            return [dict(row) for row in cursor.fetchall()]


def _upsert(cursor: Any, table_name: str, rows: list[dict[str, Any]], conflict_keys: list[str]) -> None:
    if not rows:
        return

    columns = list(rows[0].keys())
    values = [[row[column] for column in columns] for row in rows]

    assignments = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in columns if column not in conflict_keys
    )
    conflict_clause = ", ".join(conflict_keys)

    query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_clause}) DO UPDATE
        SET {assignments};
    """
    execute_values(cursor, query, values)
