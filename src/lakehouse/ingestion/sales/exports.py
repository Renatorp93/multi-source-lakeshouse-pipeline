from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from lakehouse.config.settings import Settings, resolve_path
from lakehouse.ingestion.sales.normalize import SalesSnapshot


def write_raw_api_payloads(snapshot: SalesSnapshot, settings: Settings) -> Path:
    output_dir = resolve_path(settings.landing_root) / "api" / "dummyjson_sales" / snapshot.batch_id
    output_dir.mkdir(parents=True, exist_ok=True)

    for resource_name, records in snapshot.raw_payloads.items():
        payload = {"resource": resource_name, "records": records}
        target = output_dir / f"{resource_name}.json"
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return output_dir


def write_sales_csv_exports(snapshot: SalesSnapshot, settings: Settings) -> Path:
    output_dir = resolve_path(settings.landing_root) / "csv" / "sales_exports" / snapshot.batch_id
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(output_dir / "customers.csv", snapshot.customers)
    _write_csv(output_dir / "products.csv", snapshot.products)
    _write_csv(output_dir / "orders.csv", snapshot.orders)
    _write_csv(output_dir / "order_items.csv", snapshot.order_items)

    return output_dir


def _write_csv(target: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    with target.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
