from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse.config.settings import Settings, resolve_path


@dataclass(frozen=True)
class SalesLandingBatch:
    batch_id: str
    api_dir: Path
    csv_dir: Path


def find_latest_batch(api_root: Path, csv_root: Path) -> SalesLandingBatch:
    api_batches = {path.name for path in api_root.iterdir() if path.is_dir()}
    csv_batches = {path.name for path in csv_root.iterdir() if path.is_dir()}
    common_batches = sorted(api_batches & csv_batches)

    if not common_batches:
        raise FileNotFoundError("Nenhum batch comum encontrado entre API e CSV na Landing.")

    batch_id = common_batches[-1]
    return SalesLandingBatch(batch_id=batch_id, api_dir=api_root / batch_id, csv_dir=csv_root / batch_id)


def find_latest_usable_batch(api_root: Path, csv_root: Path) -> SalesLandingBatch:
    latest_common_batch = find_latest_batch(api_root, csv_root)
    api_batches = {path.name for path in api_root.iterdir() if path.is_dir()}
    csv_batches = {path.name for path in csv_root.iterdir() if path.is_dir()}
    common_batches = sorted(api_batches & csv_batches, reverse=True)

    for batch_id in common_batches:
        if not _is_test_batch(csv_root / batch_id):
            return SalesLandingBatch(batch_id=batch_id, api_dir=api_root / batch_id, csv_dir=csv_root / batch_id)

    return latest_common_batch


def find_latest_sales_batch(settings: Settings) -> SalesLandingBatch:
    landing_root = resolve_path(settings.landing_root)
    api_root = landing_root / "api" / "dummyjson_sales"
    csv_root = landing_root / "csv" / "sales_exports"
    return find_latest_usable_batch(api_root, csv_root)


def load_raw_api_payloads(batch: SalesLandingBatch) -> dict[str, list[dict[str, Any]]]:
    payloads: dict[str, list[dict[str, Any]]] = {}
    for resource in ("users", "products", "carts"):
        resource_file = batch.api_dir / f"{resource}.json"
        payloads[resource] = json.loads(resource_file.read_text(encoding="utf-8"))["records"]
    return payloads


def parse_batch_id(batch_id: str) -> datetime:
    return datetime.strptime(batch_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def _is_test_batch(csv_batch_dir: Path) -> bool:
    customers_file = csv_batch_dir / "customers.csv"
    if not customers_file.exists():
        return False

    with customers_file.open("r", encoding="utf-8") as stream:
        lines = [line.strip() for line in stream.readlines() if line.strip()]

    if len(lines) < 2:
        return False

    headers = lines[0].split(",")
    values = lines[1].split(",")
    if "pipeline_run_id" not in headers:
        return False

    pipeline_run_id = values[headers.index("pipeline_run_id")]
    return pipeline_run_id.startswith("test")
