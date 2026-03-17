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


def find_latest_sales_batch(settings: Settings) -> SalesLandingBatch:
    landing_root = resolve_path(settings.landing_root)
    api_root = landing_root / "api" / "dummyjson_sales"
    csv_root = landing_root / "csv" / "sales_exports"
    return find_latest_batch(api_root, csv_root)


def load_raw_api_payloads(batch: SalesLandingBatch) -> dict[str, list[dict[str, Any]]]:
    payloads: dict[str, list[dict[str, Any]]] = {}
    for resource in ("users", "products", "carts"):
        resource_file = batch.api_dir / f"{resource}.json"
        payloads[resource] = json.loads(resource_file.read_text(encoding="utf-8"))["records"]
    return payloads


def parse_batch_id(batch_id: str) -> datetime:
    return datetime.strptime(batch_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
