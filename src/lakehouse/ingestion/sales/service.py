from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from lakehouse.config.settings import Settings
from lakehouse.ingestion.sales.dummyjson import DummyJsonClient
from lakehouse.ingestion.sales.exports import write_raw_api_payloads, write_sales_csv_exports
from lakehouse.ingestion.sales.normalize import SalesSnapshot, build_sales_snapshot
from lakehouse.ingestion.sales.postgres import seed_sales_snapshot


class SalesClient(Protocol):
    def fetch_sales_payloads(self) -> dict[str, list[dict]]:
        """Busca os payloads brutos de vendas."""


class SnapshotSeeder(Protocol):
    def __call__(self, snapshot: SalesSnapshot, settings: Settings) -> None:
        """Persiste o snapshot em um destino."""


@dataclass(frozen=True)
class SalesSyncResult:
    batch_id: str
    api_dir: Path
    csv_dir: Path
    customers: int
    products: int
    orders: int
    order_items: int
    schema_name: str


def sync_sales_sources(
    settings: Settings,
    client: SalesClient | None = None,
    seeder: SnapshotSeeder = seed_sales_snapshot,
) -> SalesSyncResult:
    active_client = client or DummyJsonClient(settings.api.base_url)
    raw_payloads = active_client.fetch_sales_payloads()

    snapshot = build_sales_snapshot(raw_payloads, pipeline_run_id=settings.pipeline_run_id)
    api_dir = write_raw_api_payloads(snapshot, settings)
    csv_dir = write_sales_csv_exports(snapshot, settings)
    seeder(snapshot, settings)

    return SalesSyncResult(
        batch_id=snapshot.batch_id,
        api_dir=api_dir,
        csv_dir=csv_dir,
        customers=len(snapshot.customers),
        products=len(snapshot.products),
        orders=len(snapshot.orders),
        order_items=len(snapshot.order_items),
        schema_name=settings.postgres.schema_name,
    )
