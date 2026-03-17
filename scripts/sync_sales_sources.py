from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.ingestion.sales.dummyjson import DummyJsonClient
from lakehouse.ingestion.sales.exports import write_raw_api_payloads, write_sales_csv_exports
from lakehouse.ingestion.sales.normalize import build_sales_snapshot
from lakehouse.ingestion.sales.postgres import seed_sales_snapshot


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_sync")

    logger.info("Iniciando sincronizacao das fontes de vendas")
    client = DummyJsonClient(settings.api.base_url)
    raw_payloads = client.fetch_sales_payloads()

    snapshot = build_sales_snapshot(raw_payloads, pipeline_run_id=settings.pipeline_run_id)
    api_dir = write_raw_api_payloads(snapshot, settings)
    csv_dir = write_sales_csv_exports(snapshot, settings)
    seed_sales_snapshot(snapshot, settings)

    logger.info(
        "Sincronizacao concluida | batch_id=%s | customers=%s | products=%s | orders=%s | order_items=%s",
        snapshot.batch_id,
        len(snapshot.customers),
        len(snapshot.products),
        len(snapshot.orders),
        len(snapshot.order_items),
    )
    print(f"API raw em: {api_dir}")
    print(f"CSV exportado em: {csv_dir}")
    print(f"Schema Postgres populado: {settings.postgres.schema_name}")


if __name__ == "__main__":
    main()
