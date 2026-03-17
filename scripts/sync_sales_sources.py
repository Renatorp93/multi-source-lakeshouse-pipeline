from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.ingestion.sales.service import sync_sales_sources


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_sync")

    logger.info("Iniciando sincronizacao das fontes de vendas")
    result = sync_sales_sources(settings)

    logger.info(
        "Sincronizacao concluida | batch_id=%s | customers=%s | products=%s | orders=%s | order_items=%s",
        result.batch_id,
        result.customers,
        result.products,
        result.orders,
        result.order_items,
    )
    print(f"API raw em: {result.api_dir}")
    print(f"CSV exportado em: {result.csv_dir}")
    print(f"Schema Postgres populado: {result.schema_name}")


if __name__ == "__main__":
    main()
