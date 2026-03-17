from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.gold.service import build_and_persist_sales_gold


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_gold")

    logger.info("Iniciando construcao e persistencia da Gold de vendas")
    result = build_and_persist_sales_gold(settings)
    logger.info(
        "Gold persistida | batch_id=%s | source=%s | marts=%s",
        result.batch_id,
        result.source_name,
        result.mart_counts,
    )

    print(f"Gold source: {result.source_name}")
    print(f"Batch: {result.batch_id}")
    print(f"Marts: {result.mart_counts}")
    print(f"Gold path: {next(iter(result.mart_paths.values())).parent.parent}")


if __name__ == "__main__":
    main()
