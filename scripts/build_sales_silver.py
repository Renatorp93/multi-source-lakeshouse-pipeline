from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.silver.service import build_and_persist_sales_silver


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_silver")

    logger.info("Iniciando construcao e persistencia da Silver de vendas")
    result = build_and_persist_sales_silver(settings)
    logger.info(
        "Silver persistida | batch_id=%s | source=%s | datasets=%s | quality_results=%s",
        result.batch_id,
        result.source_name,
        result.dataset_counts,
        result.quality_results_count,
    )

    print(f"Silver source: {result.source_name}")
    print(f"Batch: {result.batch_id}")
    print(f"Datasets: {result.dataset_counts}")
    print(f"Quality results: {result.quality_results_count}")
    print(f"Quality path: {result.quality_results_path}")


if __name__ == "__main__":
    main()
