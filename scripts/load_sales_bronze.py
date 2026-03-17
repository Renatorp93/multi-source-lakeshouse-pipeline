from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.bronze.sales import load_latest_sales_batch_to_bronze
from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.spark.session import create_spark_session


def main() -> None:
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_bronze")

    logger.info("Iniciando carga da Landing para Bronze")
    spark = create_spark_session(settings)

    try:
        summary = load_latest_sales_batch_to_bronze(spark, settings)
    finally:
        spark.stop()

    for source, entities in summary.items():
        logger.info("Bronze concluida | source=%s | entities=%s", source, entities)
        print(f"{source}: {entities}")


if __name__ == "__main__":
    main()
