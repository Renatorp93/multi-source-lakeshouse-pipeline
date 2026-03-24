from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.config.settings import get_settings
from lakehouse.quality.gates import emit_quality_alert


def main() -> None:
    parser = argparse.ArgumentParser(description="Emite um resumo operacional para falhas do quality gate.")
    parser.add_argument("--batch-id", dest="batch_id", default=None)
    args = parser.parse_args()

    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_quality_alert")

    message = emit_quality_alert(settings, batch_id=args.batch_id)
    logger.warning(message)
    print(message)


if __name__ == "__main__":
    main()
