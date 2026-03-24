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
from lakehouse.quality.gates import build_quality_gate_report, enforce_quality_gate, format_quality_gate_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida os quality_results persistidos da Silver.")
    parser.add_argument("--batch-id", dest="batch_id", default=None)
    args = parser.parse_args()

    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.logs_root)
    logger = get_logger("lakehouse.sales_quality_gate")

    report = build_quality_gate_report(settings, batch_id=args.batch_id)
    summary = format_quality_gate_summary(report)
    logger.info(
        "Quality gate analisado | batch_id=%s | total_rules=%s | failed_rules=%s | failed_records=%s",
        report.batch_id,
        report.total_rules,
        report.failed_rules,
        report.failed_records,
    )
    print(summary)
    enforce_quality_gate(report)


if __name__ == "__main__":
    main()
