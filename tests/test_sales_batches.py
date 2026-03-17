import shutil
from pathlib import Path

from lakehouse.ingestion.sales.batches import find_latest_batch, parse_batch_id


def test_find_latest_batch_returns_latest_common_directory():
    test_root = Path("./logs/test_sales_batches")
    if test_root.exists():
        shutil.rmtree(test_root)

    api_root = test_root / "api"
    csv_root = test_root / "csv"
    for root in (api_root, csv_root):
        root.mkdir(parents=True)

    (api_root / "20260317T120000Z").mkdir()
    (api_root / "20260317T130000Z").mkdir()
    (csv_root / "20260317T120000Z").mkdir()
    (csv_root / "20260317T130000Z").mkdir()
    (csv_root / "20260317T140000Z").mkdir()

    batch = find_latest_batch(api_root, csv_root)

    assert batch.batch_id == "20260317T130000Z"
    assert batch.api_dir == Path(api_root / "20260317T130000Z")


def test_parse_batch_id_returns_utc_datetime():
    parsed = parse_batch_id("20260317T145950Z")

    assert parsed.year == 2026
    assert parsed.month == 3
    assert parsed.tzinfo is not None
