import json
from pathlib import Path

from lakehouse.silver.service import load_persisted_silver_result
from tests.support import build_test_settings


def test_load_persisted_silver_result_returns_counts_and_paths_for_requested_batch():
    settings = build_test_settings("test_silver_persisted_loading")
    settings.ensure_directories()

    batch_id = "20260317T160000Z"
    source_name = "postgres"
    datasets = {
        "customers": [{"customer_id": 10}],
        "products": [{"product_id": 100}, {"product_id": 101}],
        "orders": [{"order_id": 1}],
        "order_items": [{"order_id": 1, "line_number": 1}],
    }
    for entity, records in datasets.items():
        target_dir = Path(settings.silver_root) / source_name / entity
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / f"{batch_id}.json").write_text(json.dumps(records), encoding="utf-8")

    quality_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    quality_dir.mkdir(parents=True, exist_ok=True)
    (quality_dir / f"{batch_id}.json").write_text(
        json.dumps(
            [
                {"rule_name": "not_null_email", "status": "passed"},
                {"rule_name": "positive_quantity", "status": "passed"},
            ]
        ),
        encoding="utf-8",
    )

    result = load_persisted_silver_result(settings, batch_id=batch_id)

    assert result.source_name == source_name
    assert result.batch_id == batch_id
    assert result.dataset_counts == {
        "customers": 1,
        "products": 2,
        "orders": 1,
        "order_items": 1,
    }
    assert result.quality_results_count == 2
    assert result.quality_results_path == quality_dir / f"{batch_id}.json"


def test_load_persisted_silver_result_fails_when_no_source_has_full_batch():
    settings = build_test_settings("test_silver_persisted_loading_missing")
    settings.ensure_directories()

    batch_id = "20260317T170000Z"
    quality_dir = Path(settings.silver_root) / "monitoring" / "quality_results"
    quality_dir.mkdir(parents=True, exist_ok=True)
    (quality_dir / f"{batch_id}.json").write_text(
        json.dumps([{"rule_name": "not_null_email", "status": "passed"}]),
        encoding="utf-8",
    )

    incomplete_dir = Path(settings.silver_root) / "api" / "customers"
    incomplete_dir.mkdir(parents=True, exist_ok=True)
    (incomplete_dir / f"{batch_id}.json").write_text(json.dumps([]), encoding="utf-8")

    try:
        load_persisted_silver_result(settings, batch_id=batch_id)
    except FileNotFoundError as exc:
        message = str(exc)
    else:
        raise AssertionError("Era esperado falhar quando a Silver persistida do batch esta incompleta.")

    assert batch_id in message
    assert "order_items" in message
