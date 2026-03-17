from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from lakehouse.config.settings import Settings, resolve_path
from lakehouse.gold.sales import GoldBuildResult, build_gold_sales_marts
from lakehouse.silver.service import SilverPersistedResult, build_and_persist_sales_silver


@dataclass(frozen=True)
class GoldPersistedResult:
    source_name: str
    batch_id: str
    mart_paths: dict[str, Path]
    mart_counts: dict[str, int]


def load_silver_datasets(dataset_paths: dict[str, Path]) -> dict[str, list[dict[str, Any]]]:
    loaded: dict[str, list[dict[str, Any]]] = {}
    for entity, path in dataset_paths.items():
        loaded[entity] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def persist_gold_result(
    gold_result: GoldBuildResult,
    settings: Settings,
    batch_id: str,
    source_name: str,
) -> GoldPersistedResult:
    gold_root = resolve_path(settings.gold_root)
    mart_paths: dict[str, Path] = {}
    mart_counts: dict[str, int] = {}

    for mart_name, records in gold_result.marts.items():
        target_dir = gold_root / "sales" / mart_name
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{batch_id}.json"
        target_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        mart_paths[mart_name] = target_path
        mart_counts[mart_name] = len(records)

    return GoldPersistedResult(
        source_name=source_name,
        batch_id=batch_id,
        mart_paths=mart_paths,
        mart_counts=mart_counts,
    )


def build_and_persist_sales_gold(
    settings: Settings,
    silver_pipeline: Callable[[Settings], SilverPersistedResult] = build_and_persist_sales_silver,
    silver_loader: Callable[[dict[str, Path]], dict[str, list[dict[str, Any]]]] = load_silver_datasets,
    gold_builder: Callable[[dict[str, list[dict[str, Any]]]], GoldBuildResult] = build_gold_sales_marts,
    persister: Callable[[GoldBuildResult, Settings, str, str], GoldPersistedResult] = persist_gold_result,
) -> GoldPersistedResult:
    silver_result = silver_pipeline(settings)
    silver_datasets = silver_loader(silver_result.dataset_paths)
    gold_result = gold_builder(silver_datasets)
    return persister(gold_result, settings, silver_result.batch_id, silver_result.source_name)
