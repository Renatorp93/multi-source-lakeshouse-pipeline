"""Transformacoes da camada Gold."""

from lakehouse.gold.sales import GoldBuildResult, build_gold_sales_marts
from lakehouse.gold.service import (
    GoldPersistedResult,
    build_and_persist_sales_gold,
    load_persisted_silver_result,
    persist_gold_result,
)

__all__ = [
    "GoldBuildResult",
    "GoldPersistedResult",
    "build_gold_sales_marts",
    "build_and_persist_sales_gold",
    "load_persisted_silver_result",
    "persist_gold_result",
]
