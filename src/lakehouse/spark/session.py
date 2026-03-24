from __future__ import annotations

import os
import shutil
from typing import Any

from pyspark.sql import SparkSession

from lakehouse.config.settings import Settings


def build_spark_builder(settings: Settings) -> SparkSession.Builder:
    """Monta o builder padrao do Spark para o projeto."""
    return (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.executor.memory", settings.spark.executor_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", settings.spark.warehouse_dir)
        .config("spark.jars.packages", settings.spark.delta_package)
    )


def ensure_java_available() -> None:
    java_home = os.getenv("JAVA_HOME")
    java_on_path = shutil.which("java")

    if java_home or java_on_path:
        return

    raise RuntimeError(
        "Java nao encontrado. Instale Java 17+ e configure JAVA_HOME antes de executar cargas Spark/Delta."
    )


def create_spark_session(settings: Settings, extra_configs: dict[str, Any] | None = None) -> SparkSession:
    """Cria uma SparkSession com suporte a Delta Lake."""
    from delta import configure_spark_with_delta_pip

    ensure_java_available()
    builder = build_spark_builder(settings)

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()
