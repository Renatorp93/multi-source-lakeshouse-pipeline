from lakehouse.config.settings import get_settings
from lakehouse.spark.session import build_spark_builder


def test_build_spark_builder_contains_delta_package():
    settings = get_settings()

    builder = build_spark_builder(settings)

    assert builder._options["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert builder._options["spark.jars.packages"] == "io.delta:delta-spark_2.12:3.2.0"
