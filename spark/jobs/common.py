"""Shared helpers for spark PySpark jobs (silver and gold tiers)."""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # Ensure both tier namespaces exist; Iceberg requires the namespace to
    # exist before tables can be created in it. Idempotent + cheap.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.gold")
    return spark
