"""Shared helpers for silver PySpark jobs."""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.silver")
    return spark
