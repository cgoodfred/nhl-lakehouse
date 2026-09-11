"""Normalize NHL shift-chart responses into ``nhl.silver.shifts``.

Source: s3a://nhl-bronze/shift-charts/season=*/date=*/game_*.json
Target: nhl.silver.shifts (Iceberg, partitioned by season)

The source endpoint returns ``{"data": [...]}``. Rows are keyed by
(game_id, player_id, period, shift_number), matching the legacy collector's
logical key. Source IDs are retained only as optional provenance and are not
used as the uniqueness key.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    lit,
    row_number,
    size,
    split,
    to_date,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from common import get_spark

BRONZE_PATH = "s3a://nhl-bronze/shift-charts/season=*/date=*/game_*.json"
BRONZE_BASE = "s3a://nhl-bronze/shift-charts"
MALFORMED_ROW_THRESHOLD = 0

SHIFT_SCHEMA = StructType(
    [
        StructField(
            "data",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("gameId", LongType()),
                        StructField("playerId", LongType()),
                        StructField("teamId", LongType()),
                        StructField("period", IntegerType()),
                        StructField("shiftNumber", IntegerType()),
                        StructField("startTime", StringType()),
                        StructField("endTime", StringType()),
                        StructField("duration", StringType()),
                        StructField("eventNumber", LongType()),
                        StructField("detailCode", LongType()),
                        StructField("eventDescription", StringType()),
                        StructField("eventDetails", StringType()),
                        StructField("typeCode", LongType()),
                    ]
                )
            ),
        )
    ]
)


def clock_seconds(value):
    """Parse an NHL ``MM:SS`` clock value into seconds.

    Invalid, blank, and null values become null so the caller can count and
    reject malformed rows instead of silently turning them into zeroes.
    """

    parts = split(value, ":")
    minutes = parts.getItem(0).cast("int")
    seconds = parts.getItem(1).cast("int")
    return when(
        value.isNotNull()
        & (value != "")
        & (size(parts) == 2)
        & minutes.isNotNull()
        & seconds.isNotNull()
        & (minutes >= 0)
        & (seconds >= 0)
        & (seconds < 60),
        minutes * lit(60) + seconds,
    )


def _project_shifts(raw: DataFrame) -> DataFrame:
    rows = raw.select("season", "date", explode("data").alias("s"))
    projected = rows.select(
        col("season").cast("int").alias("season"),
        to_date(col("date"), "yyyy-MM-dd").alias("game_date"),
        col("s.id").cast("long").alias("shift_id"),
        col("s.gameId").cast("long").alias("game_id"),
        col("s.playerId").cast("long").alias("player_id"),
        col("s.teamId").cast("long").alias("team_id"),
        col("s.period").cast("int").alias("period"),
        col("s.shiftNumber").cast("int").alias("shift_number"),
        clock_seconds(col("s.startTime")).alias("start_time_seconds"),
        clock_seconds(col("s.endTime")).alias("end_time_seconds"),
        clock_seconds(col("s.duration")).alias("duration_seconds"),
        col("s.eventNumber").cast("long").alias("event_number"),
        col("s.detailCode").cast("long").alias("detail_code"),
        col("s.eventDescription").alias("event_description"),
        col("s.eventDetails").alias("event_details"),
        col("s.typeCode").cast("long").alias("type_code"),
    )
    return projected.withColumn(
        "_malformed",
        col("game_date").isNull()
        | col("game_id").isNull()
        | col("player_id").isNull()
        | col("team_id").isNull()
        | col("period").isNull()
        | (col("period") <= 0)
        | col("shift_number").isNull()
        | (col("shift_number") <= 0)
        | col("start_time_seconds").isNull()
        | col("end_time_seconds").isNull()
        | col("duration_seconds").isNull()
        | (col("start_time_seconds") < 0)
        | (col("end_time_seconds") < 0)
        | (col("duration_seconds") < 0),
    )


def count_malformed_shifts(raw: DataFrame) -> int:
    return _project_shifts(raw).filter(col("_malformed")).count()


def transform_shifts(raw: DataFrame) -> DataFrame:
    """Filter malformed rows and deterministically deduplicate logical keys."""

    projected = _project_shifts(raw).filter(~col("_malformed")).drop("_malformed")
    key = ["game_id", "player_id", "period", "shift_number"]
    dedupe_window = Window.partitionBy(*key).orderBy(
        col("shift_id").asc_nulls_last(),
        col("start_time_seconds"),
        col("end_time_seconds"),
        col("duration_seconds"),
    )
    return (
        projected.withColumn("_row_number", row_number().over(dedupe_window))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
        .withColumn("ingested_at", current_timestamp())
    )


def main() -> None:
    spark = get_spark("silver-shifts")
    raw = (
        spark.read.option("basePath", BRONZE_BASE)
        .schema(SHIFT_SCHEMA)
        .json(BRONZE_PATH)
    )
    # Path partition discovery is explicit because date is not in the JSON
    # response. Keeping it as a separate column also makes fixture tests clear.
    raw = raw.withColumn("date", col("date"))
    malformed = count_malformed_shifts(raw)
    if malformed > MALFORMED_ROW_THRESHOLD:
        raise ValueError(
            f"silver-shifts: {malformed} malformed rows exceed threshold "
            f"{MALFORMED_ROW_THRESHOLD}"
        )
    shifts = transform_shifts(raw)
    raw_count = raw.select(explode("data")).count()
    deduplicated_count = shifts.count()
    shifts.writeTo("nhl.silver.shifts").partitionedBy(col("season")).createOrReplace()
    print(
        "silver-shifts: complete "
        f"(raw_rows={raw_count}, deduplicated_rows={deduplicated_count})"
    )


if __name__ == "__main__":
    main()
