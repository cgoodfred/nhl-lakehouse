"""Normalize per-goal tracking JSON from bronze into typed silver frames.

Source:  s3a://nhl-bronze/tracking/season=*/game_id=*/event_id=*/tracking.json
Target:  nhl.silver.tracking_frames (Iceberg, partitioned by season)

Bronze tracking files are top-level JSON arrays, one element per frame at
~10Hz. We read them with multiLine=true so Spark yields one ROW per frame
(instead of the default line-delimited assumption). Partition discovery
surfaces season / game_id / event_id from the bronze path layout — the
bronze writer's event_id={event_id}/tracking.json structure is load-
bearing here; a flat event_id=NNN.json filename would lose the column.

The on-disk onIce shape is a MAP keyed by string ids:
  - Key "1" is the puck (with empty-string player fields)
  - All other keys are players (with real playerId / sweater / team data)

Silver schema:
  - puck_x_in, puck_y_in, puck_x_ft, puck_y_ft  — split out for fast filters
  - on_ice: array<struct> of all PLAYERS (puck excluded)
  - Both _in (source inches, corner origin) and _ft (PBP feet, center origin)
    materialized so analysts don't need to know the conversion.
  - frame_index: 0-based within the goal, derived from row_number() over
    (game_id, event_id) ordered by timeStamp — the source has no index column.
  - rel_seconds: (timeStamp - max(timeStamp) over goal) / 10.0; deciseconds
    is the verified feed unit. Last frame is 0.0; everything else is negative.

Scheduled runs receive the ingest impact manifest and read only tracking
objects successfully written for its refreshed game IDs. Full-table
replacement remains available through explicit full processing mode for
backfills.

Python 3.8 in the apache/spark:3.5.7-python3 base image: see tracking_ingest
for the same constraints (no `X | None` runtime exprs, no `datetime.UTC`).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lit,
    row_number,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from common import get_spark, load_impact_scope, processing_arguments, write_incremental

BRONZE_PATH = "s3a://nhl-bronze/tracking"
BRONZE_GLOB = f"{BRONZE_PATH}/season=*/game_id=*/event_id=*/tracking.json"


def tracking_paths(game_ids: list[int]) -> list[str]:
    """Build partition-pruned tracking paths for the affected games."""

    return [
        f"{BRONZE_PATH}/season=*/game_id={game_id}/event_id=*/tracking.json"
        for game_id in game_ids
    ]


def refreshed_tracking_paths(spark, game_ids: list[int]) -> list[str]:
    """Return only tracking objects successfully written by tracking-ingest.

    PBP refreshes can include preseason games or goals without a tracking URL.
    Those games are valid inputs for the rest of the pipeline but have no
    tracking object to read. The attempts table is the authoritative bridge
    between per-event fetch outcomes and bronze object keys.
    """

    if not game_ids or not spark.catalog.tableExists("nhl.silver.tracking_attempts"):
        return []
    rows = (
        spark.read.table("nhl.silver.tracking_attempts")
        .where(
            col("game_id").isin(game_ids)
            & (col("status") == "success")
            & col("source_object_key").isNotNull()
        )
        .select("source_object_key")
        .distinct()
        .collect()
    )
    return ["s3a://nhl-bronze/" + row.source_object_key for row in rows]


# NHL PPT coordinate system: tracking inches with origin at the corner.
# Rink is 2400x1020 inches → 200x85 feet. Convert to PBP feet (center origin)
# for analyst-friendly axes that match silver.plays.x_coord / y_coord.
PPT_INCHES_PER_FT = 12.0
PPT_CENTER_X_IN = 1200.0
PPT_CENTER_Y_IN = 510.0

# Players keyed by various string ids; the puck is always key "1" with empty
# string fields for everything except its x/y. We declare playerId as a
# string here (since the source mixes "" for the puck and ints for players)
# and cast to long after filtering the puck out.
ON_ICE_ENTRY = StructType(
    [
        StructField("id", LongType()),
        StructField("playerId", StringType()),
        StructField("x", DoubleType()),
        StructField("y", DoubleType()),
        StructField("sweaterNumber", StringType()),
        StructField("teamId", StringType()),
        StructField("teamAbbrev", StringType()),
    ]
)

BRONZE_FRAME_SCHEMA = StructType(
    [
        StructField("timeStamp", LongType()),
        StructField("onIce", MapType(StringType(), ON_ICE_ENTRY)),
    ]
)


def transform_tracking_frames(raw: DataFrame) -> DataFrame:
    """Pure transformation: raw bronze rows -> typed silver rows.

    Splitting this out lets the fixture test exercise the full chain
    (puck split, on_ice transform, frame_index, rel_seconds, _ft
    materialization) without a real partition-discovery read."""

    # Window functions can't appear inside a selectExpr that also references
    # nested ops, so add them first.
    ordered_w = Window.partitionBy("game_id", "event_id").orderBy("timeStamp")
    goal_w = Window.partitionBy("game_id", "event_id")
    with_indices = raw.withColumn("frame_index", row_number().over(ordered_w) - lit(1)).withColumn(
        "rel_seconds", (col("timeStamp") - spark_max("timeStamp").over(goal_w)) / lit(10.0)
    )

    # The transform() higher-order function reads cleanly in SQL but is much
    # noisier in PySpark column DSL. The puck "1" entry is filtered out via
    # map_filter; the remaining player entries are transformed into the final
    # struct shape with both source-inches and derived-feet coordinates.
    on_ice_expr = (
        "transform("
        "  map_values(map_filter(onIce, (k, v) -> k != '1')),"
        "  e -> named_struct("
        f"    'player_id',   cast(e.playerId      as bigint),"
        f"    'sweater',     cast(e.sweaterNumber as int),"
        f"    'team_id',     cast(e.teamId        as int),"
        f"    'team_abbrev', e.teamAbbrev,"
        f"    'x_in',        e.x,"
        f"    'y_in',        e.y,"
        f"    'x_ft',        (e.x - {PPT_CENTER_X_IN}) / {PPT_INCHES_PER_FT},"
        f"    'y_ft',        (e.y - {PPT_CENTER_Y_IN}) / {PPT_INCHES_PER_FT}"
        "  )"
        ")"
    )

    puck_x = col("onIce")["1"]["x"]
    puck_y = col("onIce")["1"]["y"]
    return (
        with_indices.withColumn("puck_x_in", puck_x)
        .withColumn("puck_y_in", puck_y)
        .withColumn("puck_x_ft", (puck_x - lit(PPT_CENTER_X_IN)) / lit(PPT_INCHES_PER_FT))
        .withColumn("puck_y_ft", (puck_y - lit(PPT_CENTER_Y_IN)) / lit(PPT_INCHES_PER_FT))
        .withColumn("on_ice", expr(on_ice_expr))
        .select(
            # Partition-discovered game_id/event_id come through as IntegerType
            # (the values fit in int32). Cast to long for schema parity with
            # silver.plays.{game_id,event_id} so joins land cleanly without
            # implicit widening per row.
            col("game_id").cast("long").alias("game_id"),
            col("event_id").cast("long").alias("event_id"),
            col("season"),
            col("frame_index").cast("int").alias("frame_index"),
            col("timeStamp").cast("long").alias("timestamp_ds"),
            col("rel_seconds"),
            col("puck_x_in"),
            col("puck_y_in"),
            col("puck_x_ft"),
            col("puck_y_ft"),
            col("on_ice"),
            current_timestamp().alias("ingested_at"),
        )
    )


def main() -> None:
    spark = get_spark("silver-tracking-frames")
    args = processing_arguments()
    scope = (
        None
        if args.processing_mode == "full"
        else load_impact_scope(spark, args.impact_manifest_key)
    )
    if scope is not None and not scope.pbp_game_ids:
        print("silver.tracking_frames: no refreshed PBP games; nothing to write")
        return
    paths = (
        BRONZE_GLOB
        if scope is None
        else refreshed_tracking_paths(spark, scope.pbp_game_ids)
    )
    if scope is not None and not paths:
        print("silver.tracking_frames: no successful tracking objects; nothing to write")
        return

    raw = (
        spark.read.option("multiLine", "true")
        .option("basePath", BRONZE_PATH)  # not a .json() kwarg in PySpark
        .schema(BRONZE_FRAME_SCHEMA)
        .json(paths)
    )

    out = transform_tracking_frames(raw)

    if scope is None:
        out.writeTo("nhl.silver.tracking_frames").partitionedBy("season").createOrReplace()
    else:
        write_incremental(spark, "nhl.silver.tracking_frames", out, scope.pbp_game_ids)

    written = spark.read.table("nhl.silver.tracking_frames").count()
    print(f"silver-tracking-frames: complete (rows={written})")


if __name__ == "__main__":
    main()
