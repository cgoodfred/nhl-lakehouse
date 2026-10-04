"""Project bronze PBP envelopes into the silver.game_rosters bridge table.

Source:  s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json (rosterSpots[] array)
Target:  nhl.silver.game_rosters (Iceberg, one row per (game_id, player_id) pair,
         partitioned by season)

Bridge table semantics: NOT deduped. Captures the per-game state — which
team the player played for that night, sweater number that game, position
they lined up at. Companion to silver.players (which is the deduped dim).

Composite logical key: (game_id, player_id). Iceberg doesn't enforce
PKs; documented here so consumers know the grain.

The PBP envelope has both plays[] and rosterSpots[]. We declare only
rosterSpots — plays[] is skipped by the JSON parser.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, explode
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common import (
    get_spark,
    load_impact_scope,
    processing_arguments,
    source_paths,
    write_incremental,
)

BRONZE_PATH = "s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json"
BRONZE_BASE = "s3a://nhl-bronze/play-by-play"

_ROSTER_SPOT_STRUCT = StructType(
    [
        StructField("playerId", IntegerType()),
        StructField("teamId", IntegerType()),
        StructField("sweaterNumber", IntegerType()),
        StructField("positionCode", StringType()),
    ]
)

# `season` is NOT declared — comes from Hive-style path partition discovery.
ROSTERS_SCHEMA = StructType(
    [
        StructField("id", LongType()),  # game_id at the envelope top level
        StructField("rosterSpots", ArrayType(_ROSTER_SPOT_STRUCT)),
    ]
)


def transform_game_rosters(raw_df: DataFrame) -> DataFrame:
    """Explode rosterSpots[] without dedup — one row per (game_id, player_id).

    Splitting the transformation out of main() keeps it pure: tests can
    build raw_df from a fixture and assert on the output without touching
    S3 or the catalog.

    The input is the result of `spark.read.schema(ROSTERS_SCHEMA).json(...)`
    augmented with the `season` partition column from path discovery.
    """
    exploded = raw_df.select(
        col("id").alias("game_id"),
        col("season"),
        explode("rosterSpots").alias("r"),
    )

    return exploded.select(
        col("game_id"),
        col("r.playerId").alias("player_id"),
        col("season"),
        col("r.teamId").alias("team_id"),
        col("r.sweaterNumber").alias("sweater_number"),
        col("r.positionCode").alias("position_code"),
        current_timestamp().alias("ingested_at"),
    )


def main() -> None:
    spark = get_spark("silver-game-rosters")
    args = processing_arguments()
    scope = (
        None
        if args.processing_mode == "full"
        else load_impact_scope(spark, args.impact_manifest_key)
    )
    paths = source_paths(scope, BRONZE_PATH, "pbp")
    if not paths:
        print("silver.game_rosters: no refreshed PBP objects; nothing to write")
        return
    raw = spark.read.schema(ROSTERS_SCHEMA).option("basePath", BRONZE_BASE).json(paths)

    rosters = transform_game_rosters(raw)

    if scope is None:
        rosters.writeTo("nhl.silver.game_rosters").partitionedBy(col("season")).createOrReplace()
    else:
        write_incremental(spark, "nhl.silver.game_rosters", rosters, scope.pbp_game_ids)

    written = spark.read.table("nhl.silver.game_rosters").count()
    print(f"silver-game-rosters: complete (rows={written})")


if __name__ == "__main__":
    main()
