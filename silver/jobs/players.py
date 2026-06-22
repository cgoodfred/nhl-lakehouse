"""Project bronze PBP envelopes into the silver.players dim table.

Source:  s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json (rosterSpots[] array)
Target:  nhl.silver.players (Iceberg, one row per unique playerId)

SCD-1 semantics: latest known first/last name and position per player.
`max_by(..., game_date)` gives deterministic latest-value semantics —
plain `last(...)` after groupBy is non-deterministic in Spark and would
silently produce different results run-to-run.

The PBP envelope has both plays[] and rosterSpots[]. We declare only the
rosterSpots fields we need; plays[] is skipped by the JSON parser.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, explode, max, max_by, min, to_date
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common import get_spark

BRONZE_PATH = "s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json"
BRONZE_BASE = "s3a://nhl-bronze/play-by-play"

_NAME_STRUCT = StructType([StructField("default", StringType())])

_ROSTER_SPOT_STRUCT = StructType([
    StructField("playerId", IntegerType()),
    StructField("firstName", _NAME_STRUCT),
    StructField("lastName", _NAME_STRUCT),
    StructField("positionCode", StringType()),
])

PLAYERS_SCHEMA = StructType([
    StructField("id", LongType()),  # game_id at the envelope top level
    StructField("gameDate", StringType()),
    StructField("rosterSpots", ArrayType(_ROSTER_SPOT_STRUCT)),
])


def transform_players(raw_df: DataFrame) -> DataFrame:
    """Explode rosterSpots[] and dedup to one row per player.

    Splitting the transformation out of main() keeps it pure: tests can
    build raw_df from a fixture and assert on the output without touching
    S3 or the catalog.
    """
    exploded = raw_df.select(
        to_date("gameDate", "yyyy-MM-dd").alias("game_date"),
        explode("rosterSpots").alias("r"),
    )

    players = exploded.groupBy(col("r.playerId").alias("player_id")).agg(
        max_by(col("r.firstName.default"), col("game_date")).alias("first_name"),
        max_by(col("r.lastName.default"), col("game_date")).alias("last_name"),
        max_by(col("r.positionCode"), col("game_date")).alias("position_code"),
        min("game_date").alias("first_seen_date"),
        max("game_date").alias("last_seen_date"),
    )

    return players.withColumn("ingested_at", current_timestamp())


def main() -> None:
    spark = get_spark("silver-players")

    raw = (
        spark.read.schema(PLAYERS_SCHEMA)
        .option("basePath", BRONZE_BASE)
        .json(BRONZE_PATH)
    )

    players = transform_players(raw)

    players.coalesce(1).writeTo("nhl.silver.players").createOrReplace()

    written = spark.read.table("nhl.silver.players").count()
    print(f"silver-players: complete (rows={written})")


if __name__ == "__main__":
    main()
