"""Project bronze PBP envelopes into the silver.players dim table.

Source:  s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json (rosterSpots[] array)
Target:  nhl.silver.players (Iceberg, one row per unique playerId)

SCD-1 semantics: latest known first/last name, position, and headshot
URL per player. `max_by(..., game_date)` gives deterministic latest-value
semantics — plain `last(...)` after groupBy is non-deterministic in Spark
and would silently produce different results run-to-run.

The PBP envelope has both plays[] and rosterSpots[]. We declare only the
rosterSpots fields we need; plays[] is skipped by the JSON parser.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    max,
    max_by,
    min,
    struct,
    to_date,
)
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
    # NHL CDN headshot URL — embeds the player's CURRENT team and the season,
    # so it changes when a player is traded. max_by(.., (game_date, game_id))
    # below picks the most recent value, matching how we resolve first/last
    # name and position.
    StructField("headshot", StringType()),
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
        col("id").alias("game_id"),
        to_date("gameDate", "yyyy-MM-dd").alias("game_date"),
        explode("rosterSpots").alias("r"),
    )

    # Tie-break with game_id so a player appearing in multiple games on the
    # same date deterministically picks the higher game_id. Struct comparison
    # is field-by-field, so game_date is the primary key and game_id breaks ties.
    sort_key = struct(col("game_date"), col("game_id"))

    players = exploded.groupBy(col("r.playerId").alias("player_id")).agg(
        max_by(col("r.firstName.default"), sort_key).alias("first_name"),
        max_by(col("r.lastName.default"), sort_key).alias("last_name"),
        max_by(col("r.positionCode"), sort_key).alias("position_code"),
        max_by(col("r.headshot"), sort_key).alias("headshot"),
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
