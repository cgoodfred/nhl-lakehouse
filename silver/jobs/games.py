"""Project bronze PBP envelopes into the silver.games table.

Source:  s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json
Target:  nhl.silver.games (Iceberg, one row per game)

The PBP envelope contains the same game header as the schedule endpoint plus
the plays[] and rosterSpots[] arrays. We declare only the game-header fields
we want in the StructType so the JSON parser skips the heavy arrays without
materializing them.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BRONZE_PATH = "s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json"
BRONZE_BASE = "s3a://nhl-bronze/play-by-play"

_NAME_STRUCT = StructType([StructField("default", StringType())])

_TEAM_STRUCT = StructType([
    StructField("id", IntegerType()),
    StructField("abbrev", StringType()),
    StructField("commonName", _NAME_STRUCT),
    StructField("score", IntegerType()),
    StructField("sog", IntegerType()),
])

# Only fields we want; missing arrays (plays, rosterSpots) are skipped by the
# JSON parser. `season` is intentionally not declared — it comes from the
# Hive-style path partition discovery (s3a://.../season=YYYYYYYY/...).
GAMES_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("gameType", IntegerType()),
    StructField("gameDate", StringType()),
    StructField("startTimeUTC", StringType()),
    StructField("easternUTCOffset", StringType()),
    StructField("venueUTCOffset", StringType()),
    StructField("venue", _NAME_STRUCT),
    StructField("venueLocation", _NAME_STRUCT),
    StructField("gameState", StringType()),
    StructField("gameScheduleState", StringType()),
    StructField("homeTeam", _TEAM_STRUCT),
    StructField("awayTeam", _TEAM_STRUCT),
    StructField("periodDescriptor", StructType([
        StructField("number", IntegerType()),
        StructField("periodType", StringType()),
        StructField("maxRegulationPeriods", IntegerType()),
    ])),
    StructField("gameOutcome", StructType([
        StructField("lastPeriodType", StringType()),
    ])),
    StructField("regPeriods", IntegerType()),
    StructField("maxPeriods", IntegerType()),
    StructField("limitedScoring", BooleanType()),
    StructField("shootoutInUse", BooleanType()),
    StructField("otInUse", BooleanType()),
])


def main() -> None:
    spark = SparkSession.builder.appName("silver-games").getOrCreate()

    # Iceberg requires the namespace to exist before tables can be created.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.silver")

    raw = (
        spark.read.schema(GAMES_SCHEMA)
        .option("basePath", BRONZE_BASE)
        .json(BRONZE_PATH)
    )

    games = raw.select(
        col("id").alias("game_id"),
        col("season"),  # from path partition discovery
        col("gameType").alias("game_type"),
        to_date("gameDate", "yyyy-MM-dd").alias("game_date"),
        to_timestamp("startTimeUTC").alias("start_time_utc"),
        col("easternUTCOffset").alias("eastern_utc_offset"),
        col("venueUTCOffset").alias("venue_utc_offset"),
        col("venue.default").alias("venue_name"),
        col("venueLocation.default").alias("venue_location"),
        col("gameState").alias("game_state"),
        col("gameScheduleState").alias("game_schedule_state"),
        col("homeTeam.id").alias("home_team_id"),
        col("homeTeam.abbrev").alias("home_team_abbrev"),
        col("homeTeam.commonName.default").alias("home_team_name"),
        col("homeTeam.score").alias("home_team_score"),
        col("homeTeam.sog").alias("home_team_sog"),
        col("awayTeam.id").alias("away_team_id"),
        col("awayTeam.abbrev").alias("away_team_abbrev"),
        col("awayTeam.commonName.default").alias("away_team_name"),
        col("awayTeam.score").alias("away_team_score"),
        col("awayTeam.sog").alias("away_team_sog"),
        col("periodDescriptor.number").alias("last_period_number"),
        col("periodDescriptor.periodType").alias("last_period_type"),
        col("gameOutcome.lastPeriodType").alias("game_outcome_last_period_type"),
        col("regPeriods").alias("reg_periods"),
        col("maxPeriods").alias("max_periods"),
        col("limitedScoring").alias("limited_scoring"),
        col("shootoutInUse").alias("shootout_in_use"),
        col("otInUse").alias("ot_in_use"),
        current_timestamp().alias("ingested_at"),
    )

    row_count = games.count()
    print(f"silver-games: writing {row_count} rows to nhl.silver.games")

    games.coalesce(1).writeTo("nhl.silver.games").createOrReplace()

    print(f"silver-games: complete (rows={row_count})")


if __name__ == "__main__":
    main()
