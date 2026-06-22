"""Project bronze PBP envelopes into the silver.plays table.

Source:  s3a://nhl-bronze/play-by-play/season=*/date=*/game_*.json
Target:  nhl.silver.plays (Iceberg, one row per play event, partitioned by season)

The PBP envelope's `plays[]` array is the heavy payload — ~300 events per
game. We declare only the play-level fields we want; the JSON parser skips
the rest. Every per-event-type field lives in `details.*` and is declared
nullable, since each `typeDescKey` populates only a subset.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, explode, substring, when
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

# Union of every `details.X` field we observed across event types in bronze.
# All nullable — any given event populates a subset.
_DETAILS_STRUCT = StructType([
    # Coordinates / team
    StructField("xCoord", IntegerType()),
    StructField("yCoord", IntegerType()),
    StructField("zoneCode", StringType()),
    StructField("eventOwnerTeamId", IntegerType()),
    # Player refs (generic + per-event-type)
    StructField("playerId", IntegerType()),
    StructField("blockingPlayerId", IntegerType()),
    StructField("shootingPlayerId", IntegerType()),
    StructField("losingPlayerId", IntegerType()),
    StructField("winningPlayerId", IntegerType()),
    StructField("scoringPlayerId", IntegerType()),
    StructField("assist1PlayerId", IntegerType()),
    StructField("assist2PlayerId", IntegerType()),
    StructField("goalieInNetId", IntegerType()),
    StructField("hittingPlayerId", IntegerType()),
    StructField("hitteePlayerId", IntegerType()),
    StructField("committedByPlayerId", IntegerType()),
    StructField("drawnByPlayerId", IntegerType()),
    # Goal stats
    StructField("scoringPlayerTotal", IntegerType()),
    StructField("assist1PlayerTotal", IntegerType()),
    StructField("assist2PlayerTotal", IntegerType()),
    # Shot info
    StructField("shotType", StringType()),
    StructField("reason", StringType()),
    # Score state
    StructField("awayScore", IntegerType()),
    StructField("homeScore", IntegerType()),
    StructField("awaySOG", IntegerType()),
    StructField("homeSOG", IntegerType()),
    # Penalty
    StructField("typeCode", StringType()),
    StructField("descKey", StringType()),
    StructField("duration", IntegerType()),
    # Highlight URLs (goal-only)
    StructField("highlightClipSharingUrl", StringType()),
    StructField("highlightClipSharingUrlFr", StringType()),
    StructField("highlightClip", LongType()),
    StructField("highlightClipFr", LongType()),
    StructField("discreteClip", LongType()),
    StructField("discreteClipFr", LongType()),
])

_PERIOD_STRUCT = StructType([
    StructField("number", IntegerType()),
    StructField("periodType", StringType()),
])

_PLAY_STRUCT = StructType([
    StructField("eventId", LongType()),
    StructField("sortOrder", IntegerType()),
    StructField("typeCode", IntegerType()),
    StructField("typeDescKey", StringType()),
    StructField("periodDescriptor", _PERIOD_STRUCT),
    StructField("timeInPeriod", StringType()),
    StructField("timeRemaining", StringType()),
    StructField("situationCode", StringType()),
    StructField("homeTeamDefendingSide", StringType()),
    StructField("details", _DETAILS_STRUCT),
])

# `season` is not declared — comes from path partition discovery.
PLAYS_SCHEMA = StructType([
    StructField("id", LongType()),  # game_id at the envelope top level
    StructField("plays", ArrayType(_PLAY_STRUCT)),
])


def transform_plays(raw_df: DataFrame) -> DataFrame:
    """Explode plays[] and project to the silver.plays row shape.

    Splitting the transformation out of main() keeps it pure: the test in
    silver/tests/plays_test.py can build raw_df from a fixture and assert
    on the output without touching S3 or the catalog.

    The input is the result of `spark.read.schema(PLAYS_SCHEMA).json(...)`
    augmented with the `season` partition column from path discovery.
    """
    exploded = raw_df.select(
        col("id").alias("game_id"),
        col("season"),
        explode("plays").alias("p"),
    )

    projected = exploded.select(
        col("game_id"),
        col("season"),
        col("p.eventId").alias("event_id"),
        col("p.sortOrder").alias("sort_order"),
        col("p.typeCode").alias("type_code"),
        col("p.typeDescKey").alias("type_desc_key"),
        col("p.periodDescriptor.number").alias("period_number"),
        col("p.periodDescriptor.periodType").alias("period_type"),
        col("p.timeInPeriod").alias("time_in_period"),
        col("p.timeRemaining").alias("time_remaining"),
        col("p.homeTeamDefendingSide").alias("home_team_defending_side"),
        col("p.situationCode").alias("situation_code"),
        # Details: coordinates / event-owning team
        col("p.details.xCoord").alias("x_coord"),
        col("p.details.yCoord").alias("y_coord"),
        col("p.details.zoneCode").alias("zone_code"),
        col("p.details.eventOwnerTeamId").alias("event_owner_team_id"),
        # Details: player refs
        col("p.details.playerId").alias("player_id"),
        col("p.details.blockingPlayerId").alias("blocking_player_id"),
        col("p.details.shootingPlayerId").alias("shooting_player_id"),
        col("p.details.losingPlayerId").alias("losing_player_id"),
        col("p.details.winningPlayerId").alias("winning_player_id"),
        col("p.details.scoringPlayerId").alias("scoring_player_id"),
        col("p.details.assist1PlayerId").alias("assist1_player_id"),
        col("p.details.assist2PlayerId").alias("assist2_player_id"),
        col("p.details.goalieInNetId").alias("goalie_in_net_id"),
        col("p.details.hittingPlayerId").alias("hitting_player_id"),
        col("p.details.hitteePlayerId").alias("hittee_player_id"),
        col("p.details.committedByPlayerId").alias("committed_by_player_id"),
        col("p.details.drawnByPlayerId").alias("drawn_by_player_id"),
        # Details: goal stats
        col("p.details.scoringPlayerTotal").alias("scoring_player_total"),
        col("p.details.assist1PlayerTotal").alias("assist1_player_total"),
        col("p.details.assist2PlayerTotal").alias("assist2_player_total"),
        # Details: shot info
        col("p.details.shotType").alias("shot_type"),
        col("p.details.reason").alias("reason"),
        # Details: score state
        col("p.details.awayScore").alias("away_score"),
        col("p.details.homeScore").alias("home_score"),
        col("p.details.awaySOG").alias("away_sog"),
        col("p.details.homeSOG").alias("home_sog"),
        # Details: penalty (typeCode and descKey are also play-level field names
        # in the broader API, but here they're penalty-specific — prefix to disambiguate)
        col("p.details.typeCode").alias("penalty_type_code"),
        col("p.details.descKey").alias("penalty_desc_key"),
        col("p.details.duration").alias("penalty_duration"),
        # Details: highlight URLs (goal-only)
        col("p.details.highlightClipSharingUrl").alias("highlight_clip_sharing_url"),
        col("p.details.highlightClipSharingUrlFr").alias("highlight_clip_sharing_url_fr"),
        col("p.details.highlightClip").alias("highlight_clip"),
        col("p.details.highlightClipFr").alias("highlight_clip_fr"),
        col("p.details.discreteClip").alias("discrete_clip"),
        col("p.details.discreteClipFr").alias("discrete_clip_fr"),
    )

    # situationCode is a 4-char string: <away_goalie><away_skaters><home_skaters><home_goalie>
    # e.g. "1551" = both goalies present, 5v5 even strength.
    # strength_state is from the home team's perspective; is_empty_net is true if either
    # goalie has been pulled.
    with_situation = (
        projected
        .withColumn("away_goalie_present", substring(col("situation_code"), 1, 1) == "1")
        .withColumn("away_skaters", substring(col("situation_code"), 2, 1).cast("int"))
        .withColumn("home_skaters", substring(col("situation_code"), 3, 1).cast("int"))
        .withColumn("home_goalie_present", substring(col("situation_code"), 4, 1) == "1")
        .withColumn(
            "strength_state",
            when(col("home_skaters") > col("away_skaters"), "PP")
            .when(col("home_skaters") < col("away_skaters"), "SH")
            .otherwise("EV"),
        )
        .withColumn(
            "is_empty_net",
            (~col("away_goalie_present")) | (~col("home_goalie_present")),
        )
    )

    return with_situation.withColumn("ingested_at", current_timestamp())


def main() -> None:
    spark = get_spark("silver-plays")

    raw = (
        spark.read.schema(PLAYS_SCHEMA)
        .option("basePath", BRONZE_BASE)
        .json(BRONZE_PATH)
    )

    plays = transform_plays(raw)

    plays.writeTo("nhl.silver.plays").partitionedBy(col("season")).createOrReplace()

    written = spark.read.table("nhl.silver.plays").count()
    print(f"silver-plays: complete (rows={written})")


if __name__ == "__main__":
    main()
