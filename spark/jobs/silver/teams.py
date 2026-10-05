"""Derive the silver.teams dim from silver.games.

Source:  nhl.silver.games (NOT bronze — first silver-from-silver job)
Target:  nhl.silver.teams (Iceberg, one row per unique teamId)

Reads the home/away team columns from silver.games, unions them into a
single per-team-per-game projection, then groups by team_id and uses
max_by(field, struct(game_date, game_id)) to deterministically pick the
latest abbrev and name. struct sort key tie-breaks on game_id when
multiple games share the same date.

~32 NHL teams; abbrev/name changes are rare (relocations, rebrands) but
handled correctly — the latest known value wins.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, max, max_by, min, struct

from common import get_spark, load_impact_scope, processing_arguments


def transform_teams(games_df: DataFrame) -> DataFrame:
    """Dedup teams across home/away appearances; pick latest abbrev/name.

    Splitting the transformation out of main() keeps it pure so tests can
    build a games-shaped DataFrame in memory and exercise the logic
    without hitting the catalog.
    """
    home = games_df.select(
        col("home_team_id").alias("team_id"),
        col("home_team_abbrev").alias("abbrev"),
        col("home_team_name").alias("name"),
        col("game_date"),
        col("game_id"),
    )
    away = games_df.select(
        col("away_team_id").alias("team_id"),
        col("away_team_abbrev").alias("abbrev"),
        col("away_team_name").alias("name"),
        col("game_date"),
        col("game_id"),
    )

    sort_key = struct(col("game_date"), col("game_id"))

    teams = (
        home.union(away)
        .groupBy("team_id")
        .agg(
            max_by(col("abbrev"), sort_key).alias("abbrev"),
            max_by(col("name"), sort_key).alias("name"),
            min("game_date").alias("first_seen_date"),
            max("game_date").alias("last_seen_date"),
        )
    )

    return teams.withColumn("ingested_at", current_timestamp())


def merge_teams(existing: DataFrame, refreshed: DataFrame) -> DataFrame:
    """Merge a bounded game projection into the small teams dimension."""

    combined = existing.select(refreshed.columns).withColumn("_merge_rank", lit(0)).unionByName(
        refreshed.withColumn("_merge_rank", lit(1))
    )
    sort_key = struct(col("last_seen_date"), col("_merge_rank"))
    return combined.groupBy("team_id").agg(
        max_by(col("abbrev"), sort_key).alias("abbrev"),
        max_by(col("name"), sort_key).alias("name"),
        min("first_seen_date").alias("first_seen_date"),
        max("last_seen_date").alias("last_seen_date"),
    ).withColumn("ingested_at", current_timestamp())


def main() -> None:
    spark = get_spark("silver-teams")
    args = processing_arguments()
    scope = (
        None
        if args.processing_mode == "full"
        else load_impact_scope(spark, args.impact_manifest_key)
    )

    games = spark.read.table("nhl.silver.games")
    if scope is not None:
        if not scope.pbp_game_ids:
            print("silver.teams: no refreshed PBP games; nothing to write")
            return
        games = games.where(col("game_id").isin(scope.pbp_game_ids))

    refreshed = transform_teams(games)
    if scope is None or not spark.catalog.tableExists("nhl.silver.teams"):
        teams = refreshed
    else:
        teams = merge_teams(spark.read.table("nhl.silver.teams"), refreshed)

    teams.coalesce(1).writeTo("nhl.silver.teams").createOrReplace()

    written = spark.read.table("nhl.silver.teams").count()
    print(f"silver-teams: complete (rows={written})")


if __name__ == "__main__":
    main()
