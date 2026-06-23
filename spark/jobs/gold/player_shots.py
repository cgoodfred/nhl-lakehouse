"""Build the gold.player_shots fact table — every goal scored, denormalized.

Source:  nhl.silver.plays, nhl.silver.players, nhl.silver.teams, nhl.silver.games
Target:  nhl.gold.player_shots (Iceberg, partitioned by season)

One row per goal event with the player + team + game context joined in.
Designed for low-latency reads from BI/viz clients (Streamlit, DuckDB)
that want "show me all goals scored by X this season and where on the
ice they were taken from" without three joins on every query.

Filtered to:
  - type_desc_key = 'goal'
  - non-null scoring_player_id, x_coord, y_coord
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp

from common import get_spark


def transform_player_shots(
    plays_df: DataFrame,
    games_df: DataFrame,
    players_df: DataFrame,
    teams_df: DataFrame,
) -> DataFrame:
    """Filter plays to goals and denormalize player + team + game context.

    Splitting the transformation out of main() keeps it pure so tests can
    build four small DataFrames in memory and exercise the joins without
    hitting the catalog.
    """
    goals = plays_df.where(
        (col("type_desc_key") == "goal")
        & col("scoring_player_id").isNotNull()
        & col("x_coord").isNotNull()
        & col("y_coord").isNotNull()
    )

    return (
        goals.alias("p")
        .join(
            games_df.select("game_id", "game_date").alias("g"),
            col("p.game_id") == col("g.game_id"),
            "inner",
        )
        .join(
            players_df.select(
                col("player_id"),
                concat_ws(" ", col("first_name"), col("last_name")).alias("player_name"),
            ).alias("pl"),
            col("p.scoring_player_id") == col("pl.player_id"),
            "inner",
        )
        .join(
            teams_df.select(
                col("team_id").alias("event_owner_team_id"),
                col("abbrev").alias("event_owner_team_abbrev"),
            ).alias("t"),
            col("p.event_owner_team_id") == col("t.event_owner_team_id"),
            "inner",
        )
        .select(
            col("p.event_id").alias("event_id"),
            col("p.game_id").alias("game_id"),
            col("g.game_date").alias("game_date"),
            col("p.season").alias("season"),
            col("pl.player_id").alias("player_id"),
            col("pl.player_name").alias("player_name"),
            col("t.event_owner_team_id").alias("team_id"),
            col("t.event_owner_team_abbrev").alias("team_abbrev"),
            col("p.period_number").alias("period_number"),
            col("p.time_in_period").alias("time_in_period"),
            col("p.x_coord").alias("x_coord"),
            col("p.y_coord").alias("y_coord"),
            col("p.shot_type").alias("shot_type"),
            col("p.home_score").alias("home_score"),
            col("p.away_score").alias("away_score"),
            col("p.ppt_replay_url").alias("ppt_replay_url"),
            current_timestamp().alias("ingested_at"),
        )
    )


def main() -> None:
    spark = get_spark("gold-player-shots")

    plays = spark.read.table("nhl.silver.plays")
    games = spark.read.table("nhl.silver.games")
    players = spark.read.table("nhl.silver.players")
    teams = spark.read.table("nhl.silver.teams")

    shots = transform_player_shots(plays, games, players, teams)

    shots.writeTo("nhl.gold.player_shots").partitionedBy(col("season")).createOrReplace()

    written = spark.read.table("nhl.gold.player_shots").count()
    print(f"gold-player-shots: complete (rows={written})")


if __name__ == "__main__":
    main()
