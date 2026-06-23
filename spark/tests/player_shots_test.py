"""Tests for gold.jobs.player_shots transformation.

Source is silver (not bronze) so tests build four in-memory DataFrames
matching the subset of silver columns the transform actually reads,
then exercise the goal filter + the player/team/game joins.
"""

import datetime

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from player_shots import transform_player_shots

PLAYS_SCHEMA = StructType([
    StructField("event_id", LongType()),
    StructField("game_id", LongType()),
    StructField("season", IntegerType()),
    StructField("type_desc_key", StringType()),
    StructField("scoring_player_id", IntegerType()),
    StructField("event_owner_team_id", IntegerType()),
    StructField("period_number", IntegerType()),
    StructField("time_in_period", StringType()),
    StructField("x_coord", IntegerType()),
    StructField("y_coord", IntegerType()),
    StructField("shot_type", StringType()),
    StructField("home_score", IntegerType()),
    StructField("away_score", IntegerType()),
    StructField("ppt_replay_url", StringType()),
])

GAMES_SCHEMA = StructType([
    StructField("game_id", LongType()),
    StructField("game_date", DateType()),
])

PLAYERS_SCHEMA = StructType([
    StructField("player_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
])

TEAMS_SCHEMA = StructType([
    StructField("team_id", IntegerType()),
    StructField("abbrev", StringType()),
])

# Test data:
#   game 2024020001 on 2024-10-08 — 1 goal (event 100), 1 shot-on-goal (event 101),
#     1 goal with null coords (event 102 — must be filtered)
#   game 2024020055 on 2024-10-25 — 1 goal (event 200)
# Columns (positional with PLAYS_SCHEMA above):
#   event_id, game_id, season, type_desc_key, scoring_player_id,
#   event_owner_team_id, period_number, time_in_period, x_coord, y_coord,
#   shot_type, home_score, away_score, ppt_replay_url
_URL_100 = "https://wsr.nhle.com/sprites/x/100.json"
_URL_200 = "https://wsr.nhle.com/sprites/x/200.json"
PLAYS_DATA = [
    (100, 2024020001, 20242025, "goal", 8480113, 52, 1, "01:23", -73, 3, "wrist", 0, 1, _URL_100),
    (101, 2024020001, 20242025, "shot-on-goal", 8477942, 26, 2, "05:00", 85, 9, "wrist", 0, 1, None),  # noqa: E501
    (102, 2024020001, 20242025, "goal", 8478403, 26, 3, "10:00", None, None, "snap", 1, 1, None),
    (200, 2024020055, 20242025, "goal", 8471685, 26, 2, "08:45", 60, -20, "snap", 2, 1, _URL_200),
]

GAMES_DATA = [
    (2024020001, datetime.date(2024, 10, 8)),
    (2024020055, datetime.date(2024, 10, 25)),
]

PLAYERS_DATA = [
    (8480113, "Alex",    "Iafallo"),
    (8471685, "Anze",    "Kopitar"),
    (8477942, "Mark",    "Scheifele"),  # shoots in fixture but not a goal scorer
    (8478403, "Quinton", "Byfield"),    # null-coord goal — should be filtered out
]

TEAMS_DATA = [
    (26, "LAK"),
    (52, "WPG"),
]


def _df(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


def _build(spark):
    return (
        _df(spark, PLAYS_SCHEMA, PLAYS_DATA),
        _df(spark, GAMES_SCHEMA, GAMES_DATA),
        _df(spark, PLAYERS_SCHEMA, PLAYERS_DATA),
        _df(spark, TEAMS_SCHEMA, TEAMS_DATA),
    )


def _by_event(shots_df):
    return {row.event_id: row for row in shots_df.collect()}


def test_filters_to_goals_with_coords(spark):
    # 4 plays in fixture: 2 goals with coords, 1 goal without coords, 1 SOG.
    # Only the 2 goals with coords should survive.
    plays, games, players, teams = _build(spark)
    shots = transform_player_shots(plays, games, players, teams)
    assert shots.count() == 2


def test_joins_produce_player_and_team_names(spark):
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))

    iafallo_goal = by_event[100]
    assert iafallo_goal.player_name == "Alex Iafallo"
    assert iafallo_goal.team_abbrev == "WPG"
    assert iafallo_goal.game_date == datetime.date(2024, 10, 8)
    assert iafallo_goal.x_coord == -73
    assert iafallo_goal.y_coord == 3
    assert iafallo_goal.shot_type == "wrist"
    assert iafallo_goal.ppt_replay_url == "https://wsr.nhle.com/sprites/x/100.json"

    kopitar_goal = by_event[200]
    assert kopitar_goal.player_name == "Anze Kopitar"
    assert kopitar_goal.team_abbrev == "LAK"
    assert kopitar_goal.game_date == datetime.date(2024, 10, 25)


def test_non_goal_events_excluded(spark):
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))
    # The shot-on-goal event (101) and the null-coord goal (102) must not appear.
    assert 101 not in by_event
    assert 102 not in by_event


def test_required_fields_non_null(spark):
    plays, games, players, teams = _build(spark)
    for row in transform_player_shots(plays, games, players, teams).collect():
        assert row.event_id is not None
        assert row.game_id is not None
        assert row.game_date is not None
        assert row.season is not None
        assert row.player_id is not None
        assert row.player_name is not None
        assert row.team_id is not None
        assert row.team_abbrev is not None
        assert row.x_coord is not None
        assert row.y_coord is not None
