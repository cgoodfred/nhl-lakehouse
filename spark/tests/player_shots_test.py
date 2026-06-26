"""Tests for gold.jobs.player_shots transformation.

Source is silver (not bronze) so tests build four in-memory DataFrames
matching the subset of silver columns the transform actually reads,
then exercise the goal filter + the player/team/game joins.
"""

import datetime

from pyspark.sql.types import (
    BooleanType,
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
    StructField("period_type", StringType()),
    StructField("time_in_period", StringType()),
    StructField("x_coord", IntegerType()),
    StructField("y_coord", IntegerType()),
    StructField("shot_type", StringType()),
    StructField("strength_state", StringType()),
    StructField("is_empty_net", BooleanType()),
    StructField("home_score", IntegerType()),
    StructField("away_score", IntegerType()),
    StructField("ppt_replay_url", StringType()),
])

GAMES_SCHEMA = StructType([
    StructField("game_id", LongType()),
    StructField("game_date", DateType()),
    StructField("game_type", IntegerType()),
    StructField("home_team_abbrev", StringType()),
])

PLAYERS_SCHEMA = StructType([
    StructField("player_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("headshot", StringType()),
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
#   event_owner_team_id, period_number, period_type, time_in_period,
#   x_coord, y_coord, shot_type, strength_state, is_empty_net,
#   home_score, away_score, ppt_replay_url
_URL_100 = "https://wsr.nhle.com/sprites/x/100.json"
_URL_200 = "https://wsr.nhle.com/sprites/x/200.json"
PLAYS_DATA = [
    (100, 2024020001, 20242025, "goal", 8480113, 52, 1, "REG", "01:23", -73, 3, "wrist", "PP", False, 0, 1, _URL_100),  # noqa: E501
    (101, 2024020001, 20242025, "shot-on-goal", 8477942, 26, 2, "REG", "05:00", 85, 9, "wrist", "EV", False, 0, 1, None),  # noqa: E501
    (102, 2024020001, 20242025, "goal", 8478403, 26, 3, "REG", "10:00", None, None, "snap", "EV", False, 1, 1, None),  # noqa: E501
    (200, 2024020055, 20242025, "goal", 8471685, 26, 2, "REG", "08:45", 60, -20, "snap", "EV", True, 2, 1, _URL_200),  # noqa: E501
    # event 300 is a real goal but in a 4 Nations Face-Off game (game_type=19).
    # Should be filtered out by the NHL-only join.
    (300, 2024190001, 20242025, "goal", 8480113, 52, 1, "REG", "12:00", 70, 4, "wrist", "EV", False, 1, 0, None),  # noqa: E501
]

GAMES_DATA = [
    (2024020001, datetime.date(2024, 10, 8), 2, "WPG"),    # regular season, WPG home
    (2024020055, datetime.date(2024, 10, 25), 2, "LAK"),   # regular season, LAK home
    (2024190001, datetime.date(2025, 2, 12), 19, "SWE"),   # 4 Nations — filtered
]

PLAYERS_DATA = [
    (8480113, "Alex",    "Iafallo",   "https://assets.nhle.com/mugs/nhl/20242025/WPG/8480113.png"),
    (8471685, "Anze",    "Kopitar",   "https://assets.nhle.com/mugs/nhl/20242025/LAK/8471685.png"),
    (8477942, "Mark",    "Scheifele", "https://assets.nhle.com/mugs/nhl/20242025/WPG/8477942.png"),
    (8478403, "Quinton", "Byfield",   "https://assets.nhle.com/mugs/nhl/20242025/LAK/8478403.png"),
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
    # 5 plays in fixture: 3 goals with coords (one in a 4-Nations game),
    # 1 goal without coords, 1 SOG. The 4-Nations goal must be excluded
    # by the NHL-only game-type filter; the no-coords goal by the coord
    # filter; the SOG by the type_desc_key filter. -> 2 surviving goals.
    plays, games, players, teams = _build(spark)
    shots = transform_player_shots(plays, games, players, teams)
    assert shots.count() == 2


def test_non_nhl_game_types_excluded(spark):
    # Event 300 is a goal in a game_type=19 (4 Nations Face-Off) game.
    # The NHL-only filter at the games join must drop it.
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))
    assert 300 not in by_event


def test_game_type_column_propagated(spark):
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))
    # Both surviving goals are regular season (game_type=2) in the fixture.
    for event_id in (100, 200):
        assert by_event[event_id].game_type == 2


def test_player_headshot_joined_from_players(spark):
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))
    iafallo_goal = by_event[100]
    kopitar_goal = by_event[200]
    assert iafallo_goal.player_headshot == (
        "https://assets.nhle.com/mugs/nhl/20242025/WPG/8480113.png"
    )
    assert kopitar_goal.player_headshot == (
        "https://assets.nhle.com/mugs/nhl/20242025/LAK/8471685.png"
    )


def test_home_team_abbrev_joined_from_games(spark):
    plays, games, players, teams = _build(spark)
    by_event = _by_event(transform_player_shots(plays, games, players, teams))
    # event 100 → game 2024020001 → home WPG
    assert by_event[100].home_team_abbrev == "WPG"
    # event 200 → game 2024020055 → home LAK
    assert by_event[200].home_team_abbrev == "LAK"


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
    assert iafallo_goal.period_type == "REG"
    assert iafallo_goal.strength_state == "PP"
    assert iafallo_goal.is_empty_net is False
    assert iafallo_goal.ppt_replay_url == "https://wsr.nhle.com/sprites/x/100.json"

    kopitar_goal = by_event[200]
    assert kopitar_goal.player_name == "Anze Kopitar"
    assert kopitar_goal.team_abbrev == "LAK"
    assert kopitar_goal.game_date == datetime.date(2024, 10, 25)
    assert kopitar_goal.strength_state == "EV"
    assert kopitar_goal.is_empty_net is True


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
