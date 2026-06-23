"""Tests for silver.jobs.teams transformation.

teams reads from silver.games (not bronze), so tests build a
games-shaped DataFrame in memory with spark.createDataFrame rather
than reading a JSON fixture. Covers dedup across home/away
appearances, max_by latest-value semantics for abbrev/name changes,
and the struct(game_date, game_id) tie-break for same-date games.
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

from teams import transform_teams

GAMES_SCHEMA = StructType([
    StructField("game_id", LongType()),
    StructField("game_date", DateType()),
    StructField("home_team_id", IntegerType()),
    StructField("home_team_abbrev", StringType()),
    StructField("home_team_name", StringType()),
    StructField("away_team_id", IntegerType()),
    StructField("away_team_abbrev", StringType()),
    StructField("away_team_name", StringType()),
])

# Test scenario:
#   - LAK (26) and WPG (52) appear in normal games on different dates.
#   - Team 99 appears as "OLD" on 2024-10-09 and "NEW" on 2024-10-25 →
#     max_by must pick "NEW" because of later game_date.
#   - Team 88 appears as "VER1" and "VER2" on the SAME date (2024-10-25)
#     in different games → struct tie-break must pick "VER2" because of
#     higher game_id (2024020099 > 2024020055).
FIXTURE_GAMES = [
    (2024020001, datetime.date(2024, 10, 8),  26, "LAK",  "Kings",   52, "WPG",  "Jets"),
    (2024020003, datetime.date(2024, 10, 9),  99, "OLD",  "OldTeam", 26, "LAK",  "Kings"),
    (2024020055, datetime.date(2024, 10, 25), 88, "VER1", "V1Team",  26, "LAK",  "Kings"),
    (2024020099, datetime.date(2024, 10, 25), 99, "NEW",  "NewTeam", 88, "VER2", "V2Team"),
]


def _games(spark):
    return spark.createDataFrame(FIXTURE_GAMES, GAMES_SCHEMA)


def _by_team(teams_df):
    return {row.team_id: row for row in teams_df.collect()}


def test_dedup_count(spark):
    # 4 unique teams across 4 games: LAK(26), WPG(52), 99, 88
    teams = transform_teams(_games(spark))
    assert teams.count() == 4


def test_key_fields_non_null(spark):
    teams = transform_teams(_games(spark))
    for row in teams.collect():
        assert row.team_id is not None
        assert row.abbrev is not None
        assert row.name is not None
        assert row.first_seen_date is not None
        assert row.last_seen_date is not None


def test_max_by_picks_latest_abbrev_for_renamed_team(spark):
    # Team 99: "OLD" on 2024-10-09, "NEW" on 2024-10-25.
    # max_by(struct(game_date, game_id)) picks the later date → "NEW".
    by_id = _by_team(transform_teams(_games(spark)))
    team = by_id[99]
    assert team.abbrev == "NEW"
    assert team.name == "NewTeam"
    assert team.first_seen_date == datetime.date(2024, 10, 9)
    assert team.last_seen_date == datetime.date(2024, 10, 25)


def test_same_date_tie_break_by_game_id(spark):
    # Team 88: "VER1" in game 2024020055 and "VER2" in game 2024020099,
    # both on 2024-10-25. struct tie-break picks higher game_id → "VER2".
    by_id = _by_team(transform_teams(_games(spark)))
    team = by_id[88]
    assert team.abbrev == "VER2"
    assert team.name == "V2Team"


def test_team_seen_in_home_and_away_dedups(spark):
    # LAK (26) appears as home in game 1 and as away in games 3, 55, and 99.
    # Should produce a single row with date span covering all 4 appearances.
    by_id = _by_team(transform_teams(_games(spark)))
    lak = by_id[26]
    assert lak.abbrev == "LAK"
    assert lak.name == "Kings"
    assert lak.first_seen_date == datetime.date(2024, 10, 8)
    assert lak.last_seen_date == datetime.date(2024, 10, 25)


def test_single_appearance_team_collapses_dates(spark):
    # WPG (52) only appears in game 1 — first and last collapse to the same date.
    by_id = _by_team(transform_teams(_games(spark)))
    wpg = by_id[52]
    assert wpg.first_seen_date == datetime.date(2024, 10, 8)
    assert wpg.last_seen_date == datetime.date(2024, 10, 8)
