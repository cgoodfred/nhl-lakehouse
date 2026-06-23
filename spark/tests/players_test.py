"""Tests for silver.jobs.players transformation.

Reads two fixture envelopes (different games on different dates) to
exercise the cross-game dedup and the max_by latest-value semantics.
"""

import datetime

from players import PLAYERS_SCHEMA, transform_players


def _load_raw(spark, fixtures_dir):
    """Read both fixture envelopes; multiLine=true so they can be pretty-printed."""
    return (
        spark.read.schema(PLAYERS_SCHEMA)
        .option("multiLine", "true")
        .json(str(fixtures_dir / "sample_pbp*.json"))
    )


def _by_player(players_df):
    return {row.player_id: row for row in players_df.collect()}


def test_schema_parses_and_dedups_across_games(spark, fixtures_dir):
    # game 1: 3 players. game 2: 4 players (1 new + 2 overlapping with g1 + Kopitar).
    # game 3: 1 player (Kopitar again, same date as g2). Unique union = 5.
    players = transform_players(_load_raw(spark, fixtures_dir))
    assert players.count() == 5


def test_key_fields_non_null(spark, fixtures_dir):
    players = transform_players(_load_raw(spark, fixtures_dir))
    for row in players.collect():
        assert row.player_id is not None
        assert row.first_name is not None
        assert row.last_name is not None
        assert row.first_seen_date is not None
        assert row.last_seen_date is not None


def test_max_by_picks_latest_position_for_player_in_multiple_games(spark, fixtures_dir):
    # Player 8480113 appears in game 1 (2024-10-08) as "L" and game 2 (2024-10-25) as "C".
    # max_by(position_code, game_date) must pick the position from the later game.
    by_id = _by_player(transform_players(_load_raw(spark, fixtures_dir)))
    iafallo = by_id[8480113]
    assert iafallo.position_code == "C"
    assert iafallo.first_name == "Alex"
    assert iafallo.last_name == "Iafallo"


def test_first_and_last_seen_dates_span_games(spark, fixtures_dir):
    by_id = _by_player(transform_players(_load_raw(spark, fixtures_dir)))
    # Player in both games — date span across the two fixtures
    iafallo = by_id[8480113]
    assert iafallo.first_seen_date == datetime.date(2024, 10, 8)
    assert iafallo.last_seen_date == datetime.date(2024, 10, 25)

    # Player in only game 1 — first and last collapse to the same date
    kuemper = by_id[8475311]
    assert kuemper.first_seen_date == datetime.date(2024, 10, 8)
    assert kuemper.last_seen_date == datetime.date(2024, 10, 8)

    # Player in only game 2 — same collapse
    byfield = by_id[8478403]
    assert byfield.first_seen_date == datetime.date(2024, 10, 25)
    assert byfield.last_seen_date == datetime.date(2024, 10, 25)


def test_single_game_player_position(spark, fixtures_dir):
    by_id = _by_player(transform_players(_load_raw(spark, fixtures_dir)))
    kuemper = by_id[8475311]
    assert kuemper.position_code == "G"
    assert kuemper.first_name == "Darcy"
    assert kuemper.last_name == "Kuemper"


def test_same_date_tie_break_by_game_id(spark, fixtures_dir):
    # Player 8471685 (Kopitar) appears on 2024-10-25 in both:
    #   - game 2024020055 as position "L"
    #   - game 2024020099 as position "C"
    # Same date → max_by(struct(game_date, game_id)) must tie-break on
    # game_id and pick "C" deterministically. Without the tie-break the
    # result would be non-deterministic across runs.
    by_id = _by_player(transform_players(_load_raw(spark, fixtures_dir)))
    kopitar = by_id[8471685]
    assert kopitar.position_code == "C"
    assert kopitar.first_seen_date == datetime.date(2024, 10, 25)
    assert kopitar.last_seen_date == datetime.date(2024, 10, 25)
