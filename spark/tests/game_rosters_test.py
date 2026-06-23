"""Tests for silver.jobs.game_rosters transformation.

Reads all three fixture envelopes; the bridge table is NOT deduped,
so every (game_id, player_id) pair becomes a row. Reuses the same
fixtures as players_test.py — the bronze envelope has both arrays so
each fixture validly serves both transformations.
"""

from pyspark.sql.functions import lit

from game_rosters import ROSTERS_SCHEMA, transform_game_rosters


def _load_raw(spark, fixtures_dir):
    """Read all fixture envelopes and attach season (path partition discovery
    doesn't apply to flat-filesystem reads). All fixtures are in 2024-2025."""
    return (
        spark.read.schema(ROSTERS_SCHEMA)
        .option("multiLine", "true")
        .json(str(fixtures_dir / "sample_pbp*.json"))
        .withColumn("season", lit(20242025))
    )


def _by_game_player(rosters_df):
    return {(row.game_id, row.player_id): row for row in rosters_df.collect()}


def test_schema_parses_and_no_dedup(spark, fixtures_dir):
    # 3 spots in game 1 + 4 in game 2 + 1 in game 3 = 8 rows (no dedup)
    rosters = transform_game_rosters(_load_raw(spark, fixtures_dir))
    assert rosters.count() == 8


def test_composite_key_unique(spark, fixtures_dir):
    rosters = transform_game_rosters(_load_raw(spark, fixtures_dir))
    rows = rosters.collect()
    keys = {(r.game_id, r.player_id) for r in rows}
    assert len(keys) == len(rows)  # no duplicate (game_id, player_id) pairs


def test_key_fields_non_null(spark, fixtures_dir):
    rosters = transform_game_rosters(_load_raw(spark, fixtures_dir))
    for row in rosters.collect():
        assert row.game_id is not None
        assert row.player_id is not None
        assert row.season is not None
        assert row.team_id is not None
        assert row.position_code is not None


def test_player_in_multiple_games_keeps_per_game_state(spark, fixtures_dir):
    # Kopitar (8471685) appears in game 2 (as "L", sweater 11) and game 3
    # (as "C", sweater 11). Both rows must be present with their per-game
    # position — this is the whole point of the bridge table vs the dim.
    by_key = _by_game_player(transform_game_rosters(_load_raw(spark, fixtures_dir)))
    kopitar_g2 = by_key[(2024020055, 8471685)]
    kopitar_g3 = by_key[(2024020099, 8471685)]
    assert kopitar_g2.position_code == "L"
    assert kopitar_g3.position_code == "C"
    # Sweater number matched both games for Kopitar
    assert kopitar_g2.sweater_number == 11
    assert kopitar_g3.sweater_number == 11


def test_spot_fields_preserved(spark, fixtures_dir):
    # Iafallo on game 1: team 52 (LAK), sweater 19, position L
    by_key = _by_game_player(transform_game_rosters(_load_raw(spark, fixtures_dir)))
    iafallo_g1 = by_key[(2024020001, 8480113)]
    assert iafallo_g1.team_id == 52
    assert iafallo_g1.sweater_number == 19
    assert iafallo_g1.position_code == "L"
