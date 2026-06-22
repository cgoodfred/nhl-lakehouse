"""Tests for silver.jobs.plays transformation.

Run from the silver/ directory:
    cd silver && pytest tests/

Requires: pytest, pyspark (matching the cluster's Spark version).
"""

from pyspark.sql.functions import lit

from plays import PLAYS_SCHEMA, transform_plays


def _load_raw(spark, fixtures_dir):
    """Read the sample PBP envelope and attach the season column that path
    partition discovery would normally supply in the cluster.

    multiLine=True so the fixture can be pretty-printed for human readability;
    production bronze files are single-line per record, which Spark's default
    json() reader handles natively."""
    return (
        spark.read.schema(PLAYS_SCHEMA)
        .option("multiLine", "true")
        .json(str(fixtures_dir / "sample_pbp.json"))
        .withColumn("season", lit(20242025))
    )


def _by_event_id(plays_df):
    return {row.event_id: row for row in plays_df.collect()}


def test_schema_parses_and_row_count_matches(spark, fixtures_dir):
    plays = transform_plays(_load_raw(spark, fixtures_dir))
    # fixture has 4 plays in the array
    assert plays.count() == 4


def test_key_fields_non_null(spark, fixtures_dir):
    plays = transform_plays(_load_raw(spark, fixtures_dir))
    for row in plays.collect():
        assert row.event_id is not None
        assert row.game_id is not None
        assert row.season is not None
        assert row.type_desc_key is not None


def test_situation_code_even_strength_both_goalies(spark, fixtures_dir):
    # "1551" — both goalies present, 5v5
    by_id = _by_event_id(transform_plays(_load_raw(spark, fixtures_dir)))
    faceoff = by_id[1]
    assert faceoff.situation_code == "1551"
    assert faceoff.away_skaters == 5
    assert faceoff.home_skaters == 5
    assert faceoff.away_goalie_present is True
    assert faceoff.home_goalie_present is True
    assert faceoff.strength_state == "EV"
    assert faceoff.is_empty_net is False


def test_situation_code_home_power_play(spark, fixtures_dir):
    # "1451" — both goalies present, 4 away vs 5 home → home PP
    by_id = _by_event_id(transform_plays(_load_raw(spark, fixtures_dir)))
    goal = by_id[2]
    assert goal.situation_code == "1451"
    assert goal.away_skaters == 4
    assert goal.home_skaters == 5
    assert goal.strength_state == "PP"
    assert goal.is_empty_net is False


def test_situation_code_away_empty_net(spark, fixtures_dir):
    # "0551" — away goalie pulled, equal skaters
    by_id = _by_event_id(transform_plays(_load_raw(spark, fixtures_dir)))
    sog = by_id[3]
    assert sog.situation_code == "0551"
    assert sog.away_goalie_present is False
    assert sog.home_goalie_present is True
    assert sog.is_empty_net is True
    assert sog.strength_state == "EV"


def test_details_projection_goal(spark, fixtures_dir):
    by_id = _by_event_id(transform_plays(_load_raw(spark, fixtures_dir)))
    goal = by_id[2]
    assert goal.scoring_player_id == 8480113
    assert goal.shot_type == "wrist"
    assert goal.home_score == 1
    assert goal.event_owner_team_id == 52
    assert goal.zone_code == "O"


def test_details_projection_penalty(spark, fixtures_dir):
    by_id = _by_event_id(transform_plays(_load_raw(spark, fixtures_dir)))
    penalty = by_id[4]
    assert penalty.penalty_desc_key == "holding"
    assert penalty.penalty_duration == 2
    assert penalty.penalty_type_code == "MIN"
    assert penalty.committed_by_player_id == 8482124
