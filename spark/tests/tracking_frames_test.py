"""Tests for silver.tracking_frames.transform_tracking_frames.

The fixture (sample_tracking_ev274.json) is a real captured PPT payload —
120 frames at ~10Hz, BUF vs Buffalo opponent, recorded earlier in the
session. The fixture being a real wsr.nhle.com payload exercises the
empty-string-puck-fields edge case (puck entry's playerId/sweater/team
are empty strings, not nulls, which would crash a naive int cast).
"""

import math

from pyspark.sql.functions import lit

from tracking_frames import (
    BRONZE_FRAME_SCHEMA,
    PPT_CENTER_X_IN,
    PPT_CENTER_Y_IN,
    PPT_INCHES_PER_FT,
    transform_tracking_frames,
)

# Partition column values must come from outside the fixture file — in
# production they're discovered from the bronze path layout, which we don't
# simulate here. Numbers don't have to match the real ev274 ids; the
# transform doesn't care.
_FAKE_SEASON   = 20242025
_FAKE_GAME_ID  = 2024020001
_FAKE_EVENT_ID = 274


def _load_fixture(spark, fixtures_dir):
    return (
        spark.read.option("multiLine", "true").schema(BRONZE_FRAME_SCHEMA)
        .json(str(fixtures_dir / "sample_tracking_ev274.json"))
        .withColumn("season",   lit(_FAKE_SEASON))
        .withColumn("game_id",  lit(_FAKE_GAME_ID))
        .withColumn("event_id", lit(_FAKE_EVENT_ID))
    )


def test_one_row_per_frame(spark, fixtures_dir):
    out = transform_tracking_frames(_load_fixture(spark, fixtures_dir))
    # The captured ev274 payload has 120 frames.
    assert out.count() == 120


def test_frame_index_zero_based_dense(spark, fixtures_dir):
    rows = (
        transform_tracking_frames(_load_fixture(spark, fixtures_dir))
        .orderBy("frame_index").collect()
    )
    assert rows[0].frame_index == 0
    assert rows[-1].frame_index == 119
    # No gaps in the sequence.
    assert [r.frame_index for r in rows] == list(range(120))


def test_rel_seconds_zero_at_last_frame_and_negative_before(spark, fixtures_dir):
    rows = (
        transform_tracking_frames(_load_fixture(spark, fixtures_dir))
        .orderBy("frame_index").collect()
    )
    # Last frame is the goal moment.
    assert rows[-1].rel_seconds == 0.0
    # Earlier frames are all strictly negative (we're looking BACK from the goal).
    assert all(r.rel_seconds < 0 for r in rows[:-1])
    # Monotonically increasing toward 0.
    assert all(
        rows[i].rel_seconds <= rows[i + 1].rel_seconds for i in range(len(rows) - 1)
    )
    # Sanity: a ~14-second window is spanned.
    assert rows[0].rel_seconds < -10.0
    assert rows[0].rel_seconds > -20.0


def test_puck_columns_populated_and_within_rink(spark, fixtures_dir):
    rows = transform_tracking_frames(_load_fixture(spark, fixtures_dir)).collect()
    for r in rows:
        assert r.puck_x_in is not None and r.puck_y_in is not None
        assert r.puck_x_ft is not None and r.puck_y_ft is not None
        # Puck must be on the rink (±100 ft x, ±42.5 ft y in NHL coords).
        assert -100.5 <= r.puck_x_ft <= 100.5
        assert -43.0  <= r.puck_y_ft <= 43.0


def test_on_ice_excludes_puck_entry(spark, fixtures_dir):
    # The bronze onIce map has the puck under key "1"; the silver on_ice
    # array must NOT include it. Each surviving entry must have a real
    # integer player_id (the puck's "" would have nulled here).
    rows = transform_tracking_frames(_load_fixture(spark, fixtures_dir)).collect()
    for r in rows:
        assert r.on_ice, "expected at least one player on ice"
        for p in r.on_ice:
            assert p.player_id is not None
            assert p.player_id > 0
            # Players are real entries — none of them are the puck masquerading.
            assert p.team_abbrev != ""


def test_player_ft_and_in_coords_consistent(spark, fixtures_dir):
    # Both unit representations are materialized; they must be a clean linear
    # transform of each other. Anything else means the conversion was applied
    # to the wrong axis or a cast lost precision.
    rows = transform_tracking_frames(_load_fixture(spark, fixtures_dir)).collect()
    for r in rows:
        for p in r.on_ice:
            assert math.isclose(
                p.x_ft, (p.x_in - PPT_CENTER_X_IN) / PPT_INCHES_PER_FT,
                rel_tol=1e-9, abs_tol=1e-9,
            )
            assert math.isclose(
                p.y_ft, (p.y_in - PPT_CENTER_Y_IN) / PPT_INCHES_PER_FT,
                rel_tol=1e-9, abs_tol=1e-9,
            )


def test_partition_columns_preserved(spark, fixtures_dir):
    rows = transform_tracking_frames(_load_fixture(spark, fixtures_dir)).collect()
    assert all(r.season   == _FAKE_SEASON   for r in rows)
    assert all(r.game_id  == _FAKE_GAME_ID  for r in rows)
    assert all(r.event_id == _FAKE_EVENT_ID for r in rows)


def test_per_goal_window_isolates_indices(spark, fixtures_dir):
    # The window functions (row_number, max) partition by (game_id, event_id).
    # Simulating a second goal: same fixture loaded twice with a different
    # event_id. Each goal's frames must have their own 0-based index, and
    # rel_seconds must be computed relative to the LAST frame OF THAT GOAL,
    # not the overall maximum across goals.
    fixture_a = _load_fixture(spark, fixtures_dir)
    fixture_b = (
        spark.read.option("multiLine", "true").schema(BRONZE_FRAME_SCHEMA)
        .json(str(fixtures_dir / "sample_tracking_ev274.json"))
        .withColumn("season",   lit(_FAKE_SEASON))
        .withColumn("game_id",  lit(_FAKE_GAME_ID))
        .withColumn("event_id", lit(_FAKE_EVENT_ID + 1))
    )
    combined = fixture_a.unionByName(fixture_b)
    out = transform_tracking_frames(combined).collect()
    by_event: dict[int, list] = {}
    for r in out:
        by_event.setdefault(r.event_id, []).append(r)
    assert len(by_event) == 2
    for ev_id, rows in by_event.items():
        rows.sort(key=lambda r: r.frame_index)
        assert rows[0].frame_index == 0,    f"event {ev_id} starts at 0"
        assert rows[-1].frame_index == 119, f"event {ev_id} ends at 119"
        assert rows[-1].rel_seconds == 0.0, f"event {ev_id} last frame is 0.0s"
