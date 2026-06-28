"""Tests for gold.goal_tracking_status.transform_goal_tracking_status.

Exhaustively covers every branch of the CASE expression with small in-memory
DataFrames. The expression's order matters (no_url first; success-with-
frames before generic status checks) — these tests would fail loudly if the
clauses were reordered incorrectly.
"""

import datetime as dt

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from goal_tracking_status import transform_goal_tracking_status

# Mirror only the columns the transform actually reads — keeps tests robust
# to unrelated additions in silver.plays / silver.tracking_attempts /
# silver.tracking_frames.

_PLAYS_SCHEMA = StructType([
    StructField("type_desc_key",  StringType()),
    StructField("season",         IntegerType()),
    StructField("game_id",        LongType()),
    StructField("event_id",       LongType()),
    StructField("ppt_replay_url", StringType()),
])

_ATTEMPTS_SCHEMA = StructType([
    StructField("season",        IntegerType()),
    StructField("game_id",       LongType()),
    StructField("event_id",      LongType()),
    StructField("status",        StringType()),
    StructField("http_code",     IntegerType()),
    StructField("attempted_at",  TimestampType()),
    StructField("error_message", StringType()),
])

_FRAMES_SCHEMA = StructType([
    StructField("season",   IntegerType()),
    StructField("game_id",  LongType()),
    StructField("event_id", LongType()),
])

_NOW = dt.datetime(2026, 6, 28, tzinfo=dt.timezone.utc)
_SEASON = 20252026
_GAME   = 2025020001
_URL    = "https://wsr.nhle.com/sprites/x/100.json"


def _plays(spark, rows):
    return spark.createDataFrame(rows, _PLAYS_SCHEMA)


def _attempts(spark, rows):
    return spark.createDataFrame(rows, _ATTEMPTS_SCHEMA)


def _frames(spark, rows):
    return spark.createDataFrame(rows, _FRAMES_SCHEMA)


def _frames_for(event_id: int, n: int):
    """N synthetic frame rows for one event — only the (season, game_id,
    event_id) key matters for the count() aggregation."""
    return [(_SEASON, _GAME, event_id)] * n


def _by_event(out_df):
    return {r.event_id: r for r in out_df.collect()}


def _build_inputs(spark, plays_rows, attempts_rows, frames_rows):
    return (
        _plays(spark, plays_rows),
        _attempts(spark, attempts_rows),
        _frames(spark, frames_rows),
    )


def test_no_url_when_ppt_replay_url_is_null(spark):
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[("goal", _SEASON, _GAME, 100, None)],
        attempts_rows=[],
        frames_rows=[],
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "no_url"
    assert rows[100].frame_count is None
    assert rows[100].fetch_status is None


def test_available_when_success_and_frames_present(spark):
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[("goal", _SEASON, _GAME, 100, _URL)],
        attempts_rows=[(_SEASON, _GAME, 100, "success", 200, _NOW, None)],
        frames_rows=_frames_for(100, 140),
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "available"
    assert rows[100].frame_count    == 140
    assert rows[100].fetch_status   == "success"


def test_pending_silver_rebuild_when_success_but_no_frames(spark):
    # The freshness gap: bronze ingest logged success but silver-tracking-
    # frames hasn't been rebuilt yet, so frames_df has nothing for this
    # event. Must NOT show as 'available' (the viz would load an empty
    # animation) and must NOT show as 'fetch_failed' (it actually succeeded).
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[("goal", _SEASON, _GAME, 100, _URL)],
        attempts_rows=[(_SEASON, _GAME, 100, "success", 200, _NOW, None)],
        frames_rows=[],
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "pending_silver_rebuild"
    assert rows[100].frame_count is None
    assert rows[100].fetch_status   == "success"


def test_not_tracked_when_http_404(spark):
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[("goal", _SEASON, _GAME, 100, _URL)],
        attempts_rows=[(_SEASON, _GAME, 100, "http_404", 404, _NOW, None)],
        frames_rows=[],
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "not_tracked"
    assert rows[100].http_code      == 404


def test_fetch_failed_for_other_non_success_statuses(spark):
    # http_other, fetch_error, invalid_payload all funnel into fetch_failed.
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[
            ("goal", _SEASON, _GAME, 100, _URL),
            ("goal", _SEASON, _GAME, 200, _URL),
            ("goal", _SEASON, _GAME, 300, _URL),
        ],
        attempts_rows=[
            (_SEASON, _GAME, 100, "http_other",      500,  _NOW, "Internal Server Error"),
            (_SEASON, _GAME, 200, "fetch_error",     None, _NOW, "ConnectionError"),
            (_SEASON, _GAME, 300, "invalid_payload", 200,  _NOW, "not a list"),
        ],
        frames_rows=[],
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "fetch_failed"
    assert rows[200].tracking_status == "fetch_failed"
    assert rows[300].tracking_status == "fetch_failed"
    # Underlying fetch_status preserved so ops can see WHY it failed.
    assert rows[100].fetch_status == "http_other"
    assert rows[200].fetch_status == "fetch_error"
    assert rows[300].fetch_status == "invalid_payload"


def test_not_attempted_when_no_attempts_row(spark):
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[("goal", _SEASON, _GAME, 100, _URL)],
        attempts_rows=[],
        frames_rows=[],
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert rows[100].tracking_status == "not_attempted"
    assert rows[100].fetch_status is None
    assert rows[100].attempted_at is None


def test_non_goal_events_excluded(spark):
    # The transform filters to type_desc_key='goal'. A shot-on-goal play
    # in the source must not appear in the output even if it has tracking
    # attempts data attached (defensive — this shouldn't happen in
    # practice but the test pins the filter behavior).
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[
            ("goal",         _SEASON, _GAME, 100, _URL),
            ("shot-on-goal", _SEASON, _GAME, 200, _URL),
        ],
        attempts_rows=[
            (_SEASON, _GAME, 200, "success", 200, _NOW, None),
        ],
        frames_rows=_frames_for(200, 50),
    )
    rows = _by_event(transform_goal_tracking_status(plays, attempts, frames))
    assert 200 not in rows
    assert 100 in rows


def test_one_row_per_goal_no_join_fanout(spark):
    # frame_counts pre-aggregates, so each event_id should appear exactly
    # once in the output — proves the LEFT JOIN against silver.tracking_frames
    # isn't fanning out per-frame.
    plays, attempts, frames = _build_inputs(
        spark,
        plays_rows=[
            ("goal", _SEASON, _GAME, 100, _URL),
            ("goal", _SEASON, _GAME, 200, _URL),
        ],
        attempts_rows=[
            (_SEASON, _GAME, 100, "success", 200, _NOW, None),
            (_SEASON, _GAME, 200, "success", 200, _NOW, None),
        ],
        # Lots of frames per event — fanout bug would show up as duplicated rows.
        frames_rows=_frames_for(100, 140) + _frames_for(200, 120),
    )
    out_rows = transform_goal_tracking_status(plays, attempts, frames).collect()
    keys = [(r.game_id, r.event_id) for r in out_rows]
    assert len(keys) == 2
    assert len(keys) == len(set(keys))


def test_all_status_branches_in_one_run(spark):
    # Single transform run with one goal per CASE branch. Catches any
    # ordering regression in the when() chain.
    plays = [
        ("goal", _SEASON, _GAME, 100, None),          # no_url
        ("goal", _SEASON, _GAME, 200, _URL),          # available
        ("goal", _SEASON, _GAME, 300, _URL),          # pending_silver_rebuild
        ("goal", _SEASON, _GAME, 400, _URL),          # not_tracked
        ("goal", _SEASON, _GAME, 500, _URL),          # fetch_failed
        ("goal", _SEASON, _GAME, 600, _URL),          # not_attempted
    ]
    attempts = [
        (_SEASON, _GAME, 200, "success",  200, _NOW, None),
        (_SEASON, _GAME, 300, "success",  200, _NOW, None),
        (_SEASON, _GAME, 400, "http_404", 404, _NOW, None),
        (_SEASON, _GAME, 500, "fetch_error", None, _NOW, "timeout"),
    ]
    frames = _frames_for(200, 140)  # only event 200 has silver frames
    out = _by_event(transform_goal_tracking_status(
        _plays(spark, plays),
        _attempts(spark, attempts),
        _frames(spark, frames),
    ))
    assert {evid: row.tracking_status for evid, row in out.items()} == {
        100: "no_url",
        200: "available",
        300: "pending_silver_rebuild",
        400: "not_tracked",
        500: "fetch_failed",
        600: "not_attempted",
    }
