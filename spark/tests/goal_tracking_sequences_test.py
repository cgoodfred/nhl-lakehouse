"""Tests for gold.goal_tracking_sequences.transform_goal_tracking_sequences."""

from goal_tracking_sequences import transform_goal_tracking_sequences

_SEASON = 20242025
_GAME = 2024020001


def _frames_df(spark):
    return spark.createDataFrame(
        [
            (_GAME, 274, _SEASON, 2, 102, 1202.0, 502.0, [{"player_id": 11}]),
            (_GAME, 274, _SEASON, 0, 100, 1200.0, 500.0, [{"player_id": 11}]),
            (_GAME, 274, _SEASON, 1, 101, 1201.0, 501.0, [{"player_id": 12}]),
            (_GAME, 300, _SEASON, 0, 200, 1300.0, 600.0, [{"player_id": 21}]),
        ],
        """
        game_id long,
        event_id long,
        season int,
        frame_index int,
        timestamp_ds long,
        puck_x_in double,
        puck_y_in double,
        on_ice array<struct<player_id:long>>
        """,
    )


def test_one_row_per_goal_with_frame_count(spark):
    rows = transform_goal_tracking_sequences(_frames_df(spark)).collect()
    by_event = {row.event_id: row for row in rows}

    assert set(by_event) == {274, 300}
    assert by_event[274].frame_count == 3
    assert by_event[300].frame_count == 1


def test_frames_sorted_by_frame_index(spark):
    row = (
        transform_goal_tracking_sequences(_frames_df(spark))
        .where("event_id = 274")
        .collect()[0]
    )

    assert [frame.frame_index for frame in row.frames] == [0, 1, 2]
    assert [frame.timestamp_ds for frame in row.frames] == [100, 101, 102]
    assert row.frames[1].on_ice[0].player_id == 12
