"""Tests for the bounded scheduled-pipeline validator."""

import datetime as dt

import pytest
from pyspark.sql.functions import when
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from validate_pipeline import (
    SnapshotRecord,
    duplicate_keys,
    expected_refreshed_object_keys,
    expected_schedule_keys,
    missing_shift_coverage,
    parse_impact_manifest,
    row_count_baseline,
    tracking_reconciliation_issues,
)

_SEASON = 20262027
_GAME = 2026020001
_NOW = dt.datetime(2026, 10, 10, 18, tzinfo=dt.timezone.utc)


def _manifest_payload():
    return {
        "run_id": "scheduled-pipeline-abc12",
        "window_start": "2026-10-08",
        "window_end": "2026-10-11",
        "objects": [
            "schedule/date=2026-10-08/schedule.json",
            "schedule/date=2026-10-09/schedule.json",
            "schedule/date=2026-10-10/schedule.json",
            "schedule/date=2026-10-11/schedule.json",
            f"play-by-play/season={_SEASON}/date=2026-10-08/game_{_GAME}.json",
            f"shift-charts/season={_SEASON}/date=2026-10-08/game_{_GAME}.json",
        ],
        "games": [
            {
                "date": "2026-10-08",
                "game_id": _GAME,
                "season": _SEASON,
                "state": "FINAL",
                "pbp_status": "refreshed",
                "shift_status": "refreshed",
            }
        ],
    }


def test_manifest_projects_exact_schedule_and_refreshed_keys():
    manifest = parse_impact_manifest(_manifest_payload())

    assert expected_schedule_keys(manifest) == [
        "schedule/date=2026-10-08/schedule.json",
        "schedule/date=2026-10-09/schedule.json",
        "schedule/date=2026-10-10/schedule.json",
        "schedule/date=2026-10-11/schedule.json",
    ]
    assert expected_refreshed_object_keys(manifest) == [
        f"play-by-play/season={_SEASON}/date=2026-10-08/game_{_GAME}.json",
        f"shift-charts/season={_SEASON}/date=2026-10-08/game_{_GAME}.json",
    ]


def test_manifest_rejects_duplicate_game_ids():
    payload = _manifest_payload()
    payload["games"].append(dict(payload["games"][0]))

    with pytest.raises(ValueError, match="duplicate game_id"):
        parse_impact_manifest(payload)


def test_duplicate_key_check_reports_the_logical_key(spark):
    frame = spark.createDataFrame(
        [(_GAME, 10), (_GAME, 10), (_GAME, 20)],
        "game_id long, event_id long",
    )

    assert duplicate_keys(frame, ["game_id", "event_id"]) == [
        f"game_id={_GAME}, event_id=10, rows=2"
    ]


def test_old_completed_games_require_shift_rows(spark):
    games = spark.createDataFrame(
        [(_GAME, 2, "FINAL", dt.datetime(2026, 10, 8, 0, 0))],
        "game_id long, game_type int, game_state string, start_time_utc timestamp",
    )
    empty_shifts = spark.createDataFrame([], "game_id long")
    manifest = parse_impact_manifest(_manifest_payload())

    assert missing_shift_coverage(games, empty_shifts, manifest, _NOW) == [_GAME]
    covered = spark.createDataFrame([(_GAME,)], "game_id long")
    assert missing_shift_coverage(games, covered, manifest, _NOW) == []


def test_row_count_baseline_skips_mid_write_delete_snapshot():
    committed = dt.datetime(2026, 10, 10)
    snapshots = {
        1: SnapshotRecord(1, None, "append", committed, 100_000),
        2: SnapshotRecord(2, 1, "delete", committed, 80_000),
        3: SnapshotRecord(3, 2, "append", committed, 80_000),
    }

    assert row_count_baseline(snapshots, 3) == snapshots[1]
    assert row_count_baseline(snapshots, 2) == snapshots[1]


def test_tracking_status_reconciles_state_machine_and_frame_counts(spark):
    plays = spark.createDataFrame(
        [
            (_SEASON, _GAME, 10, "goal", None),
            (_SEASON, _GAME, 20, "goal", "https://example.test/tracking.json"),
            (_SEASON, _GAME, 30, "shot-on-goal", None),
        ],
        "season int, game_id long, event_id long, type_desc_key string, ppt_replay_url string",
    )
    attempts = spark.createDataFrame(
        [(_SEASON, _GAME, 20, "success")],
        "season int, game_id long, event_id long, status string",
    )
    frames = spark.createDataFrame(
        [(_SEASON, _GAME, 20), (_SEASON, _GAME, 20)],
        "season int, game_id long, event_id long",
    )
    sequences = spark.createDataFrame(
        [(_SEASON, _GAME, 20, 2)],
        "season int, game_id long, event_id long, frame_count int",
    )
    statuses = spark.createDataFrame(
        [
            (_SEASON, _GAME, 10, "no_url", None),
            (_SEASON, _GAME, 20, "available", 2),
        ],
        StructType(
            [
                StructField("season", IntegerType()),
                StructField("game_id", LongType()),
                StructField("event_id", LongType()),
                StructField("tracking_status", StringType()),
                StructField("frame_count", IntegerType()),
            ]
        ),
    )

    assert tracking_reconciliation_issues(
        plays, attempts, frames, sequences, statuses, [_GAME]
    ) == []

    stale_statuses = statuses.withColumn(
        "tracking_status",
        when(statuses.event_id == 20, "pending_silver_rebuild").otherwise(
            statuses.tracking_status
        ),
    )
    issues = tracking_reconciliation_issues(
        plays, attempts, frames, sequences, stale_statuses, [_GAME]
    )
    assert len(issues) == 1
    assert "expected=available/2" in issues[0]
    assert "actual=pending_silver_rebuild/2" in issues[0]


def test_tracking_pending_state_is_reported_once_even_when_actual_differs(spark):
    plays = spark.createDataFrame(
        [(_SEASON, _GAME, 20, "goal", "https://example.test/tracking.json")],
        "season int, game_id long, event_id long, type_desc_key string, ppt_replay_url string",
    )
    attempts = spark.createDataFrame(
        [(_SEASON, _GAME, 20, "success")],
        "season int, game_id long, event_id long, status string",
    )
    frames = spark.createDataFrame([], "season int, game_id long, event_id long")
    sequences = spark.createDataFrame(
        [], "season int, game_id long, event_id long, frame_count int"
    )
    statuses = spark.createDataFrame(
        [(_SEASON, _GAME, 20, "no_url", None)],
        StructType(
            [
                StructField("season", IntegerType()),
                StructField("game_id", LongType()),
                StructField("event_id", LongType()),
                StructField("tracking_status", StringType()),
                StructField("frame_count", IntegerType()),
            ]
        ),
    )

    issues = tracking_reconciliation_issues(
        plays, attempts, frames, sequences, statuses, [_GAME]
    )
    assert issues == [
        f"game_id={_GAME}, event_id=20, terminal_pipeline_status=pending_silver_rebuild"
    ]
