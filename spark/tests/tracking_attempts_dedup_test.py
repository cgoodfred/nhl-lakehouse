"""Tests for the candidate-selection + merge logic in bronze.tracking_ingest.

These are the load-bearing invariants of the current-state attempts table:

  * No (game_id, event_id) duplicates ever appear after a normal re-run
  * --retry-transient widens BOTH candidate selection AND the merge filter,
    so transient-failure rows are first re-fetched and THEN their old row
    is overwritten (filtering only at write would skip the refetch)
  * Compound key handling: same event_id in different games is two rows,
    not a collision

The fetch itself is not tested here (that's an HTTP integration concern);
the transform-vs-IO split in tracking_ingest.py lets us exercise the
DataFrame operations in isolation.
"""

import datetime as dt

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from tracking_ingest import (
    ATTEMPTS_SCHEMA,
    candidates,
    merge_attempts,
)

GOALS_SCHEMA = StructType([
    StructField("season",         IntegerType()),
    StructField("game_id",        LongType()),
    StructField("event_id",       LongType()),
    StructField("ppt_replay_url", StringType()),
])

_NOW = dt.datetime(2026, 6, 28, tzinfo=dt.timezone.utc)


def _goal(season, game_id, event_id, url=None):
    return (season, game_id, event_id, url or f"https://wsr.nhle.com/x/{game_id}/{event_id}.json")


def _attempt(season, game_id, event_id, status, http_code=None, frame_count=None, error=None):
    key = (
        f"tracking/season={season}/game_id={game_id}"
        f"/event_id={event_id}/tracking.json"
        if status == "success" else None
    )
    return (
        game_id, event_id, season,
        f"https://wsr.nhle.com/x/{game_id}/{event_id}.json",
        key,
        _NOW, status, http_code, frame_count, error,
    )


def _goals(spark, rows):
    return spark.createDataFrame(rows, GOALS_SCHEMA)


def _attempts(spark, rows):
    return spark.createDataFrame(rows, ATTEMPTS_SCHEMA)


# ---------- candidates ----------------------------------------------------


def test_candidates_first_run_returns_all_goals(spark):
    # No prior attempts table → every goal is a candidate.
    goals = _goals(spark, [
        _goal(20242025, 2024020001, 100),
        _goal(20242025, 2024020001, 200),
        _goal(20242025, 2024020002, 100),  # same event_id, different game — compound key matters
    ])
    result = candidates(existing=None, goals=goals, retry_transient=False)
    assert result.count() == 3


def test_candidates_skips_previously_attempted(spark):
    goals = _goals(spark, [
        _goal(20242025, 2024020001, 100),
        _goal(20242025, 2024020001, 200),
        _goal(20242025, 2024020002, 100),
    ])
    existing = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success", 200, 140),
        _attempt(20242025, 2024020001, 200, "http_404", 404),
    ])
    result = candidates(existing=existing, goals=goals, retry_transient=False)
    rows = [(r.game_id, r.event_id) for r in result.collect()]
    # Only the un-attempted (2024020002, 100) remains; the same-event-id-
    # different-game case is NOT skipped — confirms the compound key works.
    assert rows == [(2024020002, 100)]


def test_candidates_retry_transient_reincludes_transient_failures(spark):
    goals = _goals(spark, [
        _goal(20242025, 2024020001, 100),
        _goal(20242025, 2024020001, 200),
        _goal(20242025, 2024020001, 300),
        _goal(20242025, 2024020001, 400),
        _goal(20242025, 2024020002, 100),
    ])
    existing = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success",         200, 140),
        _attempt(20242025, 2024020001, 200, "http_404",        404),
        _attempt(20242025, 2024020001, 300, "fetch_error",     None, error="timeout"),
        _attempt(20242025, 2024020001, 400, "invalid_payload", 200,  error="not a list"),
    ])
    # Without retry: only the un-attempted (2024020002, 100) is a candidate.
    no_retry = candidates(existing=existing, goals=goals, retry_transient=False)
    assert {(r.game_id, r.event_id) for r in no_retry.collect()} == {(2024020002, 100)}
    # With retry: re-attempt the fetch_error AND invalid_payload rows; 404 and
    # success stay skipped (those are durable answers, not transient hiccups).
    with_retry = candidates(existing=existing, goals=goals, retry_transient=True)
    assert {(r.game_id, r.event_id) for r in with_retry.collect()} == {
        (2024020001, 300),
        (2024020001, 400),
        (2024020002, 100),
    }


# ---------- merge_attempts ------------------------------------------------


def test_merge_first_run_returns_new(spark):
    new = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success", 200, 140),
    ])
    result = merge_attempts(existing=None, new_df=new, retry_transient=False)
    assert result.count() == 1


def test_merge_preserves_existing_and_no_duplicates(spark):
    # Simulates the candidates-then-merge sequence: candidates picked one
    # new goal; merge writes the union of existing + new.
    existing = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success", 200, 140),
        _attempt(20242025, 2024020001, 200, "http_404", 404),
    ])
    new = _attempts(spark, [
        _attempt(20242025, 2024020002, 100, "success", 200, 130),
    ])
    result = merge_attempts(existing=existing, new_df=new, retry_transient=False)
    keys = sorted((r.game_id, r.event_id) for r in result.collect())
    assert keys == [(2024020001, 100), (2024020001, 200), (2024020002, 100)]
    # Critical: no duplicates of the existing rows.
    assert len(keys) == len(set(keys))


def test_merge_retry_transient_overwrites_failure_rows(spark):
    existing = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success",     200, 140),
        _attempt(20242025, 2024020001, 200, "fetch_error", None, error="timeout"),
    ])
    # candidates(retry_transient=True) would have re-included event 200, so
    # `new` contains its fresh attempt. merge_attempts(retry_transient=True)
    # must drop the OLD (200, fetch_error) row before unioning, otherwise we
    # end up with two rows keyed (2024020001, 200) — one success, one error.
    new = _attempts(spark, [
        _attempt(20242025, 2024020001, 200, "success", 200, 135),
    ])
    result = merge_attempts(existing=existing, new_df=new, retry_transient=True)
    rows = {(r.game_id, r.event_id): r.status for r in result.collect()}
    assert rows == {
        (2024020001, 100): "success",
        (2024020001, 200): "success",  # was fetch_error, now overwritten
    }


def test_merge_retry_transient_does_not_drop_404_or_success(spark):
    # http_404 and success are NEVER candidates for retry; they survive
    # untouched even when retry_transient=True.
    existing = _attempts(spark, [
        _attempt(20242025, 2024020001, 100, "success",  200, 140),
        _attempt(20242025, 2024020001, 200, "http_404", 404),
    ])
    new = _attempts(spark, [])  # nothing to add — empty re-run with retry on
    result = merge_attempts(existing=existing, new_df=new, retry_transient=True)
    rows = {(r.game_id, r.event_id): r.status for r in result.collect()}
    assert rows == {
        (2024020001, 100): "success",
        (2024020001, 200): "http_404",
    }
