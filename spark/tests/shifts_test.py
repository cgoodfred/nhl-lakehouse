"""Tests for the bronze shift-chart to silver transformation."""

from pyspark.sql.functions import lit
from shifts import SHIFT_SCHEMA, count_malformed_shifts, transform_shifts


def _load_fixture(spark, fixtures_dir):
    return (
        spark.read.option("multiLine", "true")
        .schema(SHIFT_SCHEMA)
        .json(str(fixtures_dir / "sample_shifts.json"))
        .withColumn("season", lit(20252026))
        .withColumn("date", lit("2025-10-08"))
    )


def test_malformed_rows_are_counted(spark, fixtures_dir):
    assert count_malformed_shifts(_load_fixture(spark, fixtures_dir)) == 1


def test_valid_rows_are_parsed_and_deduplicated(spark, fixtures_dir):
    out = transform_shifts(
        _load_fixture(spark, fixtures_dir).filter("size(data) > 0")
    ).orderBy("player_id", "shift_number")
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0].start_time_seconds == 0
    assert rows[0].end_time_seconds == 45
    assert rows[0].duration_seconds == 45
    # The lowest source ID wins deterministically for the duplicate key.
    assert rows[1].shift_id == 11
    assert rows[1].event_number == 3


def test_required_columns_and_logical_key(spark, fixtures_dir):
    rows = transform_shifts(_load_fixture(spark, fixtures_dir)).collect()
    keys = {(r.game_id, r.player_id, r.period, r.shift_number) for r in rows}
    assert keys == {
        (2025020001, 8471675, 1, 1),
        (2025020001, 8471675, 1, 2),
    }
    assert all(r.game_date.isoformat() == "2025-10-08" for r in rows)
