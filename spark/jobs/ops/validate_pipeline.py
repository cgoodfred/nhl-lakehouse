"""Validate one scheduled pipeline run against its bounded impact manifest.

The validator is the final scheduled-pipeline stage. It deliberately filters
game-keyed tables to the rolling ingest window; the only history-wide reads
are Iceberg snapshot metadata used for low-cost row-count and freshness
guards. A failed check raises after all checks have logged their outcome so
the Argo Workflow fails with useful diagnostic context.
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, when

from common import get_spark, load_impact_manifest, processing_arguments

SILVER_TABLES = (
    "nhl.silver.games",
    "nhl.silver.plays",
    "nhl.silver.players",
    "nhl.silver.teams",
    "nhl.silver.game_rosters",
    "nhl.silver.shifts",
    "nhl.silver.tracking_attempts",
    "nhl.silver.tracking_frames",
)
GOLD_TABLES = (
    "nhl.gold.player_shots",
    "nhl.gold.goal_tracking_sequences",
    "nhl.gold.goal_tracking_status",
)
REQUIRED_TABLES = SILVER_TABLES + GOLD_TABLES
FINAL_STATES = ("FINAL", "OFF", "OVER")
EXPLICIT_NO_SHIFT_DATA = ("unavailable", "no_data", "not_available")


@dataclass(frozen=True)
class ImpactGame:
    date: dt.date
    game_id: int
    season: int
    state: str
    pbp_status: str
    shift_status: str


@dataclass(frozen=True)
class ImpactManifest:
    run_id: str
    window_start: dt.date
    window_end: dt.date
    objects: tuple[str, ...]
    games: tuple[ImpactGame, ...]

    @property
    def game_ids(self) -> list[int]:
        return sorted({game.game_id for game in self.games})

    @property
    def refreshed_pbp_game_ids(self) -> list[int]:
        return sorted(
            {game.game_id for game in self.games if game.pbp_status == "refreshed"}
        )


@dataclass(frozen=True)
class SnapshotInfo:
    committed_at: dt.datetime
    total_records: int
    previous_total_records: int | None


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _required_string(payload: dict, name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"impact manifest field {name!r} must be a non-empty string")
    return value


def parse_impact_manifest(payload: dict) -> ImpactManifest:
    """Parse and validate the control-plane fields emitted by Go ingest."""

    run_id = _required_string(payload, "run_id")
    try:
        window_start = dt.date.fromisoformat(_required_string(payload, "window_start"))
        window_end = dt.date.fromisoformat(_required_string(payload, "window_end"))
    except ValueError as exc:
        raise ValueError(f"impact manifest has an invalid date: {exc}") from exc
    if window_end < window_start:
        raise ValueError("impact manifest window_end is before window_start")

    raw_objects = payload.get("objects")
    raw_games = payload.get("games")
    if not isinstance(raw_objects, list):
        raise ValueError("impact manifest field 'objects' must be a list")
    if not isinstance(raw_games, list):
        raise ValueError("impact manifest field 'games' must be a list")

    games = []
    for index, game in enumerate(raw_games):
        try:
            games.append(
                ImpactGame(
                    date=dt.date.fromisoformat(str(game["date"])),
                    game_id=int(game["game_id"]),
                    season=int(game["season"]),
                    state=str(game["state"]),
                    pbp_status=str(game["pbp_status"]),
                    shift_status=str(game["shift_status"]),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"impact manifest game at index {index} is invalid: {exc}") from exc

    game_ids = [game.game_id for game in games]
    if len(game_ids) != len(set(game_ids)):
        raise ValueError("impact manifest contains duplicate game_id entries")
    outside_window = [
        game.game_id
        for game in games
        if game.date < window_start or game.date > window_end
    ]
    if outside_window:
        raise ValueError(f"impact manifest games outside its window: {outside_window}")

    return ImpactManifest(
        run_id=run_id,
        window_start=window_start,
        window_end=window_end,
        objects=tuple(str(value) for value in raw_objects),
        games=tuple(games),
    )


def expected_schedule_keys(manifest: ImpactManifest) -> list[str]:
    days = (manifest.window_end - manifest.window_start).days
    return [
        f"schedule/date={manifest.window_start + dt.timedelta(days=offset)}/schedule.json"
        for offset in range(days + 1)
    ]


def expected_refreshed_object_keys(manifest: ImpactManifest) -> list[str]:
    keys = []
    for game in manifest.games:
        if game.pbp_status == "refreshed":
            keys.append(
                f"play-by-play/season={game.season}/date={game.date}/game_{game.game_id}.json"
            )
        if game.shift_status == "refreshed":
            keys.append(
                f"shift-charts/season={game.season}/date={game.date}/game_{game.game_id}.json"
            )
    return keys


def missing_bronze_objects(
    spark: SparkSession, object_keys: list[str]
) -> list[str]:
    """Check exact S3 keys through Hadoop without parsing their JSON bodies."""

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    missing = []
    for key in object_keys:
        path = path_class("s3a://nhl-bronze/" + key.lstrip("/"))
        if not path.getFileSystem(hadoop_conf).exists(path):
            missing.append(key)
    return missing


def duplicate_keys(frame: DataFrame, keys: list[str]) -> list[str]:
    rows = frame.groupBy(*keys).agg(count("*").alias("rows")).where(col("rows") > 1).collect()
    return [
        ", ".join(f"{key}={row[key]}" for key in keys) + f", rows={row['rows']}"
        for row in rows
    ]


def missing_game_ids(games: DataFrame, expected_ids: list[int]) -> list[int]:
    if not expected_ids:
        return []
    present = {
        int(row.game_id)
        for row in games.where(col("game_id").isin(expected_ids))
        .select("game_id")
        .distinct()
        .collect()
    }
    return sorted(set(expected_ids) - present)


def missing_shift_coverage(
    games: DataFrame,
    shifts: DataFrame,
    manifest: ImpactManifest,
    now: dt.datetime,
) -> list[int]:
    """Find supported completed games older than 24h with no shift rows."""

    unavailable = {
        game.game_id
        for game in manifest.games
        if game.shift_status in EXPLICIT_NO_SHIFT_DATA
    }
    cutoff = now - dt.timedelta(hours=24)
    eligible = {
        int(row.game_id)
        for row in games.where(
            col("game_id").isin(manifest.game_ids)
            & col("game_type").isin(2, 3)
            & col("game_state").isin(*FINAL_STATES)
            & (col("start_time_utc") < lit(cutoff))
        )
        .select("game_id")
        .distinct()
        .collect()
    } - unavailable
    if not eligible:
        return []
    covered = {
        int(row.game_id)
        for row in shifts.where(col("game_id").isin(sorted(eligible)))
        .select("game_id")
        .distinct()
        .collect()
    }
    return sorted(eligible - covered)


def tracking_reconciliation_issues(
    plays: DataFrame,
    attempts: DataFrame,
    frames: DataFrame,
    sequences: DataFrame,
    statuses: DataFrame,
    game_ids: list[int],
) -> list[str]:
    """Recompute the tracking state machine for impacted goal plays."""

    if not game_ids:
        return []
    goals = plays.where(
        col("game_id").isin(game_ids) & (col("type_desc_key") == "goal")
    ).select("season", "game_id", "event_id", "ppt_replay_url")
    scoped_attempts = attempts.where(col("game_id").isin(game_ids)).select(
        "season", "game_id", "event_id", "status"
    )
    frame_counts = (
        frames.where(col("game_id").isin(game_ids))
        .groupBy("season", "game_id", "event_id")
        .agg(count("*").alias("actual_frames"))
    )
    scoped_sequences = sequences.where(col("game_id").isin(game_ids)).select(
        "season", "game_id", "event_id", col("frame_count").alias("sequence_frames")
    )
    expected = (
        goals.alias("p")
        .join(scoped_attempts.alias("a"), ["season", "game_id", "event_id"], "left")
        .join(frame_counts.alias("f"), ["season", "game_id", "event_id"], "left")
        .join(scoped_sequences.alias("s"), ["season", "game_id", "event_id"], "left")
        .select(
            "season",
            "game_id",
            "event_id",
            when(col("ppt_replay_url").isNull(), "no_url")
            .when((col("status") == "success") & col("sequence_frames").isNotNull(), "available")
            .when(
                (col("status") == "success") & (col("actual_frames") > 0),
                "pending_gold_sequence_rebuild",
            )
            .when(
                (col("status") == "success") & col("actual_frames").isNull(),
                "pending_silver_rebuild",
            )
            .when(col("status") == "http_404", "not_tracked")
            .when(col("status").isNotNull(), "fetch_failed")
            .otherwise("not_attempted")
            .alias("expected_status"),
            col("actual_frames").alias("expected_frame_count"),
            lit(True).alias("expected_present"),
        )
    )
    actual = statuses.where(col("game_id").isin(game_ids)).select(
        "season",
        "game_id",
        "event_id",
        col("tracking_status").alias("actual_status"),
        col("frame_count").alias("actual_frame_count"),
        lit(True).alias("actual_present"),
    )
    mismatches = (
        expected.join(actual, ["season", "game_id", "event_id"], "full")
        .where(
            col("expected_present").isNull()
            | col("actual_present").isNull()
            | ~col("expected_status").eqNullSafe(col("actual_status"))
            | ~col("expected_frame_count").eqNullSafe(col("actual_frame_count"))
        )
        .collect()
    )
    issues = [
        (
            f"game_id={row.game_id}, event_id={row.event_id}, "
            f"expected={row.expected_status}/{row.expected_frame_count}, "
            f"actual={row.actual_status}/{row.actual_frame_count}"
        )
        for row in mismatches
    ]

    # These states are useful while individual jobs are running, but neither
    # is valid after the scheduled tracking frames and Gold sequence stages
    # have both completed successfully.
    pending = expected.where(col("expected_status").startswith("pending_")).collect()
    issues.extend(
        (
            f"game_id={row.game_id}, event_id={row.event_id}, "
            f"terminal_pipeline_status={row.expected_status}"
        )
        for row in pending
    )

    sequence_mismatches = (
        scoped_sequences.join(frame_counts, ["season", "game_id", "event_id"], "left")
        .where(
            col("actual_frames").isNull()
            | (col("sequence_frames") != col("actual_frames"))
        )
        .collect()
    )
    issues.extend(
        (
            f"game_id={row.game_id}, event_id={row.event_id}, "
            f"sequence_frames={row.sequence_frames}, actual_frames={row.actual_frames}"
        )
        for row in sequence_mismatches
    )
    return issues


def _summary_total(summary: dict | None) -> int | None:
    if not summary or summary.get("total-records") is None:
        return None
    return int(summary["total-records"])


def snapshot_info(spark: SparkSession, table: str) -> SnapshotInfo:
    snapshots = (
        spark.read.table(f"{table}.snapshots")
        .select("committed_at", "summary")
        .orderBy(col("committed_at").desc())
        .limit(2)
        .collect()
    )
    if not snapshots:
        raise ValueError(f"{table} has no Iceberg snapshots")
    current_total = _summary_total(snapshots[0].summary)
    if current_total is None:
        current_total = spark.read.table(table).count()
    previous_total = _summary_total(snapshots[1].summary) if len(snapshots) > 1 else None
    return SnapshotInfo(
        committed_at=snapshots[0].committed_at,
        total_records=current_total,
        previous_total_records=previous_total,
    )


def _bounded(values: list, limit: int = 25) -> str:
    shown = values[:limit]
    suffix = f" (+{len(values) - limit} more)" if len(values) > limit else ""
    return f"{shown}{suffix}"


def _record(results: list[CheckResult], name: str, issues: list, success: str) -> None:
    detail = success if not issues else _bounded(issues)
    result = CheckResult(name=name, passed=not issues, detail=detail)
    results.append(result)
    print(
        "pipeline-validation "
        f"check={result.name} outcome={'pass' if result.passed else 'fail'} detail={result.detail}"
    )


def run_validation(
    spark: SparkSession,
    manifest: ImpactManifest,
    minimum_row_ratio: float,
    now: dt.datetime | None = None,
) -> list[CheckResult]:
    results = []
    now = now or dt.datetime.now(dt.timezone.utc)

    schedule_keys = expected_schedule_keys(manifest)
    declared = set(manifest.objects)
    missing_declarations = sorted(set(schedule_keys) - declared)
    _record(
        results,
        "schedule_manifest_coverage",
        missing_declarations,
        f"objects={len(schedule_keys)}",
    )
    missing_objects = missing_bronze_objects(spark, schedule_keys)
    _record(results, "schedule_object_exists", missing_objects, f"objects={len(schedule_keys)}")

    refreshed_keys = expected_refreshed_object_keys(manifest)
    missing_refreshed_keys = sorted(set(refreshed_keys) - declared)
    _record(
        results,
        "refreshed_object_manifest_coverage",
        missing_refreshed_keys,
        f"objects={len(refreshed_keys)}",
    )
    missing_refreshed_objects = missing_bronze_objects(spark, refreshed_keys)
    _record(
        results,
        "refreshed_object_exists",
        missing_refreshed_objects,
        f"objects={len(refreshed_keys)}",
    )

    missing_tables = [table for table in REQUIRED_TABLES if not spark.catalog.tableExists(table)]
    _record(results, "required_tables_exist", missing_tables, f"tables={len(REQUIRED_TABLES)}")
    if missing_tables:
        return results

    tables = {table: spark.read.table(table) for table in REQUIRED_TABLES}
    missing_games = missing_game_ids(
        tables["nhl.silver.games"], manifest.refreshed_pbp_game_ids
    )
    _record(
        results,
        "refreshed_games_in_silver",
        missing_games,
        f"games={len(manifest.refreshed_pbp_game_ids)}",
    )

    scoped_plays = tables["nhl.silver.plays"].where(col("game_id").isin(manifest.game_ids))
    play_duplicates = duplicate_keys(scoped_plays, ["game_id", "event_id"])
    _record(results, "silver_plays_unique", play_duplicates, f"games={len(manifest.game_ids)}")

    scoped_shifts = tables["nhl.silver.shifts"].where(col("game_id").isin(manifest.game_ids))
    shift_duplicates = duplicate_keys(
        scoped_shifts, ["game_id", "player_id", "period", "shift_number"]
    )
    _record(results, "silver_shifts_unique", shift_duplicates, f"games={len(manifest.game_ids)}")

    uncovered_shifts = missing_shift_coverage(
        tables["nhl.silver.games"], tables["nhl.silver.shifts"], manifest, now
    )
    _record(results, "completed_game_shift_coverage", uncovered_shifts, "missing_games=0")

    snapshots = {}
    snapshot_issues = []
    row_drop_issues = []
    for table in REQUIRED_TABLES:
        try:
            info = snapshot_info(spark, table)
        # Catalog connector failures do not share one stable Python exception
        # base across Spark/Iceberg versions. Convert them into a named check
        # failure so the remaining tables still report their outcomes.
        except Exception as exc:
            snapshot_issues.append(f"{table}: {exc}")
            continue
        snapshots[table] = info
        print(
            "pipeline-validation "
            f"table={table} rows={info.total_records} committed_at={info.committed_at.isoformat()}"
        )
        previous = info.previous_total_records
        if previous is not None and info.total_records < previous * minimum_row_ratio:
            row_drop_issues.append(
                f"{table}: current={info.total_records}, previous={previous}, "
                f"minimum_ratio={minimum_row_ratio}"
            )
    _record(results, "iceberg_snapshot_metadata", snapshot_issues, f"tables={len(snapshots)}")
    _record(results, "table_row_count_guard", row_drop_issues, f"minimum_ratio={minimum_row_ratio}")

    freshness_dependencies = {
        "nhl.gold.player_shots": (
            "nhl.silver.games",
            "nhl.silver.plays",
            "nhl.silver.players",
            "nhl.silver.teams",
        ),
        "nhl.gold.goal_tracking_sequences": ("nhl.silver.tracking_frames",),
        "nhl.gold.goal_tracking_status": (
            "nhl.silver.plays",
            "nhl.silver.tracking_attempts",
            "nhl.silver.tracking_frames",
            "nhl.gold.goal_tracking_sequences",
        ),
    }
    stale_gold = []
    for target, sources in freshness_dependencies.items():
        if target not in snapshots or any(source not in snapshots for source in sources):
            continue
        newest_source = max(snapshots[source].committed_at for source in sources)
        if snapshots[target].committed_at < newest_source:
            stale_gold.append(
                f"{target}: target={snapshots[target].committed_at.isoformat()}, "
                f"newest_source={newest_source.isoformat()}"
            )
    _record(results, "gold_snapshot_freshness", stale_gold, "stale_tables=0")

    for table, keys in (
        ("nhl.silver.tracking_attempts", ["season", "game_id", "event_id"]),
        ("nhl.gold.goal_tracking_sequences", ["season", "game_id", "event_id"]),
        ("nhl.gold.goal_tracking_status", ["season", "game_id", "event_id"]),
    ):
        scoped = tables[table].where(col("game_id").isin(manifest.game_ids))
        duplicates = duplicate_keys(scoped, keys)
        check_name = table.replace("nhl.", "").replace(".", "_") + "_unique"
        _record(results, check_name, duplicates, "duplicates=0")

    tracking_issues = tracking_reconciliation_issues(
        tables["nhl.silver.plays"],
        tables["nhl.silver.tracking_attempts"],
        tables["nhl.silver.tracking_frames"],
        tables["nhl.gold.goal_tracking_sequences"],
        tables["nhl.gold.goal_tracking_status"],
        manifest.game_ids,
    )
    _record(results, "tracking_status_reconciles", tracking_issues, "mismatches=0")
    return results


def validation_arguments():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--minimum-row-ratio", type=float, default=0.80)
    args, _ = parser.parse_known_args()
    if not 0.0 <= args.minimum_row_ratio <= 1.0:
        raise ValueError("--minimum-row-ratio must be between 0 and 1")
    return args


def main() -> None:
    started = time.monotonic()
    spark = get_spark("validate-pipeline")
    processing = processing_arguments()
    validation = validation_arguments()
    if processing.processing_mode != "incremental":
        raise ValueError("pipeline validation requires an incremental impact manifest")

    manifest = parse_impact_manifest(
        load_impact_manifest(spark, processing.impact_manifest_key)
    )
    results = run_validation(spark, manifest, validation.minimum_row_ratio)
    failures = [result for result in results if not result.passed]
    duration = time.monotonic() - started
    print(
        "pipeline-validation "
        f"run_id={manifest.run_id} checks={len(results)} failures={len(failures)} "
        f"duration_seconds={duration:.3f}"
    )
    if failures:
        names = ", ".join(result.name for result in failures)
        raise RuntimeError(f"pipeline validation failed: {names}")


if __name__ == "__main__":
    main()
