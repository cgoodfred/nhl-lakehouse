"""Shared helpers for Spark jobs (silver and gold tiers)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # Ensure both tier namespaces exist; Iceberg requires the namespace to
    # exist before tables can be created in it. Idempotent + cheap.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nhl.gold")
    return spark


@dataclass
class ImpactScope:
    """Paths and game IDs refreshed by one ingest run."""

    pbp_game_ids: list[int]
    shift_game_ids: list[int]
    pbp_paths: list[str]
    shift_paths: list[str]


def processing_arguments():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--processing-mode", choices=("full", "incremental"), default="incremental")
    parser.add_argument("--impact-manifest-key", default="")
    args, _ = parser.parse_known_args()
    if args.processing_mode == "incremental" and not args.impact_manifest_key:
        raise ValueError("--impact-manifest-key is required in incremental mode")
    return args


def load_impact_scope(spark: SparkSession, manifest_key: str) -> ImpactScope:
    """Load the compact ingest manifest and return only refreshed source paths."""

    path = (
        manifest_key
        if manifest_key.startswith("s3")
        else "s3a://nhl-bronze/" + manifest_key.lstrip("/")
    )
    # Ingest writes impact manifests with json.MarshalIndent. Without
    # multiLine, Spark treats each line as a separate JSON record and the
    # scope fields become null, causing an incremental job to silently no-op.
    row = spark.read.option("multiLine", "true").json(path).first()
    if row is None:
        raise ValueError(f"impact manifest is empty: {path}")
    payload = row.asDict(recursive=True)
    objects = [str(value) for value in (payload.get("objects") or [])]
    games = payload.get("games") or []
    pbp_game_ids = sorted(
        {
            int(game["game_id"])
            for game in games
            if game.get("game_id") is not None and game.get("pbp_status") == "refreshed"
        }
    )
    shift_game_ids = sorted(
        {
            int(game["game_id"])
            for game in games
            if game.get("game_id") is not None and game.get("shift_status") == "refreshed"
        }
    )
    return ImpactScope(
        pbp_game_ids=pbp_game_ids,
        shift_game_ids=shift_game_ids,
        pbp_paths=[
            "s3a://nhl-bronze/" + key for key in objects if key.startswith("play-by-play/")
        ],
        shift_paths=[
            "s3a://nhl-bronze/" + key for key in objects if key.startswith("shift-charts/")
        ],
    )


def source_paths(scope, full_path: str, kind: str) -> list[str]:
    if scope is None:
        return [full_path]
    paths = scope.pbp_paths if kind == "pbp" else scope.shift_paths
    return paths


def write_incremental(spark: SparkSession, table: str, frame, game_ids: list[int]) -> None:
    """Replace all rows for affected games, then append the refreshed rows."""

    if not game_ids:
        print(f"{table}: no impacted games; nothing to write")
        return
    if not spark.catalog.tableExists(table):
        frame.writeTo(table).createOrReplace()
        return
    values = ",".join(str(game_id) for game_id in game_ids)
    spark.sql(f"DELETE FROM {table} WHERE game_id IN ({values})")
    frame.writeTo(table).append()
