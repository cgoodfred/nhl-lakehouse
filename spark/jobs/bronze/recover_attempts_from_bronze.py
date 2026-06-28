"""Recover silver.tracking_attempts from bronze JSON files.

One-off recovery for the scenario where the fetch loop completed but the
attempts-table write failed (e.g., OAuth token expired mid-loop, before
the periodic-flush fix landed). The bronze writes persisted via boto3
inside the loop; only the in-memory attempts buffer was lost with the
driver process.

This script lists bronze under \"tracking/season=X/\", derives
(season, game_id, event_id) from each path, joins against silver.plays
for the source_url, and writes status='success' rows to the attempts
table. Events that originally failed (http_404, fetch_error, etc.) have
no bronze file and won't appear here; they're left for the regular
ingest job to re-attempt on its next run.

frame_count is NOT populated by this script. We deliberately skip
re-downloading each ~150KB file just to count array elements — the
silver.tracking_frames job will compute actual_frames from the same
bronze JSON anyway, and the gold view's 'available' check joins on
that, not on attempts.frame_count. The latter is an ops/debug column
that just stays NULL for recovered rows.

Knobs (sparkConf):
  - spark.tracking.season   str, default \"\" (empty = recover all seasons)
"""

from __future__ import annotations

from datetime import datetime, timezone

import boto3
from botocore.config import Config
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from common import get_spark
from tracking_ingest import (
    ATTEMPTS_SCHEMA,
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    merge_attempts,
)


def _s3_client():
    # Same config as the main ingest job — see tracking_ingest._s3_client.
    return boto3.client("s3", config=Config(s3={"addressing_style": "path"}))


def _list_bronze_keys(s3, prefix: str):
    """Stream every object key under the given bronze prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def _parse_key(key: str):
    """tracking/season=X/game_id=Y/event_id=Z/tracking.json -> (X, Y, Z).

    Returns None if the key doesn't match the expected layout (e.g. an
    accidental stray file in the bronze prefix)."""
    parts = key.split("/")
    if (
        len(parts) != 5
        or parts[0] != BRONZE_PREFIX
        or not parts[1].startswith("season=")
        or not parts[2].startswith("game_id=")
        or not parts[3].startswith("event_id=")
        or parts[4] != "tracking.json"
    ):
        return None
    try:
        return (
            int(parts[1].split("=")[1]),
            int(parts[2].split("=")[1]),
            int(parts[3].split("=")[1]),
        )
    except ValueError:
        return None


def _read_plays_for_urls(spark, seasons: list[int]) -> DataFrame:
    """Pull (game_id, event_id) -> ppt_replay_url for the recovered season(s).

    Bounded read so we don't scan all plays just to look up a few thousand
    URLs."""
    df = (
        spark.read.table("nhl.silver.plays")
        .where((col("type_desc_key") == "goal") & col("ppt_replay_url").isNotNull())
        .select("game_id", "event_id", col("ppt_replay_url").alias("source_url"))
    )
    if seasons:
        df = df.where(col("season").isin(*seasons))
    return df


def main():
    spark = get_spark("bronze-recover-tracking-attempts")

    season_raw = spark.conf.get("spark.tracking.season", "").strip()
    season     = int(season_raw) if season_raw else None

    list_prefix = (
        f"{BRONZE_PREFIX}/season={season}/" if season else f"{BRONZE_PREFIX}/"
    )

    s3 = _s3_client()
    print(f"bronze-recover-tracking-attempts: listing s3://{BRONZE_BUCKET}/{list_prefix}")
    parsed = []
    skipped = 0
    for key in _list_bronze_keys(s3, list_prefix):
        result = _parse_key(key)
        if result is None:
            skipped += 1
            continue
        sea, gid, evid = result
        parsed.append((sea, gid, evid, key))

    print(f"  found {len(parsed)} tracking files (skipped {skipped} non-matching keys)")
    if not parsed:
        print("nothing to recover")
        return

    # Pull source_url from silver.plays so the recovered rows match what the
    # regular ingest would have written. Drives the join through the seasons
    # actually present in bronze, not just the season conf knob (recovery
    # without a season conf still gets a bounded plays read).
    seasons_present = sorted({s for s, _, _, _ in parsed})
    plays = _read_plays_for_urls(spark, seasons_present)

    bronze_df = spark.createDataFrame(
        parsed,
        schema="season int, game_id long, event_id long, source_object_key string",
    )
    joined = bronze_df.join(plays, ["game_id", "event_id"], "left").collect()

    now = datetime.now(timezone.utc)
    fallback_count = 0
    new_attempts = []
    for r in joined:
        source_url = r.source_url
        if source_url is None:
            # Bronze has a file for an event silver.plays doesn't know about.
            # Shouldn't happen in practice, but recover defensively rather
            # than dropping the row — record a placeholder URL so the row
            # still satisfies the non-null source_url schema invariant.
            fallback_count += 1
            source_url = (
                f"recovered:s3://{BRONZE_BUCKET}/{r.source_object_key}"
            )
        new_attempts.append({
            "game_id":           r.game_id,
            "event_id":          r.event_id,
            "season":            r.season,
            "source_url":        source_url,
            "source_object_key": r.source_object_key,
            "attempted_at":      now,
            "status":            "success",
            "http_code":         200,
            "frame_count":       None,
            "error_message":     None,
        })
    if fallback_count:
        print(f"  WARN: {fallback_count} bronze files had no matching silver.plays row")

    new_df = spark.createDataFrame(new_attempts, schema=ATTEMPTS_SCHEMA)

    existing = None
    if spark.catalog.tableExists("nhl.silver.tracking_attempts"):
        existing = spark.read.table("nhl.silver.tracking_attempts")

    combined = merge_attempts(existing, new_df)
    combined.coalesce(1).writeTo("nhl.silver.tracking_attempts") \
        .partitionedBy("season").createOrReplace()

    total = spark.read.table("nhl.silver.tracking_attempts").count()
    print(f"bronze-recover-tracking-attempts: complete (attempts table rows={total})")


if __name__ == "__main__":
    main()
