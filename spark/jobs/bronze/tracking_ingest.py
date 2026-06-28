"""Fetch per-goal NHL tracking payloads and land them in bronze + log attempts.

Source:  nhl.silver.plays.ppt_replay_url  (only rows where type_desc_key='goal')
Targets:
  - s3://nhl-bronze/tracking/season={season}/game_id={game_id}/event_id={event_id}/tracking.json
  - nhl.silver.tracking_attempts (Iceberg, current-state, one row per (game_id, event_id))

The actual JSON normalization to silver lives in silver/tracking_frames.py. This
job only does the fetch + bronze write + attempt logging.

Design notes:

  * Fetches run on the DRIVER, not executors. Payloads are tiny (~150KB), the
    bottleneck is rate-limiting against an external CDN, and distributing the
    fetch loop across executors would only risk getting rate-limited harder.

  * `tracking_attempts` is current-state. One row per (game_id, event_id),
    never multiple. Re-running the job is a no-op for every event already in
    the table; --retry-transient widens the candidate set to include prior
    http_other/fetch_error/invalid_payload rows so they get overwritten.

  * 200 responses are NOT trusted blindly. The CDN occasionally returns a 200
    with an HTML challenge page; we parse the body and require a non-empty
    top-level list BEFORE writing bronze, otherwise we'd commit garbage to
    durable storage and have to clean it up downstream.

  * boto3 needs its OWN endpoint/region/path-style config — the existing
    spark.hadoop.fs.s3a.* configs apply to the JVM Hadoop FileSystem path that
    Iceberg uses, not to a Python boto3 client. SeaweedFS-compat options are
    set explicitly in _s3_client().

  * Python 3.8 in the apache/spark:3.5.7-python3 base image: NO `X | None`,
    `datetime.UTC`, or `list[T]` runtime expressions. We use
    `from __future__ import annotations` to keep modern syntax in TYPE hints
    only (lazy-evaluated, never run); runtime expressions stay 3.8-compatible.

Knobs (sparkConf):
  - spark.tracking.retry_transient    bool, default false
  - spark.tracking.request_delay_ms   int,  default 150
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
import requests
from botocore.config import Config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import get_spark

BRONZE_BUCKET = "nhl-bronze"
BRONZE_PREFIX = "tracking"

# Browser-compatible headers required for wsr.nhle.com. Without these the CDN
# returns a Cloudflare interstitial page instead of the JSON payload.
PPT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nhl.com/",
    "Origin":  "https://www.nhl.com",
}

ATTEMPTS_SCHEMA = StructType([
    StructField("game_id",           LongType(),      nullable=False),
    StructField("event_id",          LongType(),      nullable=False),
    StructField("season",            IntegerType(),   nullable=False),
    StructField("source_url",        StringType(),    nullable=False),
    StructField("source_object_key", StringType(),    nullable=True),
    StructField("attempted_at",      TimestampType(), nullable=False),
    StructField("status",            StringType(),    nullable=False),
    StructField("http_code",         IntegerType(),   nullable=True),
    StructField("frame_count",       IntegerType(),   nullable=True),
    StructField("error_message",     StringType(),    nullable=True),
])

# Statuses that mean "tried, can be retried with --retry-transient".
# invalid_payload is included because a Cloudflare interstitial returning a 200
# with HTML is by nature transient — a later retry from a fresh IP / a moment
# later may get the real JSON.
TRANSIENT_STATUSES = ("http_other", "fetch_error", "invalid_payload")


@dataclass
class FetchResult:
    # 'success' | 'http_404' | 'http_other' | 'fetch_error' | 'invalid_payload'
    status: str
    http_code: int | None
    body: bytes | None                  # populated only on success
    frame_count: int | None             # populated only on success
    error: str | None


def fetch_tracking(url: str, headers: dict, timeout: int = 10) -> FetchResult:
    """Single HTTP fetch + payload validation. No retries — re-run the job
    with --retry-transient for that.

    A 200 status is NOT trusted on its own: the upstream CDN occasionally
    returns a 200 with a Cloudflare challenge page or an unrelated HTML
    response. We parse the body and require a non-empty top-level JSON list
    (the tracking payload shape) before reporting success. Anything else at
    200 becomes 'invalid_payload' so we don't write garbage to bronze."""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        return FetchResult(
            "fetch_error", None, None, None, f"{type(exc).__name__}: {exc}",
        )
    if resp.status_code == 404:
        return FetchResult("http_404", 404, None, None, None)
    if resp.status_code != 200:
        return FetchResult(
            "http_other", resp.status_code, None, None, resp.text[:200],
        )
    # 200 — parse + validate before reporting success.
    try:
        parsed = json.loads(resp.content)
    except (ValueError, UnicodeDecodeError) as exc:
        return FetchResult(
            "invalid_payload", 200, None, None,
            f"JSON parse: {type(exc).__name__}: {exc}",
        )
    if not isinstance(parsed, list):
        return FetchResult(
            "invalid_payload", 200, None, None,
            f"expected top-level list, got {type(parsed).__name__}",
        )
    if not parsed:
        return FetchResult(
            "invalid_payload", 200, None, None,
            "expected non-empty list, got empty list",
        )
    return FetchResult("success", 200, resp.content, len(parsed), None)


def candidates(
    existing: DataFrame | None,
    goals: DataFrame,
    retry_transient: bool,
) -> DataFrame:
    """Goals we still need to fetch.

    If retry_transient is True, prior http_other/fetch_error rows are removed
    from the "already attempted" set so they become candidates again. The
    SAME filtered set must be used when merging the write at the end —
    otherwise we'd filter at write time but never refetch."""
    if existing is None:
        return goals
    attempted = existing
    if retry_transient:
        attempted = attempted.filter(~col("status").isin(*TRANSIENT_STATUSES))
    return goals.join(
        attempted.select("game_id", "event_id"),
        on=["game_id", "event_id"],
        how="left_anti",
    )


def merge_attempts(
    existing: DataFrame | None,
    new_df: DataFrame,
    retry_transient: bool,
) -> DataFrame:
    """Combine prior + new attempts into the next current-state snapshot.

    Mirrors the filtering in `candidates`: with retry_transient=True we drop
    prior transient-failure rows from existing so the union with new_df
    overwrites them with the fresh attempt result. Without that, the union
    would put the new success row alongside the old failure row → duplicate
    (game_id, event_id) keys."""
    if existing is None:
        return new_df
    preserved = existing
    if retry_transient:
        preserved = preserved.filter(~col("status").isin(*TRANSIENT_STATUSES))
    return preserved.unionByName(new_df)


def _read_existing(spark: SparkSession) -> DataFrame | None:
    if not spark.catalog.tableExists("nhl.silver.tracking_attempts"):
        return None
    return spark.read.table("nhl.silver.tracking_attempts")


def _read_goals(spark: SparkSession) -> DataFrame:
    return (
        spark.read.table("nhl.silver.plays")
        .where((col("type_desc_key") == "goal") & col("ppt_replay_url").isNotNull())
        .select("season", "game_id", "event_id", "ppt_replay_url")
    )


def _s3_client():
    # boto3 picks up AWS_ENDPOINT_URL_S3, AWS_REGION, AWS_ACCESS_KEY_ID,
    # AWS_SECRET_ACCESS_KEY from the environment. Path-style addressing has
    # to come from the Config object — SeaweedFS doesn't support virtual-
    # hosted-style URLs.
    return boto3.client(
        "s3",
        config=Config(s3={"addressing_style": "path"}),
    )


def _object_key(season: int, game_id: int, event_id: int) -> str:
    # event_id is a DIRECTORY (event_id=NNN/), not a filename suffix.
    # Spark partition discovery only picks up directory segments, so silver's
    # spark.read.json(...) with basePath=tracking/ would surface season and
    # game_id but NOT event_id if we used `event_id=NNN.json`.
    return (
        f"{BRONZE_PREFIX}/season={season}/game_id={game_id}"
        f"/event_id={event_id}/tracking.json"
    )


def main():
    spark = get_spark("bronze-tracking-ingest")

    retry_transient = (
        spark.conf.get("spark.tracking.retry_transient", "false").lower() == "true"
    )
    request_delay_ms = int(spark.conf.get("spark.tracking.request_delay_ms", "150"))

    existing = _read_existing(spark)
    goals    = _read_goals(spark)
    to_fetch = candidates(existing, goals, retry_transient).collect()

    print(
        f"bronze-tracking-ingest: retry_transient={retry_transient}, "
        f"candidates={len(to_fetch)}"
    )
    if not to_fetch:
        print("bronze-tracking-ingest: nothing to fetch")
        return

    s3 = _s3_client()
    attempts: list[dict] = []
    for i, row in enumerate(to_fetch, start=1):
        result = fetch_tracking(row.ppt_replay_url, PPT_HEADERS)
        attempt = {
            "game_id":           row.game_id,
            "event_id":          row.event_id,
            "season":            row.season,
            "source_url":        row.ppt_replay_url,
            "source_object_key": None,
            "attempted_at":      datetime.now(timezone.utc),
            "status":            result.status,
            "http_code":         result.http_code,
            "frame_count":       None,
            "error_message":     result.error,
        }
        if result.status == "success":
            key = _object_key(row.season, row.game_id, row.event_id)
            # fetch_tracking has already parsed + validated the body, so the
            # PUT lands only known-good JSON. frame_count comes from the same
            # parsed length — no second parse here.
            s3.put_object(
                Bucket=BRONZE_BUCKET, Key=key,
                Body=result.body, ContentType="application/json",
            )
            attempt["source_object_key"] = key
            attempt["frame_count"]       = result.frame_count
        attempts.append(attempt)
        if i % 100 == 0 or i == len(to_fetch):
            print(f"  {i}/{len(to_fetch)} attempted")
        time.sleep(request_delay_ms / 1000.0)

    new_df   = spark.createDataFrame(attempts, schema=ATTEMPTS_SCHEMA)
    combined = merge_attempts(existing, new_df, retry_transient)
    combined.coalesce(1).writeTo("nhl.silver.tracking_attempts") \
        .partitionedBy("season").createOrReplace()

    summary = (
        spark.read.table("nhl.silver.tracking_attempts")
        .groupBy("status").count().collect()
    )
    print("bronze-tracking-ingest: complete")
    for row in summary:
        print(f"  {row['status']}: {row['count']}")


if __name__ == "__main__":
    main()
