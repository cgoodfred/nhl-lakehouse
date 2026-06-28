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

  * Rate limit + 429 backoff mirror ingest/internal/nhl/client.go: token
    bucket at 2 req/s sustained, burst 5; on 429 we honor Retry-After when
    present, otherwise exponential backoff 1s→2s→4s→...→60s cap, max 6
    in-request retries. Persistent 429 falls through to status='http_other'
    so the next job run with --retry-transient can pick it up.

  * Python 3.8 in the apache/spark:3.5.7-python3 base image: NO `X | None`,
    `datetime.UTC`, or `list[T]` runtime expressions. We use
    `from __future__ import annotations` to keep modern syntax in TYPE hints
    only (lazy-evaluated, never run); runtime expressions stay 3.8-compatible.

Knobs (sparkConf):
  - spark.tracking.retry_transient   bool,  default false
  - spark.tracking.season            str,   default ""    (empty = all seasons)
  - spark.tracking.rate_per_sec      float, default 2.0
  - spark.tracking.burst             int,   default 5
  - spark.tracking.max_retries       int,   default 6  (in-request 429 retries)
  - spark.tracking.timeout_sec       int,   default 30
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

# Rate-limit / backoff defaults — mirror the Go ingest client (2 req/s sustained,
# burst 5, 6 retries on 429, exponential 1s→60s).
DEFAULT_RATE_PER_SEC = 2.0
DEFAULT_BURST        = 5
DEFAULT_MAX_RETRIES  = 6
MAX_BACKOFF_SEC      = 60.0
DEFAULT_TIMEOUT_SEC  = 30


class TokenBucket:
    """Single-threaded token-bucket rate limiter.

    Mirrors golang.org/x/time/rate semantics for the driver's serial fetch
    loop: tokens refill continuously at `rate_per_sec`; `wait()` consumes
    one token, sleeping if the bucket is empty. Burst lets the first N
    requests fly without waiting before settling to the sustained rate."""

    def __init__(self, rate_per_sec: float, burst: int, _clock=time.monotonic):
        self.rate     = float(rate_per_sec)
        self.capacity = float(burst)
        self.tokens   = float(burst)
        self._clock   = _clock
        self._last    = _clock()

    def wait(self, _sleep=time.sleep) -> None:
        now = self._clock()
        self.tokens = min(self.capacity, self.tokens + (now - self._last) * self.rate)
        self._last = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return
        sleep_for = (1.0 - self.tokens) / self.rate
        _sleep(sleep_for)
        self._last  = self._clock()
        self.tokens = 0.0


def backoff_delay(attempt: int, retry_after: str | None) -> float:
    """Seconds to wait before the next 429 retry. Honors Retry-After when
    present (NHL CDN sends seconds, not HTTP-date), else exponential
    1, 2, 4, 8, 16, 32, 60, 60... capped at MAX_BACKOFF_SEC."""
    if retry_after:
        try:
            secs = int(retry_after)
            if secs > 0:
                return float(secs)
        except ValueError:
            pass
    return min(MAX_BACKOFF_SEC, float(1 << attempt))


@dataclass
class FetchResult:
    # 'success' | 'http_404' | 'http_other' | 'fetch_error' | 'invalid_payload'
    status: str
    http_code: int | None
    body: bytes | None                  # populated only on success
    frame_count: int | None             # populated only on success
    error: str | None


def fetch_tracking(
    url: str,
    headers: dict,
    limiter: TokenBucket | None = None,
    timeout: int = DEFAULT_TIMEOUT_SEC,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_fn=backoff_delay,
    _sleep=time.sleep,
) -> FetchResult:
    """Single logical fetch (with internal 429 retries) + payload validation.

    Cross-RUN retries (transient failures retried by re-running the job) are
    a different concern handled by --retry-transient. This function's retry
    loop is only for in-request 429s, where the upstream is asking us to
    slow down for a moment and a longer-term retry would be wasteful.

    A 200 status is NOT trusted on its own: the upstream CDN occasionally
    returns a 200 with a Cloudflare challenge page or unrelated HTML. We
    parse the body and require a non-empty top-level JSON list before
    reporting success."""
    for attempt in range(max_retries + 1):
        if limiter is not None:
            limiter.wait()
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            return FetchResult(
                "fetch_error", None, None, None, f"{type(exc).__name__}: {exc}",
            )

        if resp.status_code == 429:
            if attempt < max_retries:
                _sleep(backoff_fn(attempt, resp.headers.get("Retry-After")))
                continue
            # Exhausted in-request retries; surface an ops-useful message
            # instead of the (often empty) 429 body. Re-runs with
            # --retry-transient will still pick this up since http_other is
            # in TRANSIENT_STATUSES.
            return FetchResult(
                "http_other", 429, None, None,
                f"exhausted {max_retries} retries on 429",
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

    # Unreachable — every code path inside the loop either returns or
    # continues, and the continue path is gated by attempt < max_retries.
    raise RuntimeError("fetch_tracking loop exited without returning")


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


def _read_goals(spark: SparkSession, season: int | None) -> DataFrame:
    """Goals with a tracking URL in silver.plays, optionally filtered to one
    season for staged backfill (CDN load + wall-clock pacing). NHL season
    codes are start-year + end-year with no separator, e.g. 20252026 for the
    2025-26 season — same encoding as nhl.silver.plays.season."""
    df = (
        spark.read.table("nhl.silver.plays")
        .where((col("type_desc_key") == "goal") & col("ppt_replay_url").isNotNull())
        .select("season", "game_id", "event_id", "ppt_replay_url")
    )
    if season is not None:
        df = df.where(col("season") == season)
    return df


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
    season_raw   = spark.conf.get("spark.tracking.season", "").strip()
    season       = int(season_raw) if season_raw else None
    rate_per_sec = float(spark.conf.get("spark.tracking.rate_per_sec", str(DEFAULT_RATE_PER_SEC)))
    burst        = int(  spark.conf.get("spark.tracking.burst",        str(DEFAULT_BURST)))
    max_retries  = int(  spark.conf.get("spark.tracking.max_retries",  str(DEFAULT_MAX_RETRIES)))
    timeout_sec  = int(  spark.conf.get("spark.tracking.timeout_sec",  str(DEFAULT_TIMEOUT_SEC)))

    existing = _read_existing(spark)
    goals    = _read_goals(spark, season)
    to_fetch = candidates(existing, goals, retry_transient).collect()

    print(
        f"bronze-tracking-ingest: retry_transient={retry_transient}, "
        f"season={season or 'all'}, "
        f"rate={rate_per_sec}/s burst={burst} max_retries={max_retries} "
        f"timeout={timeout_sec}s, candidates={len(to_fetch)}"
    )
    if not to_fetch:
        print("bronze-tracking-ingest: nothing to fetch")
        return

    s3      = _s3_client()
    limiter = TokenBucket(rate_per_sec, burst)
    attempts: list[dict] = []
    for i, row in enumerate(to_fetch, start=1):
        result = fetch_tracking(
            row.ppt_replay_url, PPT_HEADERS,
            limiter=limiter, timeout=timeout_sec, max_retries=max_retries,
        )
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
