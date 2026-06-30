"""Build the gold.goal_tracking_status table — per-goal tracking availability.

Source:  nhl.silver.plays, nhl.silver.tracking_attempts, nhl.silver.tracking_frames,
         nhl.gold.goal_tracking_sequences
Target:  nhl.gold.goal_tracking_status (Iceberg, one row per goal event)

One row per goal event with a single `tracking_status` enum the viz can
switch on:

  - 'available'              fetched AND parsed AND gold sequence row exists
  - 'pending_gold_sequence_rebuild'
                             silver frames exist but gold.goal_tracking_sequences
                             hasn't been rebuilt yet
  - 'pending_silver_rebuild' fetched but silver.tracking_frames hasn't been
                             rebuilt yet (the freshness gap between bronze
                             ingest writing attempts.status='success' and
                             silver-tracking-frames running)
  - 'not_tracked'            CDN returned 404 (preseason / non-PPT game /
                             event predates PPT rollout)
  - 'fetch_failed'           any other attempt outcome (http_other,
                             fetch_error, invalid_payload) — eligible for
                             retry via bronze-tracking-ingest's
                             --retry-transient flag
  - 'not_attempted'          goal has a ppt_replay_url but no attempts row
                             yet (haven't run bronze ingest for this season)
  - 'no_url'                 silver.plays.ppt_replay_url is null

`frame_count` comes from the actual silver.tracking_frames row count, NOT
from attempts.frame_count — the latter is best-effort metadata. The viz
animation's "available" contract is stricter: it requires the Gold serving
sequence row to exist, because the app reads gold.goal_tracking_sequences
first for playback.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, current_timestamp, when

from common import get_spark


def transform_goal_tracking_status(
    plays_df: DataFrame,
    attempts_df: DataFrame,
    frames_df: DataFrame,
    sequences_df: DataFrame,
) -> DataFrame:
    """Per-goal denormalization of plays + attempts + frame counts.

    Pure transform so the tests can build small in-memory DataFrames per
    CASE branch without hitting the catalog."""

    # Frame counts pre-aggregated to one row per (season, game_id, event_id)
    # so the LEFT JOIN below stays 1:1 with plays and the gold output has
    # exactly one row per goal.
    frame_counts = (
        frames_df
        .groupBy("season", "game_id", "event_id")
        .agg(count("*").alias("actual_frames"))
    )
    sequence_counts = (
        sequences_df
        .select("season", "game_id", "event_id", "frame_count")
        .withColumnRenamed("frame_count", "sequence_frames")
    )

    goals = plays_df.where(col("type_desc_key") == "goal")

    return (
        goals.alias("p")
        .join(
            attempts_df.alias("ta"),
            (col("p.season")   == col("ta.season"))
            & (col("p.game_id")  == col("ta.game_id"))
            & (col("p.event_id") == col("ta.event_id")),
            how="left",
        )
        .join(
            frame_counts.alias("fc"),
            (col("p.season")   == col("fc.season"))
            & (col("p.game_id")  == col("fc.game_id"))
            & (col("p.event_id") == col("fc.event_id")),
            how="left",
        )
        .join(
            sequence_counts.alias("gs"),
            (col("p.season")   == col("gs.season"))
            & (col("p.game_id")  == col("gs.game_id"))
            & (col("p.event_id") == col("gs.event_id")),
            how="left",
        )
        .select(
            col("p.game_id").alias("game_id"),
            col("p.event_id").alias("event_id"),
            col("p.season").alias("season"),
            col("p.ppt_replay_url").alias("ppt_replay_url"),
            # CASE order matters: no_url first; then success + gold/silver
            # freshness states before generic status checks.
            when(
                col("p.ppt_replay_url").isNull(), "no_url",
            ).when(
                (col("ta.status") == "success") & col("gs.sequence_frames").isNotNull(),
                "available",
            ).when(
                (col("ta.status") == "success") & (col("fc.actual_frames") > 0),
                "pending_gold_sequence_rebuild",
            ).when(
                (col("ta.status") == "success") & col("fc.actual_frames").isNull(),
                "pending_silver_rebuild",
            ).when(
                col("ta.status") == "http_404", "not_tracked",
            ).when(
                col("ta.status").isNotNull(), "fetch_failed",
            ).otherwise("not_attempted").alias("tracking_status"),
            col("fc.actual_frames").alias("frame_count"),
            col("ta.attempted_at"),
            col("ta.status").alias("fetch_status"),
            col("ta.http_code"),
            col("ta.error_message"),
            current_timestamp().alias("ingested_at"),
        )
    )


def main() -> None:
    spark = get_spark("gold-goal-tracking-status")

    plays    = spark.read.table("nhl.silver.plays")
    attempts = spark.read.table("nhl.silver.tracking_attempts")
    frames   = spark.read.table("nhl.silver.tracking_frames")
    sequences = spark.read.table("nhl.gold.goal_tracking_sequences")

    out = transform_goal_tracking_status(plays, attempts, frames, sequences)

    # Small table (one row per NHL goal across all seasons, ~8k/season).
    # No partitioning; coalesce(1) so reads don't open many tiny files.
    out.coalesce(1).writeTo("nhl.gold.goal_tracking_status").createOrReplace()

    written = spark.read.table("nhl.gold.goal_tracking_status").count()
    print(f"gold-goal-tracking-status: complete (rows={written})")


if __name__ == "__main__":
    main()
