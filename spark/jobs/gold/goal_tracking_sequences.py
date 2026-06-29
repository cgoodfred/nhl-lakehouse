"""Build gold.goal_tracking_sequences for low-latency animation playback.

Source:  nhl.silver.tracking_frames
Target:  nhl.gold.goal_tracking_sequences

One row per goal event with its ordered tracking frames nested into an array.
This is a serving-shaped table for the Streamlit animation panel: the app can
load one gold row for a selected goal instead of scanning many frame rows from
the normalized silver table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_list,
    count,
    current_timestamp,
    sort_array,
    struct,
)

from common import get_spark


def transform_goal_tracking_sequences(frames_df: DataFrame) -> DataFrame:
    """Collapse per-frame silver rows to one ordered sequence per goal."""
    frame_struct = struct(
        col("frame_index").alias("frame_index"),
        col("timestamp_ds").alias("timestamp_ds"),
        col("puck_x_in").alias("puck_x_in"),
        col("puck_y_in").alias("puck_y_in"),
        col("on_ice").alias("on_ice"),
    )

    return (
        frames_df
        .groupBy("season", "game_id", "event_id")
        .agg(
            count("*").cast("int").alias("frame_count"),
            sort_array(collect_list(frame_struct)).alias("frames"),
        )
        .select(
            col("game_id"),
            col("event_id"),
            col("season"),
            col("frame_count"),
            col("frames"),
            current_timestamp().alias("ingested_at"),
        )
    )


def main() -> None:
    spark = get_spark("gold-goal-tracking-sequences")

    frames = spark.read.table("nhl.silver.tracking_frames")
    out = transform_goal_tracking_sequences(frames)

    out.writeTo("nhl.gold.goal_tracking_sequences") \
        .partitionedBy(col("season")).createOrReplace()

    written = spark.read.table("nhl.gold.goal_tracking_sequences").count()
    print(f"gold-goal-tracking-sequences: complete (rows={written})")


if __name__ == "__main__":
    main()
