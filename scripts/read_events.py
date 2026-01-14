from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, expr
from pyspark.sql.types import StructType, StructField, StringType
import os

OUTPUT_PATH = "processed/events"
CHECKPOINT_PATH = "checkpoints/events_stream"


def read_existing_events(spark):

    # Reads already processed events if present.

    if os.path.exists(OUTPUT_PATH):
        return spark.read.parquet(OUTPUT_PATH).select("event_id")
    else:
        return None


def clean_events(df):
    # Cleans event data: parses timestamps, removes invalid records, drop duplicates.

    return (
        df
        .withColumn(
            "event_ts",
            expr("try_cast(event_timestamp as timestamp)")
        )
        .filter(col("event_ts").isNotNull())
        .withColumn("event_date", to_date("event_ts"))
        .dropDuplicates(["event_id"])
    )


def process_batch_events(spark, events_path, users_df):

    batch_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(events_path)
    )

    batch_df = clean_events(batch_df)

    # Join users (unknown users retained, marked later)
    batch_df = batch_df.join(users_df, on="user_id", how="left")

    # Cross-source deduplication
    existing = read_existing_events(spark)
    if existing:
        batch_df = batch_df.join(existing, on="event_id", how="left_anti")

    batch_df.write.mode("append").parquet(OUTPUT_PATH)


def process_stream_events(spark, stream_file_path, users_df):
    """
    Simulates streaming ingestion from a single JSON file using micro-batch processing.
    """

    # Read entire file as batch
    stream_df = (
        spark.read
        .schema(
            StructType([
                StructField("event_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_timestamp", StringType(), True),
            ])
        )
        .json(stream_file_path)
    )

    # Clean events
    stream_df = clean_events(stream_df)

    # Join users
    stream_df = stream_df.join(users_df, on="user_id", how="left")

    # Simulate micro-batches
    BATCH_SIZE = 1000
    total_rows = stream_df.count()

    for offset in range(0, total_rows, BATCH_SIZE):
        microbatch_df = stream_df.limit(BATCH_SIZE).offset(offset)

        if microbatch_df.rdd.isEmpty():
            continue

        existing = read_existing_events(spark)
        if existing is not None:
            microbatch_df = microbatch_df.join(
                existing, on="event_id", how="left_anti"
            )

        microbatch_df = microbatch_df.coalesce(2)

        microbatch_df.write.mode("append").parquet(OUTPUT_PATH)

        print(f"Processed stream micro-batch starting at offset {offset}")



if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("EventsProcessing")
        .master("local[*]")
        .getOrCreate()
    )

    # Users must already be processed (latest snapshot)
    users_df = (
        spark.read.parquet("processed/users")
        .select("user_id")
        .distinct()
    )

    process_batch_events(
        spark,
        "data/events.csv",
        users_df
    )

    process_stream_events(
        spark,
        "data/events_stream.json",
        users_df
    )
