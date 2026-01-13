from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import StructType
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

    # Cleaning logic for batch + stream
    return (
        df.withColumn("event_ts", to_timestamp("event_timestamp"))
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

    batch_df.write.mode("append").partitionBy("event_date").parquet(OUTPUT_PATH)


def process_stream_events(spark, stream_path, users_df):

    stream_df = (
        spark.readStream
        .option("maxFilesPerTrigger", 1)
        .json(stream_path)
    )

    stream_df = clean_events(stream_df)

    stream_df = (
        stream_df
        .withWatermark("event_ts", "2 days")
        .join(users_df, on="user_id", how="left")
    )

    def write_microbatch(microbatch_df, batch_id):
        if microbatch_df.rdd.isEmpty():
            return

        existing = read_existing_events(spark)
        if existing:
            microbatch_df = microbatch_df.join(
                existing, on="event_id", how="left_anti"
            )

        microbatch_df.write.mode("append").partitionBy("event_date").parquet(OUTPUT_PATH)

    query = (
        stream_df.writeStream
        .foreachBatch(write_microbatch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


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
        "landing/events.csv",
        users_df
    )

    process_stream_events(
        spark,
        "landing/events_stream.json",
        users_df
    )
