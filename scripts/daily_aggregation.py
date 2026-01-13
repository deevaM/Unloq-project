from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, count, countDistinct, coalesce, lit


def daily_aggregate(events_df, users_df, countries_df):
    users_enriched = (
        users_df
        .join(countries_df, on="country_code", how="left")
        .withColumn(
            "country",
            coalesce("country_name", lit("UNKNOWN"))
        )
        .select("user_id", "country")
    )

    enriched_events = (
        events_df
        .join(users_enriched, on="user_id", how="left")
        .withColumn(
            "country",
            coalesce("country", lit("UNKNOWN"))
        )
    )

    daily_df = (
        enriched_events
        .withColumn("date", to_date("event_ts"))
        .groupBy("date", "country")
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_active_users")
        )
    )

    return daily_df


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("DailyAggregationJob")
        .master("local[*]")
        .getOrCreate()
    )

    # Read input data
    events_df = spark.read.parquet("processed/events")
    users_df = spark.read.parquet("processed/users")

    countries_df = (
        spark.read
        .option("header", "true")
        .csv("data/countries.csv")
    )

    # Run aggregation
    daily_df = daily_aggregate(events_df, users_df, countries_df)

    # Write output
    (
        daily_df
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet("output/daily_aggregation")
    )

    spark.stop()
