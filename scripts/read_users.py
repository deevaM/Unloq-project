from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window


def read_and_process_users(spark: SparkSession):
    users_df = (
        spark.read
        .option("mode", "PERMISSIVE")
        .json("data/users.json")
    )

    # Handle missing country codes
    users_df = users_df.withColumn(
        "country_code",
        when(col("country_code").isNull(), "UNKNOWN")
        .otherwise(col("country_code"))
    )

    # Latest record per user (SCD-1)
    window = Window.partitionBy("user_id").orderBy(col("created_at").desc())

    users_latest = (
        users_df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Write cleaned data to /processed/users.parquet
    users_latest.write.mode("overwrite").parquet("processed/users")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadUsers").getOrCreate()
    read_and_process_users(spark)
    