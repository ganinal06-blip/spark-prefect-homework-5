from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    upper,
    trim,
    lower
)

INPUT_PATH = "hdfs:///user/team/hw4_input/customers.csv"
OUTPUT_PATH = "hdfs:///user/team/hw5_output/customer_stats"

DATABASE_NAME = "team12_hw5"
TABLE_NAME = "customer_stats"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"

spark = (
    SparkSession.builder
    .appName("team12_hw5_data_pipeline")
    .enableHiveSupport()
    .getOrCreate()
)

try:
    print("STEP 1: Reading data from HDFS")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )

    required_columns = {"id", "city", "status"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    print("STEP 2: Transforming data")

    df_clean = (
        df
        .withColumn("id", col("id").cast("int"))
        .withColumn("city_clean", upper(trim(col("city"))))
        .withColumn("status", lower(trim(col("status"))))
        .filter(col("status").isNotNull())
    )

    print("STEP 3: Aggregating data")

    df_result = (
        df_clean
        .groupBy("status")
        .agg(
            count("*").alias("customer_count"),
            countDistinct("city_clean").alias("unique_cities")
        )
        .orderBy("status")
    )

    print("Aggregation result")
    df_result.show(truncate=False)

    print("STEP 4: Saving Parquet and registering table")

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"
    )

    spark.sql(
        f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}"
    )

    (
        df_result.write
        .mode("overwrite")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .saveAsTable(FULL_TABLE_NAME)
    )

    print("STEP 5: Checking saved table")

    spark.sql(
        f"SELECT * FROM {FULL_TABLE_NAME}"
    ).show(truncate=False)

    print("SUCCESS: Spark pipeline completed")

finally:
    spark.stop()
