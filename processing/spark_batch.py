
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    year,
    month,
    sum as spark_sum,
    count,
    avg,
    desc
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# Project paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_FILE = os.path.join(
    BASE_DIR, "data", "raw", "orders.jsonl"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR, "data", "processed"
)

ORDERS_OUTPUT = os.path.join(OUTPUT_DIR, "orders")
SUMMARY_OUTPUT = os.path.join(OUTPUT_DIR, "product_summary")


def main():

    print("=" * 55)
    print("SUPPLY CHAIN - SPARK BATCH PROCESSING")
    print("=" * 55)

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}. "
            "Run the Kafka producer and consumer first."
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize Spark
    spark = (
        SparkSession.builder
        .appName("SupplyChainIntelligence")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Define input schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])

    print("\nReading raw order events...")

    raw_df = (
        spark.read
        .schema(schema)
        .json(INPUT_FILE)
    )

    print("Raw records:", raw_df.count())

    # Clean and transform
    clean_df = (
        raw_df
        .dropDuplicates(["event_id"])
        .withColumn(
            "product_name",
            col("product_name")
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"))
        )
        .withColumn(
            "order_date",
            to_date(col("event_timestamp"))
        )
        .withColumn(
            "order_year",
            year(col("event_timestamp"))
        )
        .withColumn(
            "order_month",
            month(col("event_timestamp"))
        )
        .filter(col("event_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .filter(col("total_amount") > 0)
        .filter(col("event_timestamp").isNotNull())
        .filter(
            col("total_amount") ==
            col("quantity") * col("unit_price")
        )
    )

    print("Valid records:", clean_df.count())

    # Select final columns
    final_df = clean_df.select(
        "event_id",
        "order_id",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "total_amount",
        "order_status",
        "event_timestamp",
        "order_date",
        "order_year",
        "order_month"
    )

    print("\nProcessed order data:")

    final_df.show(10, truncate=False)

    # Save processed orders as Parquet
    final_df.write.mode("overwrite").parquet(
        ORDERS_OUTPUT
    )

    print("Processed orders saved:", ORDERS_OUTPUT)

    # Product-level analytics
    product_summary = (
        final_df
        .groupBy("product_id", "product_name")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("average_order_value")
        )
        .orderBy(desc("total_revenue"))
    )

    print("\nPRODUCT PERFORMANCE SUMMARY")

    product_summary.show(truncate=False)

    # Save analytics as Parquet
    product_summary.write.mode("overwrite").parquet(
        SUMMARY_OUTPUT
    )

    # Overall business metrics
    print("\nOVERALL BUSINESS METRICS")

    final_df.agg(
        count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("units_sold"),
        spark_sum("total_amount").alias("total_revenue")
    ).show(truncate=False)

    print("\nSPARK PROCESSING COMPLETED SUCCESSFULLY")

    spark.stop()


if __name__ == "__main__":
    main()
