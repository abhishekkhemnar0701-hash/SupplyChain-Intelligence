
import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ORDERS_PATH = os.path.join(
    BASE_DIR, "data", "processed", "orders"
)

spark = (
    SparkSession.builder
    .appName("VerifySupplyChainData")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(ORDERS_PATH)

print("TOTAL PROCESSED RECORDS:", df.count())

print("\nPARQUET DATA:")
df.show(10, truncate=False)

print("\nDATA SCHEMA:")
df.printSchema()

spark.stop()
