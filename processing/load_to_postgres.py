import os
from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed", "orders")

spark = (
    SparkSession.builder
    .appName("SupplyChainLoadToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

connection = None

try:
    df = spark.read.parquet(PARQUET_PATH)

    columns = [
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
        "order_month",
    ]

    df = df.select(*columns)

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "supplychain"),
        user=os.getenv("POSTGRES_USER", "supplyadmin"),
        password=os.getenv(
            "POSTGRES_PASSWORD",
            "change_this_local_password"
        ),
    )

    insert_sql = """
        INSERT INTO processed_orders (
            event_id, order_id, product_id, product_name,
            quantity, unit_price, total_amount, order_status,
            event_timestamp, order_date, order_year, order_month
        )
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            order_status = EXCLUDED.order_status,
            total_amount = EXCLUDED.total_amount;
    """

    batch = []
    total_loaded = 0

    with connection.cursor() as cursor:
        for row in df.toLocalIterator():
            batch.append(tuple(row[column] for column in columns))

            if len(batch) >= 500:
                execute_values(cursor, insert_sql, batch)
                connection.commit()
                total_loaded += len(batch)
                batch = []

        if batch:
            execute_values(cursor, insert_sql, batch)
            connection.commit()
            total_loaded += len(batch)

    print(f"Successfully loaded {total_loaded} records into PostgreSQL.")

except Exception as error:
    if connection:
        connection.rollback()
    print("PostgreSQL loading failed:", error)
    raise

finally:
    if connection:
        connection.close()

    spark.stop()