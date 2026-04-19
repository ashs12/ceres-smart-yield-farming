from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os


DATA_PATH = os.path.join(os.getcwd(), "data", "gold", "farm_current_status")

builder = SparkSession.builder \
    .appName("DataCheck") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load(DATA_PATH)

print(f"Total rows: {df.count()}")
print("-" * 30)
print("Unique entries in dataset (Label & Region):")
df.select("label", "region").distinct().show(truncate=False)

print("Unique entries in dataset (Lat & Lon):")
df.select("lat", "lon").distinct().show(truncate=False)