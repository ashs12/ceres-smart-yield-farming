from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

def get_spark():

    builder = SparkSession.builder \
        .appName("Debug") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[*]")
    return configure_spark_with_delta_pip(builder).getOrCreate()

spark = get_spark()

def check_layer(name, path):
    print(f"\n--- CHECKING {name} LAYER ---")
    if not os.path.exists(path):
        print(f"Path '{path}' does not exist!")
        return
    
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"Total records found: {count}")
        print("Crop distribution (top 20):")
        df.groupBy("label").count().orderBy("count", ascending=False).show(20, truncate=False)
    except Exception as e:
        print(f"Error reading {name} layer: {e}")

check_layer("BRONZE", "data/bronze/delta_agri_history")
check_layer("SILVER", "data/silver/enriched_agri_history")
check_layer("GOLD", "data/gold/farm_current_status")