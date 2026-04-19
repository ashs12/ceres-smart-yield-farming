from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = SparkSession.builder \
    .appName("InspectGold") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
try:
    df_gold = spark.read.format("delta").load("data/gold/farm_current_status")
    
    print("\n--- REGIONS FOUND IN GOLD LAYER ---")
    df_gold.select("region").distinct().show(truncate=False)

    print("\n--- COUNT PER REGION ---")
    df_gold.groupBy("region").count().show()

except Exception as e:
    print(f"Error reading Delta table: {e}")

spark.stop()
df_gold = spark.read.format("delta").load("data/gold/farm_current_status")

print("--- REGIONS FOUND IN GOLD LAYER ---")
df_gold.select("region").distinct().show()

print("--- COUNT PER REGION ---")
df_gold.groupBy("region").count().show()