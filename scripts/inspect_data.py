from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = SparkSession.builder.appName("Ceres_Inspector") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "data/bronze/delta_agri_history"
df = spark.read.format("delta").load(path)

print("\n" + "="*50)
print(f"TOTAL ROWS IN BRONZE: {df.count():,}")
print("="*50)
df.select("farm_id", "label", "temperature", "timestamp").show(10, truncate=False)
print("="*50 + "\n")