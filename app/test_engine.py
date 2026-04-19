from engine import get_live_weather, build_rag_context, get_ai_advice
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os



PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "farm_current_status")

builder = SparkSession.builder \
    .appName("TestEngine") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


exists = os.path.exists(DATA_PATH)

if exists:
    print(f"DEBUG: Contents of folder: {os.listdir(DATA_PATH)}")
else:
    parent = os.path.dirname(DATA_PATH)


df_gold = spark.read.format("delta").load(DATA_PATH)

farm = df_gold.limit(1).collect()[0]
api_key = os.getenv("OPENWEATHER_API_KEY")

weather = get_live_weather(farm['lat'], farm['lon'], api_key)

context = build_rag_context(farm['farm_id'], df_gold, weather)

advice = get_ai_advice(context)

print("AI ADVICE")
print(advice)