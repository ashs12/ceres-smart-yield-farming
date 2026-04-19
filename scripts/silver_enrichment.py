import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder.appName("Ceres_Silver_Enrichment") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[*]") 
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_bronze_data(spark):
    print("Loading Bronze data...")
    return spark.read.format("delta").load("data/bronze/delta_agri_history")

def clean_and_enrich(spark, df_bronze):

    df = df_bronze.withColumnRenamed("N", "n") \
                  .withColumnRenamed("P", "p") \
                  .withColumnRenamed("K", "k")
    
    df_cleaned = df.na.drop(subset=["ph", "n", "p", "k"]) \
                   .filter((F.col("ph") >= 0) & (F.col("ph") <= 14))

    df_silver = df_cleaned.withColumn("region", F.lit("India")) \
                          .withColumn("lat", F.lit(20.59) + (F.rand() * 4.0 - 2.0)) \
                          .withColumn("lon", F.lit(78.96) + (F.rand() * 4.0 - 2.0)) \
                          .withColumn("label", F.trim(F.lower(F.col("label")))) \
                          .withColumn("date", F.to_date("timestamp"))

    return df_silver
def main():
    spark = create_spark_session()
    df_bronze = load_bronze_data(spark)
    

    df_silver = clean_and_enrich(spark, df_bronze)

    output_path = "data/silver/enriched_agri_history"
    df_silver.write.format("delta").mode("overwrite").partitionBy("region").save(output_path)
    
    print(f"Silver Layer processing complete! Processed {df_silver.count()} records.")

if __name__ == "__main__":
    main()