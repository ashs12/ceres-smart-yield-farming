import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window  
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder.appName("Ceres_Gold_Analytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[*]") 
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    spark = create_spark_session()

    silver_path = "data/silver/enriched_agri_history"
    df_silver = spark.read.format("delta").load(silver_path)

    print("Generating Insights for India...")


    df_daily = df_silver.withColumn("date", F.to_date("timestamp")) \
        .groupBy("farm_id", "region", "label", "date") \
        .agg(
            F.avg("temperature").alias("avg_temp"), 
            F.avg("humidity").alias("avg_humidity"),
            F.avg("ph").alias("avg_ph"),
            F.avg("n").alias("avg_n"),       
            F.avg("p").alias("avg_p"),       
            F.avg("k").alias("avg_k"),       
            F.max("lat").alias("lat"),
            F.max("lon").alias("lon")
        )

    window_spec = Window.partitionBy("farm_id", "label").orderBy(F.col("timestamp").desc())
    
    df_final_status = df_silver.withColumn("row_num", F.row_number().over(window_spec)) \
                               .filter(F.col("row_num") == 1) \
                               .withColumn("date", F.to_date("timestamp")) \
                               .drop("row_num")

    gold_path = "data/gold/farm_daily_stats"
    gold_status_path = "data/gold/farm_current_status" 

    
    df_daily.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_path)
    
    
    df_final_status.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_status_path)

    print(f"Gold Layer Created successfully.")
    print(f"Daily Stats records: {df_daily.count()}")
    print(f"Current Farm Status records: {df_final_status.count()}")

if __name__ == "__main__":
    main()