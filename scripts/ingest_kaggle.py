import os 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark_session():
    builder = SparkSession.builder.appName("Ceres_Bronze_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "8")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def ingest_raw_data(spark):
    csv_path = "data/bronze/agri_data.csv"

    df_raw  = spark.read.option("header", "true")\
    .option("inferSchema", "true").csv(csv_path)

    return df_raw


def multiply_data(df_raw):
    spark = df_raw.sparkSession
    multiplier_df = spark.range(0,500).withColumnRenamed("id", "multiplier_id")
    return df_raw.crossJoin(multiplier_df)



def add_realism(df_boosted):
    df_with_id = df_boosted.withColumn("farm_id", F.concat(F.lit("FARM_"), F.col("multiplier_id")))

    df_realistic = df_with_id.withColumn(
        "temperature", 
        F.round(F.col("temperature") + (F.rand() * 2 - 1), 2)
    ).withColumn(
        "humidity", 
        F.round(F.col("humidity") + (F.rand() * 5 - 2.5), 2)
    )
    df_final = df_realistic.withColumn(
        "timestamp", 
        F.expr("current_timestamp() - make_interval(0,0,0,0,0, multiplier_id, 0)")
    )

    return df_final


def save_to_bronze(df_final):

    output_path = "data/bronze/delta_agri_history"
    (df_final.write.format("delta").mode("overwrite").partitionBy("label")\
     .save(output_path))
    

def main():
    spark = create_spark_session()
    df_raw = ingest_raw_data(spark)
    df_boosted = multiply_data(df_raw)
    df_final = add_realism(df_boosted)
    save_to_bronze(df_final)


if __name__ == "__main__":
    main()

