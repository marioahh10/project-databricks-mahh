from pyspark.sql import functions as F

RAW_PATH = "abfss://raw@<storage_account>.dfs.core.windows.net/ecommerce/"
BRONZE_TABLE = "retail_bronze.ecommerce_orders"

def ingest_ecommerce_orders(spark):
    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(RAW_PATH)
    )

    df = df.withColumn("ingestion_ts", F.current_timestamp())
    df = df.withColumn("source_system", F.lit("kaggle_ecommerce"))

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(BRONZE_TABLE)
    )

if __name__ == "__main__":
    # En Databricks el objeto spark ya existe
    ingest_ecommerce_orders(spark)
