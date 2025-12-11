from pyspark.sql import functions as F

RAW_ECOMMERCE_PATH = (
    "abfss://raw@datalakeproject.dfs.core.windows.net/"
    "retail/ecommerce/"
)
BRONZE_ECOMMERCE_TABLE = "retail_bronze.ecommerce_raw"


def load_ecommerce_bronze(spark):
    df = (
        spark.read
             .parquet(RAW_ECOMMERCE_PATH)
    )

    df = df.withColumn("bronze_load_ts", F.current_timestamp())

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(BRONZE_ECOMMERCE_TABLE)
    )


if __name__ == "__main__":
    load_ecommerce_bronze(spark)
