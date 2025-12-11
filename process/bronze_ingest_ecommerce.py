from pyspark.sql import functions as F


KAGGLE_ECOMMERCE_CSV = (
    "abfss://landing@datalakeproject.dfs.core.windows.net/"
    "kaggle/Ecommerce_Sales_Prediction_Dataset.csv"
)


RAW_ECOMMERCE_PATH = (
    "abfss://raw@datalakeproject.dfs.core.windows.net/"
    "retail/ecommerce/"
)


def ingest_ecommerce_to_raw(spark):
    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(KAGGLE_ECOMMERCE_CSV)
    )

    df = df.withColumn("raw_ingestion_ts", F.current_timestamp())

    (df.write
       .mode("overwrite")
       .parquet(RAW_ECOMMERCE_PATH)
    )


if __name__ == "__main__":
    ingest_ecommerce_to_raw(spark)
