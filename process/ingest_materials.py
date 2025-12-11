from pyspark.sql import functions as F

KAGGLE_MATERIALS_CSV = (
    "abfss://landing@datalakeproject.dfs.core.windows.net/"
    "kaggle/youngs_modulus.csv"
)

RAW_MATERIALS_PATH = (
    "abfss://raw@datalakeproject.dfs.core.windows.net/"
    "retail/materials/"
)


def ingest_materials_to_raw(spark):
    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(KAGGLE_MATERIALS_CSV)
    )

    df = df.withColumn("raw_ingestion_ts", F.current_timestamp())

    (df.write
       .mode("overwrite")
       .parquet(RAW_MATERIALS_PATH)
    )


if __name__ == "__main__":
    ingest_materials_to_raw(spark)
