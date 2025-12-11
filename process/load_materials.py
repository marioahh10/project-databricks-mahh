from pyspark.sql import functions as F

RAW_MATERIALS_PATH = (
    "abfss://raw@datalakeproject.dfs.core.windows.net/"
    "retail/materials/"
)
BRONZE_MATERIALS_TABLE = "retail_bronze.materials_raw"


def load_materials_bronze(spark):
    df_raw = (
        spark.read
             .parquet(RAW_MATERIALS_PATH)
    )

    col_name = df_raw.columns[0]

    df_split = df_raw.withColumn(
        "split_values",
        F.split(F.col(col_name), ";")
    ).select(
        F.col("split_values").getItem(0).alias("metal"),
        F.col("split_values").getItem(1).alias("symbol"),
        F.col("split_values").getItem(2).alias("youngs_modulus_gpa"),
        F.current_timestamp().alias("bronze_load_ts")
    )

    (df_split.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_MATERIALS_TABLE)
    )


if __name__ == "__main__":
    load_materials_bronze(spark)
