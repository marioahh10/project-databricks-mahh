from pyspark.sql import functions as F

BRONZE_MATERIALS_TABLE = "retail_bronze.materials_raw"
SILVER_DIM_MATERIAL_TABLE = "retail_silver.dim_material"


def build_dim_material(spark):
    df = spark.table(BRONZE_MATERIALS_TABLE)

    df = (
        df
        .withColumn("youngs_modulus_gpa", F.col("youngs_modulus_gpa").cast("double"))
        .withColumn("material_id", F.monotonically_increasing_id())
    )

    dim_material = df.select(
        "material_id",
        "metal",
        "symbol",
        "youngs_modulus_gpa"
    )

    (dim_material.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_DIM_MATERIAL_TABLE)
    )


if __name__ == "__main__":
    build_dim_material(spark)
