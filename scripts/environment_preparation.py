def create_schemas(spark):
    """
    Crea los schemas necesarios para la arquitectura Medallion.
    """
    spark.sql("CREATE SCHEMA IF NOT EXISTS retail_bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retail_silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS retail_gold")


if __name__ == "__main__":
    create_schemas(spark)
