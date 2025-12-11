def grant_gold_permissions(spark):

    spark.sql("GRANT USAGE ON SCHEMA retail_gold TO `grupo_analistas`;")
    spark.sql("GRANT SELECT ON ALL TABLES IN SCHEMA retail_gold TO `grupo_analistas`;")

if __name__ == "__main__":
    grant_gold_permissions(spark)
