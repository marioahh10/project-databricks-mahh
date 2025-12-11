def revoke_gold_permissions(spark):
 
    spark.sql("REVOKE SELECT ON ALL TABLES IN SCHEMA retail_gold FROM `grupo_analistas`;")
    spark.sql("REVOKE USAGE ON SCHEMA retail_gold FROM `grupo_analistas`;")

if __name__ == "__main__":
    revoke_gold_permissions(spark)
