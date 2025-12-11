from pyspark.sql import functions as F

BRONZE_ECOMMERCE_TABLE = "retail_bronze.ecommerce_raw"
SILVER_FACT_SALES_TABLE = "retail_silver.fact_sales"


def build_fact_sales(spark):

    df = spark.table(BRONZE_ECOMMERCE_TABLE)

    # Renombrar columnas a nombres estándar
    df = (
        df
        .withColumnRenamed("Date", "order_date")
        .withColumnRenamed("Product_Category", "category")
        .withColumnRenamed("Price", "unit_price")
        .withColumnRenamed("Discount", "discount_percent")
        .withColumnRenamed("Customer_Segment", "customer_segment")
        .withColumnRenamed("Marketing_Spend", "marketing_spend")
        .withColumnRenamed("Units_Sold", "quantity")
    )

    # Tipos correctos
    df = (
        df
        .withColumn("order_date", F.to_date("order_date", "dd-MM-yyyy"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_percent", F.col("discount_percent").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("marketing_spend", F.col("marketing_spend").cast("double"))
    )

    # Canal fijo: ecommerce
    df = df.withColumn("channel", F.lit("E_COMMERCE"))

    # Monto de venta considerando descuento %
    df = df.withColumn(
        "sales_amount",
        F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_percent") / 100.0)
    )

    # Columnas finales
    fact_sales = df.select(
        "order_date",
        "category",
        "unit_price",
        "discount_percent",
        "quantity",
        "sales_amount",
        "customer_segment",
        "marketing_spend",
        "channel"
    )

    (fact_sales.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_FACT_SALES_TABLE)
    )


if __name__ == "__main__":
    build_fact_sales(spark)
