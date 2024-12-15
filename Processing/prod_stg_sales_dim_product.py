# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

product_df=spark.read.table("prod_land_sales.prod_land_sales_dim_product")

# COMMAND ----------

product_df = product_df.filter(
    col("sku").isNotNull() & col("name").isNotNull()
).dropDuplicates(["sku", "name"])

# COMMAND ----------

product_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("prod_stg_sales.prod_stg_sales_dim_product")
