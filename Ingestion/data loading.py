# Databricks notebook source
spark.conf.set("fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net", "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ==")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net", "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ==")

# COMMAND ----------

# DBTITLE 1,Order Table Load
order_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f'abfss://devdatapartnership-raw@saadlspartnershipdevst01.dfs.core.windows.net/raw/orders/year=2024/month=12/date=10/order.csv')



order_df = order_df.withColumn("order_id", order_df["order_id"].cast("STRING")) \
                   .withColumn("order_timestamp", order_df["order_timestamp"].cast("TIMESTAMP")) \
                       .withColumn("customer_id", order_df["customer_id"].cast("STRING")) \
                   .withColumn("quantity", order_df["quantity"].cast("BIGINT")) \
                   .withColumn("total", order_df["total"].cast("BIGINT")) \
                   .withColumn("sku", order_df["sku"].cast("STRING"))

order_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prod_land_sales.prod_land_sales_fact_order")

# COMMAND ----------

# DBTITLE 1,Customer Table Load
customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f'abfss://devdatapartnership-raw@saadlspartnershipdevst01.dfs.core.windows.net/raw/customer/year=2024/month=12/date=10/customer.csv')

customer_df = customer_df.withColumn("customer_id", customer_df["customer_id"].cast("STRING"))

customer_df.write.format("delta").option("mergeSchema", "true").saveAsTable("prod_land_sales.prod_land_sales_dim_customer", mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Product Table Load
product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f'abfss://devdatapartnership-raw@saadlspartnershipdevst01.dfs.core.windows.net/raw/product/year=2024/month=12/date=10/product.csv')


product_df=product_df.withColumn("price", product_df["price"].cast("double"))\
     .withColumn("updated", product_df["updated"].cast("Timestamp"))\
     .withColumn("sku", product_df["sku"].cast("string"))


product_df.write.format("delta").option("mergeSchema", "true").saveAsTable("prod_land_sales.prod_land_sales_dim_product", mode='overwrite')
