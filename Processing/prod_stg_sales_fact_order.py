# Databricks notebook source
# DBTITLE 1,Import functions
from pyspark.sql.functions import year, month, dayofmonth

# COMMAND ----------

order_df=spark.read.table('prod_land_sales.Prod_land_sales_fact_order')

# COMMAND ----------

# Removing duplicates
order_df=order_df.dropDuplicates()

# handling null values
order_df = order_df.fillna({
    "order_id": "Unknown",
    "order_timestamp": "1970-01-01 00:00:00",
    "customer_id": "Unknown",
    "sku": "Unknown"
})


# COMMAND ----------

order_df = order_df.withColumn("Order_year", year("order_timestamp").cast("STRING")) \
                   .withColumn("Order_month", month("order_timestamp").cast("STRING")) \
                   .withColumn("Order_date", dayofmonth("order_timestamp").cast("STRING"))

# COMMAND ----------


order_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("Order_year", "Order_month", "Order_date") \
    .saveAsTable("prod_stg_sales.prod_stg_sales_fact_order")
