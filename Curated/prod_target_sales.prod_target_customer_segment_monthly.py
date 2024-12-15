# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW prod_target_sales.prod_target_customer_segment_monthly AS
# MAGIC SELECT
# MAGIC     o.customer_id,
# MAGIC     c.gender,
# MAGIC     o.order_year,
# MAGIC    o.Order_month AS month,
# MAGIC     COUNT(o.order_id) AS order_count,
# MAGIC     SUM(o.total) AS total_spent
# MAGIC FROM
# MAGIC     prod_stg_sales.prod_stg_sales_fact_order o
# MAGIC JOIN
# MAGIC     prod_stg_sales.prod_stg_sales_dim_customer c ON o.customer_id = c.customer_id
# MAGIC WHERE
# MAGIC     o.order_timestamp IS NOT NULL
# MAGIC GROUP BY
# MAGIC     o.customer_id,
# MAGIC     c.gender,
# MAGIC     o.Order_year,
# MAGIC    o.Order_month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prod_target_sales.prod_target_customer_segment_monthly
