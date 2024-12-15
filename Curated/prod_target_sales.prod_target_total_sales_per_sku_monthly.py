# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW prod_target_sales.prod_target_total_sales_per_sku_monthly AS
# MAGIC SELECT
# MAGIC     p.sku,
# MAGIC     p.name,
# MAGIC     p.category,
# MAGIC     o.order_year,
# MAGIC     o.order_month,
# MAGIC     SUM(o.quantity) AS quantity,
# MAGIC     c.country_code,
# MAGIC     SUM(o.quantity * p.price) AS total_sales
# MAGIC FROM
# MAGIC     prod_stg_sales.prod_stg_sales_fact_order o
# MAGIC JOIN
# MAGIC     prod_stg_sales.prod_stg_sales_dim_product p ON o.sku = p.sku
# MAGIC JOIN
# MAGIC     prod_stg_sales.prod_stg_sales_dim_customer c ON o.customer_id = c.customer_id
# MAGIC GROUP BY
# MAGIC     p.sku,
# MAGIC     p.name,
# MAGIC     p.category,
# MAGIC     o.order_year,
# MAGIC     o.order_month
# MAGIC     ,
# MAGIC     c.country_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prod_target_sales.prod_target_total_sales_per_sku_monthly

# COMMAND ----------


