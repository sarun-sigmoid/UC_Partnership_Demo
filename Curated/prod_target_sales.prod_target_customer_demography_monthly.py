# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW prod_target_sales.prod_target_customer_demography_monthly AS
# MAGIC SELECT
# MAGIC     c.country_code,
# MAGIC     c.gender,
# MAGIC     COUNT(DISTINCT o.customer_id) AS customer_count,
# MAGIC     SUM(o.total) AS total_sales
# MAGIC FROM
# MAGIC     prod_stg_sales.prod_stg_sales_fact_order o
# MAGIC JOIN
# MAGIC     prod_stg_sales.prod_stg_sales_dim_customer c ON o.customer_id = c.customer_id
# MAGIC WHERE
# MAGIC     o.order_timestamp IS NOT NULL
# MAGIC GROUP BY
# MAGIC     c.country_code,
# MAGIC     c.gender,
# MAGIC     o.order_year,
# MAGIC     o.order_month

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prod_target_sales.prod_target_customer_demography_monthly order by country_code
