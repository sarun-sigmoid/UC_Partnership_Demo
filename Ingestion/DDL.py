# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if  exists prod_land_sales cascade;
# MAGIC drop database if  exists prod_stg_sales cascade;
# MAGIC drop database if  exists prod_target_sales cascade;

# COMMAND ----------

# DBTITLE 1,Creating schemas
# MAGIC %sql
# MAGIC create database if not exists prod_land_sales;
# MAGIC create database if not exists prod_stg_sales;
# MAGIC create database if not exists prod_target_sales;

# COMMAND ----------

# DBTITLE 1,Landing Managed table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS prod_land_sales.prod_land_sales_fact_order (
# MAGIC     order_id STRING,
# MAGIC     order_timestamp TIMESTAMP,
# MAGIC     customer_id STRING,
# MAGIC     sku STRING,
# MAGIC     quantity BIGINT,
# MAGIC     total BIGINT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Landing Parquet External table
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS prod_land_sales.prod_land_sales_dim_product (
# MAGIC     sku STRING,
# MAGIC     name STRING,
# MAGIC     price DOUBLE,
# MAGIC     category STRING,
# MAGIC     updated TIMESTAMP
# MAGIC )
# MAGIC using DELTA
# MAGIC LOCATION 'abfss://partnershipdata-landing@saadlspartnershipdevst01.dfs.core.windows.net/landing/prod_land_sales_dim_product/';

# COMMAND ----------

# DBTITLE 1,Landing External table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS prod_land_sales.prod_land_sales_dim_customer (
# MAGIC     customer_id STRING,
# MAGIC     email STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     gender STRING,
# MAGIC     street STRING,
# MAGIC     city STRING,
# MAGIC     country_code STRING,
# MAGIC     row_status STRING,
# MAGIC     row_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://partnershipdata-landing@saadlspartnershipdevst01.dfs.core.windows.net/landing/prod_land_sales_dim_customer/';

# COMMAND ----------

# DBTITLE 1,Staging Managed Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS prod_stg_sales.prod_stg_sales_fact_order (
# MAGIC   order_id STRING, 
# MAGIC order_timestamp Timestamp,
# MAGIC Order_year STRING,
# MAGIC Order_month STRING,
# MAGIC Order_date STRING, 
# MAGIC customer_id STRING, 
# MAGIC sku STRING, 
# MAGIC quantity BIGINT, 
# MAGIC total BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Order_year, Order_month, Order_date)

# COMMAND ----------

# DBTITLE 1,Staging External table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS prod_stg_sales.prod_stg_sales_dim_customer (
# MAGIC     customer_id STRING,
# MAGIC     email STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     gender STRING,
# MAGIC     street STRING,
# MAGIC     city STRING,
# MAGIC     country_code STRING,
# MAGIC     row_status STRING,
# MAGIC     row_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://partnershipdata-stage@saadlspartnershipdevst01.dfs.core.windows.net/stage/prod_stg_sales_dim_customer/';

# COMMAND ----------

# DBTITLE 1,Staging parquet External Table
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS prod_stg_sales.prod_stg_sales_dim_product (
# MAGIC     sku STRING,
# MAGIC     name STRING,
# MAGIC     price DOUBLE,
# MAGIC     category STRING,
# MAGIC     updated TIMESTAMP
# MAGIC )
# MAGIC using DELTA
# MAGIC LOCATION 'abfss://partnershipdata-stage@saadlspartnershipdevst01.dfs.core.windows.net/stage/prod_stg_sales_dim_product/';
