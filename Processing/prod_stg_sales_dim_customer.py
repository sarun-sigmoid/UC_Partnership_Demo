# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.saadlspartnershipdevst01.dfs.core.windows.net",
    "BK+97X2QUkWmZ8fHSbEmh2iY4RZcZCZ0y8R6vH1+5+0bjuHPoVVwo7IDNebAPdH6Tql0rzxv6PMD+AStwAkgYQ=="
)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, when,udf
from pyspark.sql.types import StringType

# COMMAND ----------

customer_df=spark.table('prod_land_sales.Prod_land_sales_dim_customer')

# COMMAND ----------



def validate_email(email):
    if "@" in email:
        return email
    else:
        return "Invalid Email"


validate_email_udf = udf(validate_email, StringType())
customer_df = customer_df.withColumn("validated_email", validate_email_udf("email"))

#remove email column
customer_df = customer_df.drop("email")
customer_df=customer_df.withColumnRenamed("validated_email", "email")


# COMMAND ----------


customer_df = customer_df.fillna({
    "email": "unknown@example.com", 
    "first_name": "unknown_user",
    "last_name":"unknown_user"
})



# COMMAND ----------

customer_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("prod_stg_sales.prod_stg_sales_dim_customer")
