# Databricks notebook source
from pyspark.sql import SparkSession
import re

# Initialize Spark session
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Define source and destination schema/catalog names
src_view_schema = "prod_target_sales"                          # Source schema for views
src_table_schema = "prod_stg_sales"                            # Source schema for tables referenced in views
dst_catalog = "uc_adb_partnership_demo"                        # Destination catalog name in Unity Catalog
dst_table_schema = f"{dst_catalog}.uc_prod_stg_sales"          # Target table schema with catalog
dst_view_schema = f"{dst_catalog}.uc_prod_target_sales"        # Target view schema with catalog

# Step 1: Create the target view schema in Unity Catalog if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dst_view_schema}")

# Step 2: Get all views from the source schema
views_df = spark.sql(f"SHOW VIEWS IN {src_view_schema}").collect()

# Step 3: Process each view
for view in views_df:
    view_name = view["viewName"]  # Extract view name
    
    # Fetch the CREATE VIEW SQL for the current view
    view_definition = spark.sql(f"SHOW CREATE TABLE {src_view_schema}.{view_name}").collect()[0][0]
    
    # Replace references to the source table schema with the target table schema (including catalog)
    updated_view_definition = re.sub(
        rf"\b{src_table_schema}\b", dst_table_schema, view_definition
    )
    
    # Replace the schema in the CREATE VIEW statement to point to the target view schema
    updated_view_definition = re.sub(
        rf"CREATE VIEW {src_view_schema}\.", f"CREATE VIEW {dst_view_schema}.", updated_view_definition
    )

    # Print the updated SQL for debugging (optional)
    print(f"Recreating view: {view_name} in {dst_view_schema}")
    print(f"Updated SQL:\n{updated_view_definition}\n")
    
    # Execute the updated CREATE VIEW command
    try:
        spark.sql(updated_view_definition)
        print(f"Successfully migrated view: {view_name}")
    except Exception as e:
        print(f"Failed to migrate view {view_name}: {str(e)}")
    
print("View migration completed successfully.")

