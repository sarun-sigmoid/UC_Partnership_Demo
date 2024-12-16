from demoFunction import *

catalogName = "partnership-dev"
tableName   = "customer_copilot"
dbName      = "github-copilot"
columnName  = "email"
columnValue = "jane.smith@example.com"

# If the table exists in the specified database...
if tableExists(catalogName, tableName, dbName):

  df = spark.sql(f"SELECT * FROM {catalogName}.{dbName}.{tableName}")

  # And the specified column exists in that table...
  if columnExists(df, columnName):
    # Then report the number of rows for the specified value in that column.
    numRows = numRowsInColumnForValue(df, columnName, columnValue)

    print(f"There are {numRows} rows in '{tableName}' where '{columnName}' equals '{columnValue}'.")
  else:
    print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
else:
  print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 
