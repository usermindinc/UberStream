# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Repos/CDP/UberStream/cdp-conversion-library

# COMMAND ----------

dbutils.widgets.text('parquet_path', 's3://acid-cdp-staging', 'Base location of parquet files')

input_path = dbutils.widgets.get('parquet_path')""
parquet_path = input_path[0:-1] if input_path.endswith('/') else input_path


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS usermind_reporting 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS usermind_reporting.failed_db_conversions (org_id STRING, entity_table STRING, exceptionMesage STRING)

# COMMAND ----------

def write_failure(org_id, entity_table, ex):
  spark.sql(f"INSERT INTO usermind_reporting.failed_db_conversions VALUES('{org_id}', '{entity_table}', '{str(ex)}')")

# COMMAND ----------


org_id = ""
entity_table = ""
# Iterates through all schemas that start with org_
for row in spark.sql("SHOW SCHEMAS LIKE 'org_*'").collect():
    # Shows tables in the schema not starting with usermind that ends in at least 3 numbers
    print(row)
    for database_row in spark.sql(f"SHOW TABLES FROM {row[0]} LIKE '^((?!usermind).)*([0-9]){{3,20}}$'").collect():
        try:
          org_id, entity_table = database_row[0], database_row[1]
          if is_table_databricks_managed(org_id, entity_table):
            print("Table {org_id}.{entity_table} is already databricks managed. Skipping processing.")
            continue
          print(f"Processing org {org_id} and table {entity_table}")
          replace_table_with_databricks_managed_table(org_id, entity_table, file_list)
          raise ex()
          delete_files(file_list)
        except Exception as ex:
          print("something", org_id, entity_table, ex)
          raise ex
          write_failure(org_id, entity_table, "fake ex")
          

# COMMAND ----------

spark.sql("SHOW TABLES FROM org_1089824 LIKE '^((?!usermind).)*([0-9])+$'").show()

# COMMAND ----------

rename_dbtemp_table("org_1012768", "csv_importer_test1_1045748")

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE 
