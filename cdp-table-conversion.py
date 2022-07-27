# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Repos/CDP/UberStream/cdp-conversion-library

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
    for database_row in spark.sql(f"SHOW TABLES FROM {row[0]} LIKE '^((?!usermind).)*([0-9]){{3,20}}$'").collect():
        try:
          org_id, entity_table = database_row[0], database_row[1]
          print(f"Processing {org_id}.{entity_table}")
          if is_table_databricks_managed(org_id, entity_table):
            print("Table {org_id}.{entity_table} is already databricks managed. Skipping processing.")
            continue
          print(f"Processing org {org_id} and table {entity_table}")
          replace_table_with_databricks_managed_table(org_id, entity_table, file_list)
          delete_files(file_list)
        except Exception as ex:
          print(f"Encountered exception while processing {org_id}.{enttity_table}: {ex}", org_id, entity_table, ex)
          write_failure(org_id, entity_table, str(ex))
          
