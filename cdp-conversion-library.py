# Databricks notebook source
dbutils.widgets.text('parquet_path', 's3://acid-cdp-staging', 'Base location of parquet files')

input_path = dbutils.widgets.get('parquet_path')
PARQUET_PATH = input_path[0:-1] if input_path.endswith('/') else input_path


# COMMAND ----------

def is_table_databricks_managed(org_id, entity_table):
  try:
    if spark.sql(f"DESCRIBE TABLE EXTENDED {org_id}.{entity_table}").where("col_name = 'Type' AND data_type = 'MANAGED'").count() > 0:
      print(f"Table {org_id}.{entity_table} is databricks managed")
      return True
    print(f"Table {org_id}.{entity_table} is NOT databricks managed")
    return False
  except AnalysisException as Ex:
    print(f"Table {org_id}.{entity_table} not found, creating table now")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {org_id}")
    print(f"Successfully database table {org_id}")
    file_list = get_all_files(org_id, entity_table)
    load_data(org_id, entity_table, file_list)
    return True

# Creates backup of existing table, loads all existing data into table with suffix _dbtemp, drops existing table, and then renames db_temp table to replace table
def replace_table_with_databricks_managed_table(org_id, entity_table):
  print(f"Starting replacement process for {org_id}.{entity_table}")
  spark.sql(f"CREATE TABLE {org_id}.{entity_table}_backup AS SELECT * FROM {org_id}.{entity_table}")
  print(f"Finished creating {org_id}.{entity_table}_backup")
  file_list = get_all_files(org_id, entity_table)
  load_data(org_id, entity_table + "_dbtemp", file_list)
  drop_table(org_id, entity_table)
  rename_dbtemp_table(org_id, entity_table)

#Gets all parquet files for a given org_id.entity_table
def get_all_files(org_id, entity_table):
  print(f"Retrieving all files for {org_id}.{entity_table}")
  file_path_list = []
  entity_table_id = entity_table.split("_")[-1]
  org_id = org_id.split("_")[-1]
  file_info_list = dbutils.fs.ls(f"{parquet_path}/{org_id}/{entity_table_id}")
  for file_info in file_info_list:
    file_path_list += [file_info[0]]
  print(f"Retrieved files for {org_id}.{entity_table}: {file_path_list}")
  return file_path_list

def read_files(file_list):
  if len(file_list) > 0:
    return spark.read.parquet(*file_list)
  print("Newer file list was empty, returning None")
  return None

# Reads in all data from file_list and adds um_processed_date before appending the data to the org_id.entity_table
def load_data(org_id, entity_table, file_list):
  print(f"Loading all data to {org_id}.{entity_table}")
  existing_data_df = read_files(file_list)
  if existing_data_df is None:
    print("No files to read, skipping to next entityTable")
    return
  print("Successfully read all data")
  existing_data_df = existing_data_df.withColumn("um_processed_date", F.to_date(F.col("um_creation_date_utc"),"MM-dd-yyyy"))
  print("Successfully added um_processed_date column")
  append_data_to_table(existing_data_df, org_id, entity_table)

  print(f"Successfully appended data to {org_id}.{entity_table}")

def append_data_to_table(newer_data, org_id, entity_table):
  if newer_data:
    newer_data.write.partitionBy("um_processed_date").mode("append").saveAsTable(f"{org_id}.{entity_table}")
  else:
    print("Dataframe passed to append_newer_file_dataframe with {org_id}.{entity_table} was None, not appending data")

def drop_table(org_id, entity_table):
    spark.sql(f"DROP TABLE {org_id}.{entity_table}")

def rename_dbtemp_table(org_id, entity_table):
  spark.sql(f"ALTER TABLE {org_id}.{entity_table}_dbtemp RENAME TO {org_id}.{entity_table}")
  
    
def delete_files(file_list):
  for file in file_list:
    print(f"Removing file {file}")
    dbutils.fs.rm(file)
