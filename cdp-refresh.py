# Databricks notebook source
sqlContext.sql('set spark.sql.caseSensitive=true')

dbutils.widgets.text('parquet_path', 's3://acid-cdp-staging', 'Base location of parquet files')

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import time
import psycopg2
import datetime
from datetime import timedelta
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

# COMMAND ----------

#All postgres access functions here.

thylacine_aurora_username = dbutils.secrets.get(scope = "thylacine-aurora", key = "username")
thylacine_aurora_password = dbutils.secrets.get(scope = "thylacine-aurora", key = "password")
jdbc_hostname = dbutils.secrets.get(scope = "aurora-host", key = "aurora-host-name")
jdbc_database = 'insights'
jdbc_port = '5432'

current_env = dbutils.secrets.get(scope = "databricks-workspaces", key = "current-environment")


input_path = dbutils.widgets.get('parquet_path')
parquet_path = input_path[0:-1] if input_path.endswith('/') else input_path

def delete_from_update_table(max_id):
  conn = None
  try:
    conn = psycopg2.connect(host=jdbc_hostname,database=jdbc_database, user=thylacine_aurora_username, password=thylacine_aurora_password)
    cur = conn.cursor()
    delete_sql = "DELETE FROM umcnc.table_updates_parquet WHERE id <= " + str(max_id)
    print(delete_sql)
    cur.execute(delete_sql)
    cur.close()
    conn.commit()
  except Exception as ex:
    print(ex)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')
  
def select_from_update_table():  
  jdbcUrl = "jdbc:postgresql://%s:%s/%s?user=%s&password=%s" % (jdbc_hostname, jdbc_port, jdbc_database, thylacine_aurora_username, thylacine_aurora_password)
  query =  """(SELECT * FROM (select distinct on (schema_name, table_name) 
            schema_name, table_name, id, create_table_query
            from umcnc.table_updates_parquet 
            order by schema_name, table_name, created_at) as t ORDER BY id) as v """
  return sqlContext.read.jdbc(jdbcUrl, query, properties={'user':thylacine_aurora_username, 'password':thylacine_aurora_password, 'sslmode':'require'})

# COMMAND ----------

#refresh/create function

get_time = lambda: int(round(time.time() * 1000))


def get_cdp_parquet_bucket():
  if current_env == "pre-prod":
    return "s3a://usermind-preprod-cdp"
  elif current_env == "staging":
    return "s3a://acid-cdp-staging"

# Iterates through all parquet files to find the most recent
def get_newest_file(org_id, entity_table):
  file_path_list = []
  org_id = org_id.split("_")[-1]
  entity_table_id = entity_table.split("_")[-1]
  file_info_list = dbutils.fs.ls(f"{get_cdp_parquet_bucket()}/{org_id}/{entity_table_id}")
  most_recent_time = 1658188800011 #Epoc start
  most_recent_file = ""
  for file_info in file_info_list:
    if file_info[3] > most_recent_time:
      most_recent_file = file_info[0]
      most_recent_time = file_info[3]
  print(f"Most recent file found is: {most_recent_file}")
  return most_recent_file

# Finds diff between current entity table columns, columns in latest entity table parquet file, and adds columns to entity table if new columns are found
def fix_spark_sql_missing_columns(org_id, entity_table):
  col_name_type_dict = {col[0]:col[1] for col in spark.sql(f"DESCRIBE TABLE {org_id}.{entity_table}").collect()}
  newest_file = get_newest_file(org_id, entity_table)
  print(f"Using newest file {newest_file} to detect schema additions for {org_id}.{entity_table}")
  ptable = spark.read.parquet(newest_file)
  col_name_type_dict_new = { field.name : field for field in ptable.schema }
  new_fields_types_dict = { col_name: col_name_type_dict_new[col_name] for col_name in set(col_name_type_dict_new) - set(col_name_type_dict)}
  if len(new_fields_types_dict) > 0:
    column_definitions = ', '.join([f'{field.name} {field.dataType.simpleString()}' for field in new_fields_types_dict.values()])
    sql = f'ALTER TABLE {org_id}.{entity_table} ADD COLUMNS {column_definitions}'
    print(f"SQL used to add columns to table {org_id}.{entity_table}: \n {sql}")
    spark.sql(sql)
  else:
    print(f'table {org_id}.{entity_table} is up to date')

# Checks if data type of table is MANAGED 
def is_table_databricks_managed(org_id, entity_table, create_table_query):
  try:
    if spark.sql(f"DESCRIBE TABLE EXTENDED {org_id}.{entity_table}").where("col_name = 'Type' AND data_type = 'MANAGED'").count() > 0:
      print(f"Table {org_id}.{entity_table} is databricks managed")
      return True
    print(f"Table {org_id}.{entity_table} is NOT databricks managed")
  except AnalysisException as Ex:
    print(f"Table {org_id}.{entity_table} not found, creating table now")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {org_id}")
    spark.sql(create_table_query)
    print(f"Successfully created table {org_id}.{entity_table}")
    return True
  
# We are no longer using the parquet location for the table so we want the query up to the USING keyword and then add um_creation_date and um_processed_timestamp columns  
def format_create_table_query(create_table_query):
  unclosed_query = create_table_query.split(")  USING")[0]
  unclosed_query += ", `um_processed_date` DATE"
  closed_query = unclosed_query + ")"
  closed_query += "PARTITIONED BY (um_processed_date)"
  return closed_query

# Creates backup of existing table, drops table, and then creates a new table partitioned by um_processed_date
def replace_table_with_databricks_managed_table(org_id, entity_table, create_table_query):
  print(f"Starting replacement process for {org_id}.{entity_table}")
  spark.sql(f"CREATE TABLE {org_id}.{entity_table}_backup AS SELECT * FROM {org_id}.{entity_table}")
  print(f"Finished creating {org_id}.{entity_table}_backup")
  #Overwrite original table with processingDate and partitioned by processingDate
  print(f"Dropping original non-databricks managed table {org_id}.{entity_table}")
  #spark.sql(f"DROP TABLE {org_id}.{entity_table}")
  print(f"Recreating databricks managed table from {org_id}.{entity_table}")
  spark.sql(f"{create_table_query}")  

#Gets all parquet files for a given org_id.entity_table
def get_all_files(org_id, entity_table):
  print(f"Retrieving all files for {org_id}.{entity_table}")
  file_path_list = []
  entity_table_id = entity_table.split("_")[-1]
  org_id = org_id.split("_")[-1]
  file_info_list = dbutils.fs.ls(f"{get_cdp_parquet_bucket()}/{org_id}/{entity_table_id}")
  for file_info in file_info_list:
    file_path_list += [file_info[0].replace("dbfs:/mnt/cdp/", "s3://usermind-preprod-cdp/")]
  print(f"Retrieved files for {org_id}.{entity_table}: {file_path_list}")
  return file_path_list

def read_files(file_list):
  if len(file_list) > 0:
    return spark.read.parquet(*file_list)
  print("Newer file list was empty, returning None")
  return None

# Reads in all data from file_list and adds um_processed_date before appending the data to the org_id.entity_table
def load_data(org_id, entity_table, file_list):
  existing_data_df = read_files(file_list)
  if existing_data_df is None:
    print("No files to read, skipping to next entityTable")
    return
  print("Successfully read all data")
  existing_data_df = existing_data_df.withColumn("um_processed_date", F.to_date(F.col("um_creation_date_utc"),"MM-dd-yyyy"))
  print("Successfully added um_processed_date column")
  existing_data_df.show()
  append_data_to_table(existing_data_df, org_id, entity_table)
  print(f"Successfully appended data to {org_id}.{entity_table}")

def append_data_to_table(newer_data, org_id, entity_table):
  if newer_data:
    newer_data.write.mode("append").saveAsTable(f"{org_id}.{entity_table}")
  else:
    print("Dataframe passed to append_newer_file_dataframe with {org_id}.{entity_table} was None, not appending data")

def delete_files(file_list):
  for file in file_list:
    print(f"Removing file {file}")
    dbutils.fs.rm(file)

# COMMAND ----------

# Driver

# Select all rows to refresh
data = select_from_update_table()

# Store max id so that we can delete records after refresh
max_id = data.agg({"id": "max"}).collect()[0]["max(id)"]
print("Max id: " + str(max_id))

# get data count for timings
data_count = data.count();
print("Entities to update: " + str(data_count))

if data_count > 0:
  # refresh tables
  data.show()
  pending_refreshes = data.rdd.collect()
  for table_row in pending_refreshes:
    try:
        print(table_row)
        table_dict = table_row.asDict()
        org_id = table_dict["schema_name"]
        entity_table = table_dict["table_name"]
        file_list = get_all_files(org_id, entity_table)
        create_table_query = format_create_table_query(table_dict["create_table_query"])
        if not is_table_databricks_managed(org_id, entity_table, create_table_query):
          replace_table_with_databricks_managed_table(org_id, entity_table, create_table_query)
        else:
          fix_spark_sql_missing_columns(org_id, entity_table)
        print(f"Found the following files to append to {org_id}.{entity_table}: {file_list}")
        load_data(org_id, entity_table, file_list)
        delete_files(file_list)
    except Exception as ex:
        print(f"Unable to process {org_id}.{entity_table} due to exception: {ex}")
        raise ex
  delete_from_update_table(max_id)

# COMMAND ----------


