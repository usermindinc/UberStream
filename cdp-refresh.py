# Databricks notebook source
sqlContext.sql('set spark.sql.caseSensitive=true')

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

# MAGIC %run /Repos/CDP/UberStream/cdp-conversion-library

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

def delete_from_update_table(id):
  conn = None
  try:
    conn = psycopg2.connect(host=jdbc_hostname,database=jdbc_database, user=thylacine_aurora_username, password=thylacine_aurora_password)
    cur = conn.cursor()
    delete_sql = "DELETE FROM umcnc.table_updates_parquet WHERE id = " + str(id)
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

# Iterates through all parquet files to find the most recent
def get_newest_file(org_id, entity_table):
  file_path_list = []
  org_id = org_id.split("_")[-1]
  entity_table_id = entity_table.split("_")[-1]
  file_info_list = dbutils.fs.ls(f"{parquet_path}/{org_id}/{entity_table_id}")
  most_recent_time = 0 #Epoc start
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


# COMMAND ----------

# Driver

# Select all rows to refresh
data = select_from_update_table()

# Store max id so that we can delete records after refresh
# get data count for timings
data_count = data.count();
print("Entities to update: " + str(data_count))

if data_count > 0:
  # refresh tables
  pending_refreshes = data.rdd.collect()
  print(pending_refreshes)
  raise Ex()
  for table_row in pending_refreshes:
    try:
        print(table_row)
        table_dict = table_row.asDict()
        org_id = table_dict["schema_name"]
        entity_table = table_dict["table_name"]
        file_list = get_all_files(org_id, entity_table)
        if not is_table_databricks_managed(org_id, entity_table, create_table_query):
          replace_table_with_databricks_managed_table(org_id, entity_table)
        elif len(file_list) > 0:
          fix_spark_sql_missing_columns(org_id, entity_table)
          print(f"Found the following files to append to {org_id}.{entity_table}: {file_list}")
          load_data(org_id, entity_table, file_list)
        #delete_files(file_list)
        #delete_from_update_table(table_dict["id"])
    except Exception as ex:
        print(f"Unable to process {org_id}.{entity_table} due to exception: {ex}")
        raise ex
  

# COMMAND ----------


