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

def get_newest_file(path):
  URI = sc._gateway.jvm.java.net.URI
  Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
  FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
  conf = sc._jsc.hadoopConfiguration()
  pathObject = Path(path)
  fs = pathObject.getFileSystem(sc._jsc.hadoopConfiguration())

  inodes = fs.listStatus(pathObject)
  time = inodes[0].getModificationTime()
  file = inodes[0].getPath().toString()

  for i in range(1, len(inodes)):
    if (inodes[i].getModificationTime() > time):
      time = inodes[i].getModificationTime()
      file = inodes[i].getPath().toString()
  return file

def fix_spark_sql_missing_columns(org_id, entity_name):
  entity_id = entity_name.split('_')[-1]
  path = '%s/%s/%s' % (parquet_path, org_id, entity_id)
  table_name = 'org_%s.%s' % (org_id, entity_name)
  stable = spark.table(table_name)
  try:
    ptable = spark.read.parquet(get_newest_file(path))
    map_new = { field.name : field for field in ptable.schema }
    map_old = { field.name : field for field in stable.schema }
    new_fields = { k : map_new[k] for k in set(map_new) - set(map_old) }

    if (len(new_fields) > 0):
      column_definitions = ', '.join(['%s %s' % (field.name, field.dataType.simpleString()) for field in new_fields.values()])
      sql = 'ALTER TABLE %s ADD COLUMNS (%s)' % (table_name, column_definitions)
      print(sql)
      spark.sql(sql)
    else:
      print('table org_%s.%s is up to date' % (org_id, entity_name))
  except:
    print('table org_%s.%s does not have a parquet file' % (org_id, entity_name))

def is_table_databricks_managed(schema, table, create_table_query):
  try:
    if spark.sql(f"DESCRIBE TABLE EXTENDED {schema}.{table}").where("col_name = 'Type' AND data_type = 'MANAGED'").count() > 0:
      print(f"Table {schema}.{table} is databricks managed")
      return True
    print(f"Table {schema}.{table} is NOT databricks managed")
  except AnalysisException as Ex:
    print(f"Table {schema}.{table} not found, creating table now")
    spark.sql(create_table_query)
    return True
  
# We are no longer using the parquet location for the table so we want the query up to the USING keyword and then add um_creation_date and um_processed_timestamp columns  
def format_create_table_query(create_table_query):
  unclosed_query = create_table_query.split(")  USING")[0]
  unclosed_query += ", `um_processed_date` DATE, `um_processed_timestamp` TIMESTAMP"
  closed_query = unclosed_query + ")"
  print(closed_query)
  return closed_query
    
def replace_table_with_databricks_managed_table(schema, table, current_time):
  table_df = spark.table(f"{schema}.{table}") 
  spark.sql(f"CREATE TABLE {schema}.{table}_backup AS SELECT * FROM {schema}.{table}")
  print(f"Finished creating {schema}.{table}_backup, now overwriting {schema}.{table}")
  #Overwrite original table with processingDate and partitioned by processingDate
  print(f"Dropping original non-databricks managed table {schema}.{table}")
  spark.sql(f"DROP TABLE {schema}.{table}")
  print(f"Recreating databricks managed table from {schema}.{table}_backup")
  spark.sql(f"CREATE OR REPLACE TABLE {schema}.{table}_test1 PARTITIONED BY (um_processed_date) AS SELECT *, CAST('{current_time}' AS DATE) \
            AS um_processed_date, '{current_time} AS um_processed_timestamp' FROM {schema}.{table}_backup")  

def get_max_um_processed_timestamp(org_id, entity_table):
  return spark.sql(f"""SELECT MAX(um_processed_timestamp) AS processed_timestamp FROM {org_id}.{entity_table}
      WHERE um_processed_date = '(SELECT MAX(um_processed_date) FROM {org_id}.{entity_table})'""").collect()[0]["processed_timestamp"]

def file_newer_than_last_processed(schema_name, table_name, last_processed):
  entity_id = table_name.split("_")[-1]
  path = f"{parquet_path}/{schema_name}/{entity_id}"
  if last_processed is None:
    last_processed_epoc = 1658102400011
  else:
    last_processed_epoc = last_processed.timestamp() * 1000
  URI = sc._gateway.jvm.java.net.URI
  Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
  FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
  conf = sc._jsc.hadoopConfiguration()
  pathObject = Path(path)
  fs = pathObject.getFileSystem(sc._jsc.hadoopConfiguration())
  try:
    inodes = fs.listStatus(pathObject)
    time = inodes[0].getModificationTime()
    file = inodes[0].getPath().toString()
    files = []
    for i in range(0, len(inodes)):
      if (inodes[i].getModificationTime() > last_processed_epoc):
        time = inodes[i].getModificationTime()
        files += [inodes[i].getPath().toString()]
    return files
  except Py4JJavaError as ex:
    print(f"Unable to process {schema_name}.{table_name}")
    return []

def read_newer_files(newer_file_list):
  if len(newer_file_list) > 0:
    return spark.read.parquet(*newer_file_list)
  print("Newer file list was empty, returning None")
  return None


def append_newer_file_dataframe(newer_data, org_id, entity_table):
  if newer_data:
    newer_data.write.mode("append").saveAsTable(f"org_{org_id}.{entity_table}_test1")
  else:
    print("Dataframe passed to append_newer_file_dataframe with {org_id}.{entity_table} was None, not appending data")


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
    print(table_row)
    table_dict = table_row.asDict()
    schema_name = table_dict["schema_name"]
    table_name = table_dict["table_name"]
    if not is_table_databricks_managed(schema_name, table_name, format_create_table_query(table_dict["create_table_query"])):
      replace_table_with_databricks_managed_table(schema_name, table_name)
    latest_processed_time = get_max_um_processed_timestamp(schema_name, table_name)
    print(f"Latest processed time for {schema_name}.{table_name} is {latest_processed_time}")
    new_files = file_newer_than_last_processed(schema_name, table_name, latest_processed_time)
    print(f"Found the following files to append to {schema_name}.{table_name}: {new_files}")
    append_newer_file_dataframe(read_newer_files(new_files), schema_name, table_name)

# COMMAND ----------


