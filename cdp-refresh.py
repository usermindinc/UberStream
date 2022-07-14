# Databricks notebook source
sqlContext.sql('set spark.sql.caseSensitive=true')

dbutils.widgets.text('parquet_path', 's3://acid-cdp-staging', 'Base location of parquet files')

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import time
import psycopg2

# COMMAND ----------

umcnc.table_updates_parquet#All postgres access functions here.

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

def refresh_data(row): 
  schema_name = row.schema_name
  org_id = schema_name[4:]
  table_name = row.table_name
  try:
    start_time = get_time()
    fix_spark_sql_missing_columns(org_id, table_name)
    sqlContext.sql("REFRESH TABLE `" + schema_name + "`.`" + table_name + "`")
    return (get_time() - start_time)
  except Exception as ex: 
    print(ex)
    start_time = get_time()
    sqlContext.sql("CREATE DATABASE IF NOT EXISTS `" + schema_name + "`")
    print(row.create_table_query)
    sqlContext.sql(row.create_table_query)
    return (get_time() - start_time)

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
  refresh_results = list(map(refresh_data, pending_refreshes))
  print(refresh_results)
  print("average refresh time: " + str(sum(refresh_results) / data_count))

  # delete all refreshed tables
  delete_from_update_table(max_id)
