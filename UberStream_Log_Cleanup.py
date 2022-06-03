# Databricks notebook source
import time
import datetime as datetime
import pyspark.sql.functions as F

# COMMAND ----------

CLEANUP_TTL_KEY = "CLEANUP_TTL"
CLEANUP_TTL_DEFAULT = "14"
dbutils.widgets.text(CLEANUP_TTL_KEY, CLEANUP_TTL_DEFAULT, "Max time in days a row is kept before being deleted")
CLEANUP_TTL_VAL = dbutils.widgets.get(CLEANUP_TTL_KEY)
STREAMING_LOG_TABLES = ["action_stream", "transition_stream", "traveler_stream"]

# COMMAND ----------

def load_table(table_name):
  return spark.table(table_name)

def filter_old_rows(df, cutoff_timestamp):
  return df.filter(f"processingTimestamp > '{cutoff_timestamp}'")

def overwrite_streaming_log_table(df, streaming_log_table):
  df.write.saveAsTable(streaming_log_table, mode="overwrite")

def validate_ttl(ttl):
  try:
    return int(ttl)
  except Exception as ex:
    print("Unable to parse provided TTL")
    raise ex

# We want to ensure there is always at least one entry in the table. If all entries are older than the TTL, we pick the latest entry to keep
def ensure_one_entry(og_df, filtered_df):
  row = og_df.orderBy(F.col("processingTimestamp").desc()).first() 
  spark.createDataFrame([row], filtered_df.schema).show()
  if filtered_df.count() > 0:
    return filtered_df
  return spark.createDataFrame([row], filtered_df.schema)
        
    
parsed_ttl = validate_ttl(CLEANUP_TTL_VAL)
current_time_epoc_secs = time.time()
time_diff = parsed_ttl * 24 * 60 * 60 #TTL days in milliseconds
cutoff_time_epoc_secs = current_time_epoc_secs - time_diff
cutoff_timestamp = datetime.datetime.fromtimestamp(cutoff_time_epoc_secs)
print(current_time_epoc_secs, time_diff, cutoff_time_epoc_secs)

for streaming_log_table in STREAMING_LOG_TABLES:
  try:
    print(f"Performing cleanup on table {streaming_log_table} of all entries older than {cutoff_timestamp}")
    df = load_table(f"streaming_logs.{streaming_log_table}")
    df.show()
    filtered_df = filter_old_rows(df, cutoff_timestamp)
    
    filtered_df = ensure_one_entry(df, filtered_df)
    filtered_df.orderBy(F.col("processingTimestamp").asc()).show()
    overwrite_streaming_log_table(filtered_df, streaming_log_table)
    print(f"Successfully cleaned up table {streaming_log_table} of all entries older than {cutoff_timestamp}")
  except Exception as ex:
    print(f"Encountered exception processing table {streaming_log_table}")
    raise ex


# COMMAND ----------



# COMMAND ----------

for table in STREAMING_LOG_TABLES:
    df = spark.table(f"streaming_logs.{table}")
    new_table_name = table + "temp_3"
    print(new_table_name)
    df.write.saveAsTable(new_table_name)

# COMMAND ----------


