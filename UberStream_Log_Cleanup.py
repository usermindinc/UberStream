# Databricks notebook source
import time
import datetime as datetime

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
  #processingTimestamp
  return df.filter(f"processingTimestamp < {cutoff_timestamp}")

def overwrite_streaming_log_table(df, streaming_log_table):
  df.write.saveAsTable(streaming_log_table, mode="overwrite")

def validate_ttl(ttl):
  try:
    return int(ttl)
  except Exception as ex:
    print("Unable to parse provided TTL")
    raise ex

parsed_ttl = validate_ttl(CLEANUP_TTL_VAL)
current_time_epoc_secs = time.time()
time_diff = parsed_ttl * 24 * 60 * 60 #TTL days in milliseconds
cutoff_time_epoc_secs = current_time_epoc_secs - time_diff
cutoff_timestamp = datetime.datetime.fromtimestamp(oldest_time_epoc_secs)
#for all streaming_log tables:
for streaming_log_table in STREAMING_LOG_TABLES:
  try:
    print(f"Performing cleanup on table {streaming_log_table} of all entries older than {oldest_timestamp}")
    df = load_table(f"streaming_logs.{streaming_log_table}")
    filtered_df = filter_old_rows(df, cutoff_timestamp)
    overwrite_streaming_log_table()
    print(f"Successfully cleaned up table {streaming_log_table} of all entries older than {oldest_timestamp}")
  except Exception as ex:
    print(f"Encountered exception processing table {streaming_log_table}")
    raise ex


# COMMAND ----------

test_data = [{
  
}]
