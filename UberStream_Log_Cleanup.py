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

MAX_OFFSET_PER_PARTITION_QUERY = """
SELECT group_by_data.topic, group_by_data.partition, group_by_data.offset, outer_data.timestamp, outer_data.data, outer_data.batchid, outer_data.processingTimestamp, outer_data.processingDate
FROM streaming_logs.[STREAM_TABLE] outer_data
INNER JOIN (SELECT topic, partition, MAX(offset) AS offset FROM streaming_logs.[STREAM_TABLE]
GROUP BY topic, partition) group_by_data
ON outer_data.offset = group_by_data.offset"""
STREAM_TABLE_KEY = "[STREAM_TABLE]"

# COMMAND ----------

def filter_old_rows(table_name, cutoff_timestamp):
    return spark.sql(f"SELECT * FROM streaming_logs.{table_name} WHERE processingTimestamp > '{cutoff_timestamp}'")

def overwrite_streaming_log_table(df, streaming_log_table):
    df.write.mode("overwrite").saveAsTable(f"streaming_logs.{streaming_log_table}")

def validate_ttl(ttl):
    try:
        return int(ttl)
    except Exception as ex:
        print(f"Unable to parse provided TTL: {ttl}")
        raise ex

# We want to ensure there is always at least one entry in the table. If all entries are older than the TTL, we pick the latest entry to keep
def ensure_one_entry(table_name, filtered_df):
    if filtered_df.count() > 0:
        return filtered_df
    query = MAX_OFFSET_PER_PARTITION_QUERY.replace(STREAM_TABLE_KEY, table_name)
    return spark.sql(query)
    
parsed_ttl = validate_ttl(CLEANUP_TTL_VAL)
current_time_epoc_secs = time.time()
time_diff = parsed_ttl * 24 * 60 * 60 #TTL days in milliseconds
cutoff_time_epoc_secs = current_time_epoc_secs - time_diff
cutoff_timestamp = datetime.datetime.fromtimestamp(cutoff_time_epoc_secs)

for streaming_log_table in STREAMING_LOG_TABLES:
    try:
        print(f"Performing cleanup on table {streaming_log_table} of all entries older than {cutoff_timestamp}")
        filtered_df = filter_old_rows(streaming_log_table, cutoff_timestamp)
        filtered_df.show()
        filtered_df = ensure_one_entry(streaming_log_table, filtered_df)
        print(f"Showing Filtered data for table {streaming_log_table} with count {filtered_df.count()}")
        filtered_df.show()
        
        overwrite_streaming_log_table(filtered_df, streaming_log_table)
        print(f"Successfully cleaned up table {streaming_log_table} of all entries older than {cutoff_timestamp}")
    except Exception as ex:
        print(f"Encountered exception processing table {streaming_log_table}")
        raise ex


# COMMAND ----------


