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

DELETE_QUERY = """
DELETE FROM streaming_logs.[STREAM_TABLE] WHERE
processingTimestamp < '[CUTOFF_TIMESTAMP]';
"""

DELETE_ALL_BUT_NEWEST_QUERY = """
DELETE FROM streaming_logs.[STREAM_TABLE] WHERE offset NOT IN (
SELECT group_by_data.offset
FROM streaming_logs.[STREAM_TABLE] outer_data
INNER JOIN (SELECT topic, partition, MAX(offset) AS offset FROM streaming_logs.[STREAM_TABLE]
GROUP BY topic, partition) group_by_data
ON outer_data.offset = group_by_data.offset)
"""



STREAM_TABLE_KEY = "[STREAM_TABLE]"
CUTOFF_TIMESTAMP_KEY = "[CUTOFF_TIMESTAMP]"


# COMMAND ----------

def ensure_one_entry(table_name, cutoff_timestamp):
    return spark.sql(f"SELECT * FROM streaming_logs.{table_name} WHERE processingTimestamp > '{cutoff_timestamp}'")

def execute_delete_query(table_name, cutoff_timestamp):
    query = DELETE_QUERY.replace(STREAM_TABLE_KEY, table_name) \
            .replace(CUTOFF_TIMESTAMP_KEY, str(cutoff_timestamp))
    print(f"Query being executed: {query}")
    df = spark.sql(query)
    df.show()

def execute_delete_all_but_newest_query(table_name):
    query = DELETE_ALL_BUT_NEWEST_QUERY.replace(STREAM_TABLE_KEY, table_name)
    print(f"Query being executed: {query}")
    df = spark.sql(query)
    df.show()

def validate_ttl(ttl):
    try:
        return int(ttl)
    except Exception as ex:
        print(f"Unable to parse provided TTL: {ttl}")
        raise ex




# COMMAND ----------

# We want to ensure there is always at least one entry in the table. If all entries are older than the TTL, we pick the latest entry to keep
    
parsed_ttl = validate_ttl(CLEANUP_TTL_VAL)
current_time_epoc_secs = time.time()
time_diff = parsed_ttl * 24 * 60 * 60 #TTL days in milliseconds
cutoff_time_epoc_secs = current_time_epoc_secs - time_diff
cutoff_timestamp = "2022-06-15T21:10:04"#datetime.datetime.fromtimestamp(cutoff_time_epoc_secs)

for streaming_log_table in STREAMING_LOG_TABLES:
    try:
        streaming_log_table += "_newestt"
        
        print(f"Performing cleanup on table {streaming_log_table} of all entries older than {cutoff_timestamp}")
        result = ensure_one_entry(streaming_log_table, cutoff_timestamp)
        if result.count() > 0:
            print("There was at least one row, performing normal time cutoff query")
            execute_delete_query(streaming_log_table, cutoff_timestamp)
        else:
            print(f"There were no rows newer than {cutoff_timestamp}, performing query to delete all but newest rows")
            execute_delete_all_but_newest_query(streaming_log_table)
    except Exception as ex:
        print(f"Encountered exception processing table {streaming_log_table}")
        raise ex

# COMMAND ----------

for table in STREAMING_LOG_TABLES:
    df = spark.table(f"streaming_logs.{table}")
    df.write.saveAsTable(f"streaming_logs.{table}_newestt")
    

# COMMAND ----------


