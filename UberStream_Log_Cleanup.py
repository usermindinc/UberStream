# Databricks notebook source
import time
from datetime import date, timedelta
import pyspark.sql.functions as F

# COMMAND ----------

CLEANUP_TTL_KEY = "CLEANUP_TTL"
CLEANUP_TTL_DEFAULT = "14"
dbutils.widgets.text(CLEANUP_TTL_KEY, CLEANUP_TTL_DEFAULT, "Max time in days a row is kept before being deleted")
CLEANUP_TTL_VAL = dbutils.widgets.get(CLEANUP_TTL_KEY)
STREAMING_LOG_TABLES = ["action_stream", "transition_stream", "traveler_stream"]

DELETE_QUERY = """
DELETE FROM streaming_logs.[STREAM_TABLE] WHERE
processingDate < '[CUTOFF_DATE]';
"""

STREAM_TABLE_KEY = "[STREAM_TABLE]"
CUTOFF_DATE_KEY = "[CUTOFF_DATE]"


# COMMAND ----------

def execute_delete_query(table_name, cutoff_date):
    query = DELETE_QUERY.replace(STREAM_TABLE_KEY, table_name) \
            .replace(CUTOFF_DATE_KEY, str(cutoff_date))
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

 
today_date = date.today()
diff = timedelta(parsed_ttl)
cutoff_date = today_date - diff
for streaming_log_table in STREAMING_LOG_TABLES:
    try:
        print(f"Performing cleanup on table {streaming_log_table} of all entries older than {cutoff_date}")
        execute_delete_query(streaming_log_table, cutoff_date)
    except Exception as ex:
        print(f"Encountered exception processing table {streaming_log_table}")
        raise ex

# COMMAND ----------



# COMMAND ----------


