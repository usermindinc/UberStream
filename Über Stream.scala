// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Über Stream
// MAGIC 
// MAGIC ## Usage Notes
// MAGIC - Usage
// MAGIC   - Can be run as a job
// MAGIC   - Note that when restarted, it will reprocess entire topics and produce duplicate metrics
// MAGIC 
// MAGIC ## TODO
// MAGIC - Resolve Mismatches between Kakfa data and table schemas/aurora schemas
// MAGIC - Automated Testing
// MAGIC - Metrics
// MAGIC - Get env data from env secrets
// MAGIC - Check for duplicates, merge update will fail on dupes
// MAGIC - What happens to unparseable kafka messages?
// MAGIC - Remove Parquet Consumers from Insights
// MAGIC - Drop related tables from `umcnc`
// MAGIC - Drop old `usermind_` parquet tables from S3 (maybe wait a couple weeks)
// MAGIC - Drop old `action_status` delta tables + remove them from S3 (check to see if anything uses them and repoint)
// MAGIC - Drop `umcnc.table_updates_parquet_versioned`
// MAGIC - Consider using consumer groups so we can see what it's doing/whether it's behind

// COMMAND ----------

// DBTITLE 1,Widgets
dbutils.widgets.text("Delta Path","s3://acid-cdp-staging/delta","Base location of Delta tables")
dbutils.widgets.text("kafkaBootstrapServers","kafka-0-staging.usermind.com:9092,kafka-1-staging.usermind.com:9092,kafka-2-staging.usermind.com:9092","Kafka Bootstrap Servers")
dbutils.widgets.text("INTERVAL","10 minutes","Pause between stream runs")
dbutils.widgets.text("env","staging","Env")


// COMMAND ----------

// DBTITLE 1,Imports and Variables
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

val inputPath = dbutils.widgets.get("Delta Path")
val DELTA_PATH = "s3://usermind-preprod-cdp/delta" //if(inputPath.endsWith("/")) inputPath else (inputPath + "/")

val kafkaBootstrapServers = dbutils.widgets.get("kafkaBootstrapServers")

// Defines how often we read from Kafka
val INTERVAL = dbutils.widgets.get("INTERVAL")

val env = dbutils.widgets.get("env")

val logDatabase = "streaming_logs"

// TODO compute this from the cluster size
spark.conf.set("spark.sql.shuffle.partitions", 16) // 16=4 cores x 4 host
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) //disable broadcast join as it makes the job fail with OOM exception

// COMMAND ----------

// DBTITLE 1,Kafka Functions
def loadKafkaStream(bootstrapServers : String, topic : String, offsets: String) : DataFrame = { 
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)   // comma separated list of broker:host
    .option("subscribe", topic)    // comma separated list of topics
    .option("startingOffsets", offsets)
    //.option("minPartitions", "12")  
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "100000") // TODO make this configurable
    .load()
}

// COMMAND ----------

// DBTITLE 1,Useful Functions
import scala.collection._

def hasOrg(orgId:Long) : Boolean = spark.catalog.databaseExists(s"org_$orgId")

def hasTable(orgId:Long, name:String) : Boolean = spark.catalog.tableExists(s"org_$orgId.usermind_$name")

def tableExists(name:String) : Boolean = spark.catalog.tableExists(s"$name")

def hasColumnForTable(orgId:Long, name:String, column:String) : Boolean = spark.catalog.listColumns(s"org_$orgId.usermind_$name").where(s"name = '$column'").count > 0


// useful function to find all orgs with table of type tableType (not used in the streams)
def dbList(tableType : String) : String = {
  val dbs = spark.sql("SHOW DATABASES").toDF()
              .filter($"databaseName" like "org_%")
              .withColumn("orgId", split(col("databaseName"), "_").getItem(1).cast("long"))

  val orgList = dbs.select("orgId")
      .collect()
      .map(row=>row.getLong(0))
      .filter(orgId=>hasTable(orgId, tableType))
      .mkString(",")
  
  if(orgList == "") "0" else orgList
}

def dbCreate(name:String) : Unit = {
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $name")
}

// a function which converts a column from string to timestamp depending on given format
def convertStringToTimestamp(df:DataFrame, dateColumn:String) : DataFrame = {
  df.withColumn(s"${dateColumn}1", to_timestamp(col(dateColumn)))
    .withColumn(s"${dateColumn}2", to_timestamp(col(dateColumn), "yyyy-MM-dd'T'HH:mm'Z'"))
    .withColumn(s"${dateColumn}3", when(col(s"${dateColumn}1").isNull, col(s"${dateColumn}2")).otherwise(col(s"${dateColumn}1")))
    .drop(dateColumn)
    .drop(s"${dateColumn}1")
    .drop(s"${dateColumn}2")
    .withColumnRenamed(s"${dateColumn}3", dateColumn)
}

def getLatestOffsets(topicName:String, tableName:String) : String = {
  spark.sql(s"SELECT partition, max(offset) offset FROM $tableName WHERE topic='$topicName' GROUP BY partition ORDER BY partition")
  .collect()
  .map(row => "\""+row.getInt(0).toString+"\":"+row.getLong(1).toString)
  .toList
  .mkString(",")
}

def getOffsets(topics: String, tableName: String) :String = {
  try {
      val results = topics.split(",").map(topic => {
        "\""+ topic.trim() +"\":{"+ getLatestOffsets(topic.trim(), tableName) + "}"
      }).mkString(",")
    "{" + results + "}"
  } catch {
    case _: Throwable => "earliest"
  }
}

// COMMAND ----------

// DBTITLE 1,Delta Table Functions
def pathForTable(orgId:Long, typeId:Int) : String = DELTA_PATH + orgId + "/" + typeId

def createDeltaTable(df:DataFrame, path:String) : Unit = {
  dbutils.fs.rm(path, true)
  df
    .write
    .format("delta") 
    .option("checkpointLocation", path + "/_checkpoint") 
    .partitionBy("namespace", "creationDate")
    .mode("Overwrite")
    .save(path)
}

def addTableToUI(orgId:Long, table:String, path: String) : Unit = {
  val tableName = s"org_$orgId.usermind_$table"
  spark.sql(s"DROP TABLE IF EXISTS $tableName")
  spark.sql(s"""
    CREATE TABLE $tableName
    USING DELTA
    LOCATION '$path'
  """)
  spark.sql(s"""
    ALTER TABLE $tableName
      SET TBLPROPERTIES ('delta.checkpointRetentionDuration' = '30 days')
  """)
  }

def addColumnToTable(orgId:Long, table:String, column:String, columnType:String) : Unit = {
  val tableName = s"org_$orgId.usermind_$table"
  spark.sql(s"""
    ALTER TABLE $tableName
      ADD COLUMNS ($column $columnType)
  """)
}

def addDateColumnToTable(table: String, dateCol: String) : Unit = {
  val dateColPresent = spark.sql(f"SHOW COLUMNS IN streaming_logs.$table%s").where(f"col_name = '$dateCol%s'")
  if (dateColPresent.count() > 0) {
    println(f"$dateCol%s column found in table streaming_logs.$table%s")
    return
  } else {
    println(f"$dateCol%s column not found in table streaming_logs.$table%s. Saving current table to streaming_logs.$table%s_backup")
    //Create backup table
    spark.sql(f"CREATE TABLE streaming_logs.$table%s_backup AS SELECT * FROM streaming_logs.$table%s")
    println(f"Finished creating streaming_logs.$table%s_backup, now overwriting streaming_logs.$table%s")
    //Overwrite original table with processingDate and partitioned by processingDate
    spark.sql(f"REPLACE TABLE streaming_logs.$table%s PARTITIONED BY (processingDate) AS SELECT *, CAST(processingTimestamp AS DATE) AS processingDate FROM streaming_logs.$table%s")  
  }
}


// COMMAND ----------

dbCreate(logDatabase)

// COMMAND ----------

// MAGIC %run "./Action Stream Library"

// COMMAND ----------

// MAGIC %run "./Transition Stream Library"

// COMMAND ----------

// MAGIC %run "./Traveler Stream Library"

// COMMAND ----------

// DBTITLE 1,Action Stream
addDateColumnToTable("action_stream", "processingDate")
addNewColumnsToActionLogTable("action_stream")
val actionOffsets = getOffsets(actionTopic, logDatabase + "." + actionLogTable)
val actionDF = extractActionKafkaStream(loadKafkaStream(kafkaBootstrapServers, actionTopic, actionOffsets))
val actionStream = startActionStream(actionDF)

// COMMAND ----------

// DBTITLE 1,Transition Stream
addDateColumnToTable("transition_stream", "processingDate")
addNewColumnsToTransitionLogTable("transition_stream")
val transitionOffsets = getOffsets(transitionTopic, logDatabase + "." + transitionLogTable)
println(transitionOffsets)
val offsets2 = "{\"insights-transitions\":{\"0\":5668000}}"
val kafkaBootstrapServers2 = "kafka-broker-0-preprod.preprod.usermind.com:9092,kafka-broker-1-preprod.preprod.usermind.com:9092,kafka-broker-2-preprod.preprod.usermind.com:9092"
val transitionDF = extractTransitionKafkaStream(loadKafkaStream(kafkaBootstrapServers2, transitionTopic, offsets2))
val transitionStream = startTransitionStream(transitionDF)

// COMMAND ----------

// DBTITLE 1,TravelerEvent Stream
addDateColumnToTable("traveler_stream", "processingDate")
val travelerOffsets = getOffsets(travelerTopic, logDatabase + "." + travelerLogTable)
val travelerDF = extractTravelerKafkaStream(loadKafkaStream(kafkaBootstrapServers, travelerTopic, travelerOffsets))
val travelerStream = startTravelerStream(travelerDF)

// COMMAND ----------


