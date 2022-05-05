// Databricks notebook source
// DBTITLE 1,Transition Stream Library
// MAGIC %md
// MAGIC 
// MAGIC ## Usage Notes
// MAGIC - Usage
// MAGIC 
// MAGIC 
// MAGIC ## TODO
// MAGIC - Some kind of automated test

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

val transitionTopic = "insights-transitions"
val transitionLogTable = "transition_stream"

// COMMAND ----------

// DBTITLE 1,Format Stream
val transitionSchema = new StructType()
  .add("creationDateUtc", StringType)
  .add("customerDateUtc", StringType)
  .add("executionType", StringType)
  .add("id", StringType)
  .add("jobId", StringType)
  .add("journeyId", StringType)
  .add("journeyName", StringType)
  .add("namespace", StringType)
  .add("objectId", StringType)
  .add("sessionId", StringType)
  .add("orgId", LongType)
  .add("waveId", StringType)
// Values subdoc
  .add("values", new StructType()
       .add("currentValue", StringType)
       .add("milestone", StringType)
       .add("previousValue", StringType)
       .add("travelerGroup", ShortType)
       .add("travelerLeadConnectionId", StringType)
       .add("travelerLeadConnectionName", StringType)
       .add("travelerLeadEntityId", StringType)
       .add("travelerLeadEntityType", StringType)
       .add("travelerLeadIntegrationType", StringType)
       .add("travelerOutcome", StringType)
       .add("travelerStatus", StringType)
       .add("transitionNumber", StringType)
       .add("currentMilestoneId", StringType)
       .add("previousMilestoneId", StringType)
       .add("timeInPreviousMilestone", StringType)
     )

def extractTransitionKafkaStream(df:DataFrame) : DataFrame = {
  df.select($"topic", $"partition", $"offset", $"timestamp", from_json($"value".cast("string"), transitionSchema).alias("data"))
}

def formatTransitionStream(df:DataFrame) : DataFrame = {
  val formattedDf = df.select("data.*")
    .select($"*", $"values.*")
    .drop($"values")

  val convertedCreationDate = convertStringToTimestamp(formattedDf, "creationDateUtc")
  val convertedCustomerDate = convertStringToTimestamp(convertedCreationDate, "customerDateUtc")
  convertedCustomerDate.withColumn("creationDate", col("creationDateUtc").cast(DateType))
  
}


// COMMAND ----------

// DBTITLE 1,Create Table


def createTransitionTable(orgId:Long, dfUpdates: DataFrame) : Unit = {
  println("Creating Transition Table for " + orgId)
  
  spark.sql(s"CREATE DATABASE IF NOT EXISTS org_$orgId")
  
  val path = pathForTable(orgId, 2)
  createDeltaTable(dfUpdates, path)
  addTableToUI(orgId, "transition", path)
}

// COMMAND ----------

// Function to add four new columns to the usermind_transition table for every org
def validateAndUpdateTransitionTableSchema(orgId: Long) : Unit = {
  val newColumns = List("sessionId", "transitionNumber", "currentMilestoneId", "previousMilestoneId", "timeInPreviousMilestone")
  println(s"Validating TransitionTable Schema for org_$orgId.usermind_transition to have new columns $newColumns")
  val transition_table = spark.table(s"org_$orgId.usermind_transition").limit(0)
  val column_set = transition_table.columns.toSet
  val baseSql = s"""
ALTER TABLE org_$orgId.usermind_transition ADD columns"""
  println(column_set)
  var addingColumns = false
  var addingColumnsString = ""
  for (newColumn <- newColumns) {
    if (!column_set.contains(newColumn)) {
      addingColumns = true
      if (newColumn.equals("sessionId")) {
        addingColumnsString += s" $newColumn String AFTER objectId,"
      } else {
        addingColumnsString += s" $newColumn String,"
      }
    }
  }
  
  if (addingColumns) {
    addingColumnsString = addingColumnsString.substring(0, addingColumnsString.length()-1)
    val newColumnSql = baseSql + s"( $addingColumnsString);"
    println(s"Sql Query to be run: $newColumnSql on table org_$orgId.usermind_transition")
    transition_table.sparkSession.sql(newColumnSql)
  } else {
    println(s"Table org_$orgId.usermind_transition validated to have all new columns $newColumns")
  }
}

def moveObjectIdToSessionId(df: DataFrame) : DataFrame = {
  //val newDf = df.withColumn("D", when($"B".isNull or $"B" === "", 0).otherwise(1))
  val newDf = df.withColumn("sessionId", when($"objectId".isNotNull, $"objectId"))
  newDf.show()
  return newDf
}

def addNewColumnsToTransitionLogTable(table: String): Unit = {
  spark.sql(s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.sessionId String AFTER objectId);")
  spark.sql(s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.values.transitionNumber String AFTER travelerStatus, data.values.currentMilestoneId String AFTER transitionNumber, data.values.previousMilestoneId String AFTER currentMilestoneId, data.values.timeInPreviousMilestone String AFTER previousMilestoneId);")
}


// COMMAND ----------

def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  return reader.json(sc.parallelize(Array(json)))
}

val transitionSchema = new StructType()
  .add("creationDateUtc", StringType)
  .add("customerDateUtc", StringType)
  .add("executionType", StringType)
  .add("id", StringType)
  .add("jobId", StringType)
  .add("journeyId", StringType)
  .add("journeyName", StringType)
  .add("namespace", StringType)
  .add("objectId", StringType)
  .add("orgId", LongType)
  .add("waveId", StringType)
  .add("sessionId", StringType)
// Values subdoc
  .add("values", new StructType()
       .add("currentValue", StringType)
       .add("milestone", StringType)
       .add("previousValue", StringType)
       .add("travelerGroup", ShortType)
       .add("travelerLeadConnectionId", StringType)
       .add("travelerLeadConnectionName", StringType)
       .add("travelerLeadEntityId", StringType)
       .add("travelerLeadEntityType", StringType)
       .add("travelerLeadIntegrationType", StringType)
       .add("travelerOutcome", StringType)
       .add("travelerStatus", StringType)
       .add("transitionNumber", StringType)
       .add("currentMilestoneId", StringType)
       .add("previousMilestoneId", StringType)
       .add("timeInPreviousMilestone", StringType)
     )


val events = jsonToDataFrame("""
{ 
  "creationDateUtc" : "2019-11-13T20:08:47.408",
  "customerDateUtc" : "2019-11-13T20:08:47.408",
  "id": "usermind-c3ba1b72-ef34-4e12-8068-510b541fa9b9-1611002051482-f38b74a2-9458-4fe0-a462-58bc0f3e52a9",  
  "jobId": "862b5ab5-9de1-4864-8614-a58c7ec4b748",
  "journey_id":"32784",
  "journeyName":"webhook",
  "namespace":"43ebe2b0-5b6a-4fba-93ba-e7386c2c616f",
  "objectId": "c3ba1b72-ef34-4e12-8068-510b541fa9b9",
  "orgId": "186",
  "waveId": "1611001971000",
    "values": {
      "currentValue": "prvi",
      "milestone": "r",
      "previousValue": "",
      "travelerGroup": "67s",
      "travelerLeadConnectionId": "1967",
      "travelerLeadConnectionName": "Webhook",
      "travelerLeadEntityId": "1",
      "travelerLeadEntityType": "foo",
      "travelerLeadIntegrationType": "webhook",
      "travelerOutcome": "",
      "travelerStatus": "Journey",
      "transitionNumber": "5",
      "transitionTime": "6000",
      "actionsGenerated": "5"
    }
}
""", transitionSchema)
//val df = spark.table("streaming_logs.transition_stream")
//df.write.format("delta").mode("overwrite").saveAsTable("streaming_logs.transition_stream_temp")
//spark.sql("ALTER TABLE streaming_logs.transition_stream_temp ADD COLUMNS (data.values.transitionNumber String AFTER travelerStatus, data.values.currentMilestoneId String AFTER transitionNumber, data.values.previousMilestoneId String AFTER currentMilestoneId, data.values.timeInPreviousMilestone String AFTER previousMilestoneId);")
//val df = jsonToDataFrame(events, transitionSchema)
//spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 
//events.write.format("delta").mode("overwrite").saveAsTable(s"org_100000000001.usermind_transition")
//spark.databricks.delta.schema.autoMerge.enabled
//val df = spark.table("streaming_logs.transition_stream")
//df.write.format("delta").mode("overwrite").saveAsTable("streaming_logs.transition_stream_temp")
//spark.sql("SELECT * FROM streaming_logs.transition_stream ORDER BY Offset Desc").show()
//val df = spark.table("org_458.usermind_transition")
//df.filter("transitionNumber is not NULL").select("jobId", "namespace", "previousMilestoneId").show()
"""
.add("currentValue", StringType)
       .add("milestone", StringType)
       .add("previousValue", StringType)
       .add("travelerGroup", ShortType)
       .add("travelerLeadConnectionId", StringType)
       .add("travelerLeadConnectionName", StringType)
       .add("travelerLeadEntityId", StringType)
       .add("travelerLeadEntityType", StringType)
       .add("travelerLeadIntegrationType", StringType)
       .add("travelerOutcome", StringType)
       .add("travelerStatus", StringType)
       .add("transitionNumber", StringType)
       .add("transitionTime", StringType)
       .add("actionsGenerated", StringType)
"""
val df = spark.table("org_10000.usermind_transition")
val df2 = df.filter($"orgId" !== "458")
df2.show()
df2.write.format("delta").partitionBy("namespace", "creationDate").mode("overwrite").saveAsTable("org_10000.usermind_transition")

//df.write.format("delta").mode("overwrite").saveAsTable("streaming_logs.transition_stream_temp")
//val df_with_dropped_collumns = df.drop("data.values.sessionId")
//df_with_dropped_collumns.show()
//df_with_dropped_collumns.write.partitionBy("namespace", "creationDate").format("delta").mode("overwrite").saveAsTable("org_477.usermind_transition")
//dbutils.fs.rm("/user/hive/warehouse/streaming_logs.db/travelerupdatesbyorgandnamespace")

// COMMAND ----------

// DBTITLE 1,Start Transition Stream
import scala.collection.convert.decorateAsScala._

def startTransitionStream(dfStream : DataFrame) : StreamingQuery = {
  dfStream
    .writeStream
    .foreachBatch ( (df: DataFrame, batchId: Long) => {
      //augment stream data and create a view for logging
      df.withColumn("batchId", lit(batchId)).withColumn("processingTimestamp", lit(current_timestamp())).createOrReplaceTempView("transitionStreamContent")
      
      //format stream to just contain transition data
      val dfFormatted = formatTransitionStream(df)
      dfFormatted.createOrReplaceTempView("transitionUpdates")
      
      //branch by org
      df.sparkSession.sql(s""" SELECT DISTINCT orgId FROM transitionUpdates """)
        .collect()
        .map(_(0).asInstanceOf[Long])
        .toList
        .foreach(orgId => {
          //create trasition table for new org
          println(s"Working on orgID $orgId")
          if(!hasTable(orgId, "transition")){
            val dfByOrg = df.sparkSession.sql(s"""SELECT * FROM transitionUpdates WHERE orgId = $orgId""")
            createTransitionTable(orgId, dfFormatted)
          } else {
            validateAndUpdateTransitionTableSchema(orgId)
            moveObjectIdToSessionId(df)
            // force refresh before merge	
            df.sparkSession.sql(s"""	
              REFRESH TABLE org_$orgId.usermind_transition	
              """)   
          
              df.sparkSession.sql(s"""SELECT DISTINCT namespace FROM transitionUpdates WHERE orgId=$orgId""")
                  .collect()
                  .map(_(0).asInstanceOf[String])
                  .toList
                  .foreach(namespace => {
                    val dfUpdates = df.sparkSession.sql(s"""
                      SELECT *
                      FROM transitionUpdates 
                      WHERE orgId = $orgId AND namespace = '$namespace'
                      """)
                    dfUpdates.createOrReplaceTempView("transitionUpdatesByOrgAndNamespace")       

              val mergeSQL = s"""
                  MERGE INTO org_$orgId.usermind_transition T
                  USING transitionUpdatesByOrgAndNamespace U
                    ON T.namespace = '$namespace' AND T.id = U.id AND T.orgId = U.orgId AND T.namespace = U.namespace AND T.creationDate = U.creationDate
                  WHEN MATCHED THEN UPDATE SET *    
                  WHEN NOT MATCHED THEN INSERT *  """
                    
                df.sparkSession.sql(mergeSQL)
                  })
            }          
        })
      if(!tableExists(logDatabase + "." + transitionLogTable)){
            df.sparkSession.sql(s"""CREATE TABLE $logDatabase.$transitionLogTable AS SELECT * FROM transitionStreamContent""")    
        } else {
            df.sparkSession.sql(s"""INSERT INTO $logDatabase.$transitionLogTable SELECT * FROM transitionStreamContent""")         
        }
      () // return Unit otherwise it will crash, weird scala 2.12 issue
    })
    .trigger(Trigger.ProcessingTime(INTERVAL)) // without this it starts when the last batch ends
    .start()
}
