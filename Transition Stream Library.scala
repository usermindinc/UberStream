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
  print("Showing values of df select data.*")
  df.select("data.*").show()
  print("Showing values of df select data.* values.*")
  //df.select("data.*").select($"values.*").show()
  df.select("data.*").select($"*", $"values.*").show()
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

// Function to fill SessionID column if ObjectID is not null. This function is useful as Crucible switches over from writing ObjectId to SessionId in the Uberstream.
def moveObjectIdToSessionId(df: DataFrame) : DataFrame = {
  println("Moving object ID values to sessionID")
  val newDf = df.withColumn("sessionId", when($"sessionId".isNull || lit($"sessionId").equals(""), $"objectId").otherwise($"sessionId"))
  newDf.show()
  return newDf
}

//Function to add the necessary new columns to the streaming_logs.transition_stream table
def addNewColumnsToTransitionLogTable(table: String): Unit = {
  val sessionIdCommand = s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.sessionId String AFTER objectId);"
  val dataCommand = s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.values.transitionNumber String AFTER travelerStatus, data.values.currentMilestoneId String AFTER transitionNumber, data.values.previousMilestoneId String AFTER currentMilestoneId, data.values.timeInPreviousMilestone String AFTER previousMilestoneId);"
  val commandList: List[String] = List(sessionIdCommand, dataCommand)
  for (command <- commandList) {
    try {
      spark.sql(command)
    } catch {
      case ex: AnalysisException => println("New columns already present in table: " + ex)
      case ex: Throwable => throw ex
    }
  }
}

// COMMAND ----------

// DBTITLE 1,Start Transition Stream
import scala.collection.convert.decorateAsScala._

def startTransitionStream(dfStream : DataFrame) : StreamingQuery = {
  dfStream
    .writeStream
    .foreachBatch ( (df: DataFrame, batchId: Long) => {
      //augment stream data and create a view for logging
      val current_time = current_timestamp()
      
      df.withColumn("batchId", lit(batchId))
          .withColumn("processingTimestamp", lit(current_time))
          .withColumn("processingDate", to_date(lit(current_time),"yyyy-MM-dd"))
          .createOrReplaceTempView("transitionStreamContent")
      
      //format stream to just contain transition data
      val dfFormatted = formatTransitionStream(df)
      val dfFormattedWithSessionId = moveObjectIdToSessionId(dfFormatted)
      dfFormattedWithSessionId.createOrReplaceTempView("transitionUpdates")
      println("Showing formatted df")
        dfFormattedWithSessionId.show()
      //branch by org
      df.sparkSession.sql(s""" SELECT DISTINCT orgId FROM transitionUpdates """)
        .collect()
        .map(_(0).asInstanceOf[Long])
        .toList
        .foreach(orgId => {
          println(s"Processing orgID $orgId")
          //create trasition table for new org
          if(!hasTable(orgId, "transition")){
            val dfByOrg = df.sparkSession.sql(s"""SELECT * FROM transitionUpdates WHERE orgId = $orgId""")
            createTransitionTable(orgId, dfFormattedWithSessionId)
          } else {
            validateAndUpdateTransitionTableSchema(orgId)
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
              println(s"Showing org_$orgId.usermind_transition")
              df.sparkSession.sql(s"SELECT * FROM org_$orgId.usermind_transition").show()
              println("Showing transitionUpdatesByOrgAndNamespace")
              df.sparkSession.sql("SELECT * FROM transitionUpdatesByOrgAndNamespace").show()
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
            df.sparkSession.sql(s"""CREATE TABLE $logDatabase.$transitionLogTable PARTITIONED BY (processingDate) AS SELECT * FROM transitionStreamContent""")    
        } else {
            df.sparkSession.sql(s"""INSERT INTO $logDatabase.$transitionLogTable SELECT * FROM transitionStreamContent""")         
        }
      () // return Unit otherwise it will crash, weird scala 2.12 issue
    })
    .trigger(Trigger.ProcessingTime(INTERVAL)) // without this it starts when the last batch ends
    .start()
}
