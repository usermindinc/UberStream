// Databricks notebook source
// DBTITLE 1,Action Stream Library
// MAGIC %md
// MAGIC 
// MAGIC ## Usage Notes
// MAGIC - Usage
// MAGIC   - Set orgs to `0` to start stream on all orgs with a `usermind_action` table
// MAGIC 
// MAGIC ## TODO
// MAGIC - Read `namespace` and `creationDateUtc` from the `action-status` stream and optimize the merge
// MAGIC   - `select from_unixtime(ts,'YYYY-MM-dd') as 'ts' from mr`
// MAGIC - Resolve Mismatches between Kakfa data and table schema
// MAGIC - Confusion between `id` and `actionId`
// MAGIC - Consider only updating status within a certain date range
// MAGIC - What happens if we get an action status with no action?
// MAGIC - What happens if we get an unparseable action json? 
// MAGIC - Add a metric for status without an action or missing action_id

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

// Read both streams at once and merge them together
val actionTopic = "insights-actions,action-status"
val actionLogTable = "action_stream"

// COMMAND ----------

// DBTITLE 1,Format Stream
val actionSchema = new StructType()
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
  .add("organizationName", StringType)
  .add("orgId", LongType)
  .add("waveId", StringType)
// not used
  .add("ruleOutcome", BooleanType)

// action status fields
  .add("action_id", StringType)
  .add("org_id", StringType)
  .add("output", StringType)
  .add("timestamp", StringType)
  .add("status", StringType)

// placeholders for action status
  .add("statusDescription", StringType)
  .add("statusDetail", StringType)
  .add("status_detail", StringType)
  .add("statusUpdatedDateUtc", StringType)

// Values subdoc
  .add("values", new StructType()
       .add("actionGuardKey", StringType)
       .add("actionId", StringType)
       .add("actionName", StringType)
       .add("actionTargetConnectionId", StringType)
       .add("actionTargetConnectionName", StringType)
       .add("actionTargetIntegrationType", StringType)
       .add("actionTargetIntegrationConnectionId", StringType)
       .add("actionTriggerEntityConnectionId", StringType)
       .add("actionTriggerEntityConnectionName", StringType)
       .add("actionTriggerEntityId", StringType)
       .add("actionTriggerEntityIntegrationType", StringType)
       .add("actionTriggerEntityType", StringType)
       .add("milestone", StringType)
       .add("payload", StringType)
       .add("ruleId", StringType)
       .add("ruleName", StringType)
       .add("sendActions", BooleanType)
       .add("travelerGroup", ShortType)
       .add("travelerLeadConnectionId", StringType)
       .add("travelerLeadConnectionName", StringType)
       .add("travelerLeadEntityId", StringType)
       .add("travelerLeadEntityType", StringType)
       .add("travelerLeadIntegrationType", StringType)
       .add("travelerOutcome", StringType)
       .add("travelerStatus", StringType)
       .add("milestoneId", StringType)
      )

def extractActionKafkaStream(df:DataFrame) : DataFrame = {
  df.select($"topic", $"partition", $"offset", $"timestamp", from_json($"value".cast("string"), actionSchema).alias("data"))
}

def formatActionStream(df:DataFrame) : DataFrame = {
  val formattedDf = df.select("data.*")
     .select($"*", $"values.*")
     .drop($"values")

  val convertedTimestamp = convertStringToTimestamp(formattedDf, "timestamp")
  val convertedCreationDate = convertStringToTimestamp(convertedTimestamp, "creationDateUtc")
  val convertedCustomerDate = convertStringToTimestamp(convertedCreationDate, "customerDateUtc")
  val convertedStatusUpdatedDate = convertStringToTimestamp(convertedCustomerDate, "statusUpdatedDateUtc")

  // add a partition column
  convertedStatusUpdatedDate.withColumn("creationDate",col("creationDateUtc").cast(DateType))
      // Convert to Long
     .withColumn("org_id2", col("org_id").cast(LongType))
     .drop("org_id")
     .withColumnRenamed("org_id2", "org_id")
     .withColumn("statusDetailTemp", when(col("status_detail").isNotNull, col("status_detail")).otherwise(col("statusDetail")))
     .drop("status_detail")
     .drop("statusDetail")
     .withColumnRenamed("statusDetailTemp", "statusDetail")
}


// COMMAND ----------

def createActionTable(orgId:Long, dfUpdates:DataFrame) : Unit = {
  println("Creating Action Table for " + orgId)
  
  spark.sql(s"CREATE DATABASE IF NOT EXISTS org_$orgId")
  
  val path = pathForTable(orgId, 3)
  createDeltaTable(dfUpdates, path)
  addTableToUI(orgId, "action", path)
}

def checkAndAddNewColumns(orgId:Long) : Unit = {
  val columns_to_add = List(List("statusDetail", "String"), List("sessionId", "String"), List("milestoneId", "String"))
  for (columnTypeTuple <- columns_to_add) {
    if (!hasColumnForTable(orgId, "action", columnTypeTuple(0))) {
      println(s"Add Column $columnTypeTuple(0) of type $columnTypeTuple(1) in Action Table for " + orgId)
      addColumnToTable(orgId, "action", columnTypeTuple(0), columnTypeTuple(1))
    }
  }
}

// Function to fill SessionID column if ObjectID is not null. This function is useful as Crucible switches over from writing ObjectId to SessionId in the Uberstream.
def moveObjectIdToSessionId(df: DataFrame) : DataFrame = {
  println("Moving object ID values to sessionID")
  val newDf = df.withColumn("sessionId", when($"sessionId".isNull && !lit($"sessionId").equals(""), $"objectId").otherwise($"sessionId"))
  newDf.show()
  return newDf
}

def addNewColumnsToActionLogTable(table: String): Unit = {
  val sessionIdCommand = s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.sessionId String AFTER objectId);"
  val dataCommand = s"ALTER TABLE streaming_logs.$table ADD COLUMNS (data.values.milestoneId String AFTER travelerStatus);"
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

import scala.collection.convert.decorateAsScala._

def startActionStream(dfStream : DataFrame) : StreamingQuery = {
  dfStream
    .writeStream
    .foreachBatch ( (df: DataFrame, batchId: Long) => {
      //augment stream data and create a view for logging
      val current_time = current_timestamp()
      
      df.withColumn("batchId", lit(batchId))
          .withColumn("processingTimestamp", lit(current_time))
          .withColumn("processingDate", to_date(lit(current_time),"yyyy-MM-dd"))
          .createOrReplaceTempView("actionStreamContent")
      val dfFormatted = formatActionStream(df)
      val dfFormattedWithSessionId = moveObjectIdToSessionId(dfFormatted)
      dfFormattedWithSessionId.createOrReplaceTempView("actionUpdates")
      
      // Process actions. insights_actions has orgId, action_status has org_id
      df.sparkSession.sql(s"""SELECT DISTINCT orgId FROM actionUpdates WHERE orgId IS NOT NULL """)
        .collect()
        .map(_(0).asInstanceOf[Long])
        .toList
        .foreach(orgId => {
          println(s"Processing orgId $orgId")
          val dfUpdates = df.sparkSession.sql(s"""
            SELECT 
              actionGuardKey, actionId, actionName, actionTargetConnectionId, actionTargetConnectionName,
              actionTargetIntegrationType, actionTargetIntegrationConnectionId , 
              actionTriggerEntityConnectionId, actionTriggerEntityConnectionName,
              actionTriggerEntityId, actionTriggerEntityIntegrationType, actionTriggerEntityType,
              creationDateUtc, customerDateUtc,
              executionType, id, journeyId, journeyName,
              milestone, namespace, objectId, payload,
              ruleId, ruleName, ruleOutcome, sendActions,
              status, statusDescription, statusDetail, statusUpdatedDateUtc,
              travelerLeadConnectionId, travelerLeadConnectionName,
              travelerLeadEntityId, travelerLeadEntityType, travelerLeadIntegrationType,
              travelerGroup, travelerOutcome, travelerStatus,
              waveId, jobId, orgId,
              creationDate
            FROM
            (
              SELECT a.*, ROW_NUMBER() OVER (PARTITION BY actionId, namespace, creationDate ORDER BY creationDateUtc DESC) rn
              FROM actionUpdates a
              WHERE a.orgId = $orgId
              AND a.status IS NULL
              AND actionId IS NOT NULL AND actionId != ""
            ) T
            WHERE rn = 1
            """)
          dfUpdates.createOrReplaceTempView("actionUpdatesByOrg")
          if (!hasTable(orgId, "action")) {
            createActionTable(orgId, dfUpdates)
          } else {
            checkAndAddNewColumns(orgId)
            // force refresh before merge	
            df.sparkSession.sql(s"""REFRESH TABLE org_$orgId.usermind_action""")
            
            df.sparkSession.sql(s"""SELECT DISTINCT namespace FROM actionUpdates WHERE orgId=$orgId""")
            .collect()
                .map(_(0).asInstanceOf[String])
                .toList
                .foreach(namespace => {
                  val dfActions = df.sparkSession.sql(s"""
                    SELECT *
                    FROM actionUpdates 
                    WHERE orgId = $orgId AND namespace = '$namespace'
                    """)
                  dfActions.createOrReplaceTempView("actionUpdatesByOrgAndNamespace")   
                  
                val mergeSQL = s"""
                 MERGE INTO org_$orgId.usermind_action T
                 USING actionUpdatesByOrgAndNamespace U
                   ON T.namespace = '$namespace' AND T.actionId = U.actionId AND T.orgId = U.orgId AND T.namespace = U.namespace
                 WHEN MATCHED THEN UPDATE SET 
                    actionGuardKey = U.actionGuardKey,
                    actionId = U.actionId,
                    actionName = U.actionName,
                    actionTargetConnectionId = U.actionTargetConnectionId,
                    actionTargetConnectionName = U.actionTargetConnectionName,
                    actionTargetIntegrationType = U.actionTargetIntegrationType,
                    actionTargetIntegrationConnectionId = U.actionTargetIntegrationConnectionId,
                    actionTriggerEntityConnectionId = U.actionTriggerEntityConnectionId,
                    actionTriggerEntityConnectionName = U.actionTriggerEntityConnectionName,
                    actionTriggerEntityId = U.actionTriggerEntityId,
                    actionTriggerEntityIntegrationType = U.actionTriggerEntityIntegrationType,
                    actionTriggerEntityType = U.actionTriggerEntityType,
                    creationDateUtc = U.creationDateUtc,
                    customerDateUtc = U.customerDateUtc,
                    executionType = U.executionType,
                    id = U.id,
                    journeyId = U.journeyId,
                    journeyName = U.journeyName,
                    milestone = U.milestone,
                    namespace = U.namespace,
                    objectId = U.objectId,
                    payload = U.payload,
                    ruleId = U.ruleId,
                    ruleName = U.ruleName,
                    ruleOutcome = U.ruleOutcome,
                    sendActions = U.sendActions,
                    travelerLeadConnectionId = U.travelerLeadConnectionId,
                    travelerLeadConnectionName = U.travelerLeadConnectionName,
                    travelerLeadEntityId = U.travelerLeadEntityId,
                    travelerLeadEntityType = U.travelerLeadEntityType,
                    travelerLeadIntegrationType = U.travelerLeadIntegrationType,
                    travelerGroup = U.travelerGroup,
                    travelerOutcome = U.travelerOutcome,
                    travelerStatus = U.travelerStatus,
                    waveId = U.waveId,
                    jobId = U.jobId,
                    orgId = U.orgId,
                    creationDate = U.creationDate
                 WHEN NOT MATCHED THEN INSERT *  """
                df.sparkSession.sql(mergeSQL) 
            })
          }
        })
      
      // process action status
      df.sparkSession.sql(s"""SELECT DISTINCT org_id FROM actionUpdates WHERE org_id IS NOT NULL """)
        .collect()
        .map(_(0).asInstanceOf[Long])
        .toList
        .filter(orgId => hasTable(orgId, "action")) // not creating a table based on action status
        .filter(orgId => hasColumnForTable(orgId, "action", "statusDetail")) // not creating new columns based on action status
        .foreach(orgId => {
          println(s"Updating statuses for org $orgId")
          // just take the latest record if there are dupes or MERGE will complain
          df.sparkSession.sql(s"""
            SELECT DISTINCT org_id AS orgId, status, statusDetail, action_id AS actionId, output, timestamp, namespace
            FROM 
            (
              SELECT a.*, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY timestamp DESC) rn
              FROM actionUpdates a
              WHERE a.org_id = $orgId
              AND a.status IS NOT NULL
              AND action_id IS NOT NULL AND action_id != ""
            ) T
            WHERE rn = 1
            """)
            .createOrReplaceTempView("actionStatusUpdatesByOrg")

          // force refresh before merge	
          df.sparkSession.sql(s"""REFRESH TABLE org_$orgId.usermind_action""")
          
          df.sparkSession.sql(s"""SELECT DISTINCT namespace FROM actionStatusUpdatesByOrg""")
                .collect()
                .map(_(0).asInstanceOf[String])
                .toList
                .foreach(namespace => {
              df.sparkSession.sql(s"""SELECT * FROM actionStatusUpdatesByOrg WHERE namespace = '$namespace'""")
                  .createOrReplaceTempView("actionStatusUpdatesByOrgAndNamespace")
              val mergeSQL = s"""
               MERGE INTO org_$orgId.usermind_action T
               USING actionStatusUpdatesByOrgAndNamespace S
                 ON T.namespace = '$namespace' AND T.actionId = S.actionId AND T.orgId = S.orgId
               WHEN MATCHED THEN UPDATE 
                 SET status = S.status,
                     statusDescription = S.output,
                     statusDetail = S.statusDetail,
                     statusUpdatedDateUtc = S.timestamp
               WHEN NOT MATCHED THEN 
                 INSERT (
                    orgId,
                    status,
                    actionId, 
                    statusDescription,
                    statusDetail,
                    statusUpdatedDateUtc,
                    creationDateUtc,
                    creationDate,   
                    namespace,
                    actionGuardKey,
                    actionName,
                    actionTargetConnectionId,
                    actionTargetConnectionName,
                    actionTargetIntegrationConnectionId,
                    actionTargetIntegrationType,
                    actionTriggerEntityConnectionId,
                    actionTriggerEntityConnectionName,
                    actionTriggerEntityId,
                    actionTriggerEntityIntegrationType,
                    actionTriggerEntityType,
                    customerDateUtc,
                    executionType,
                    id,
                    jobId,
                    journeyId,
                    journeyName,
                    milestone,
                    objectId,
                    payload,
                    ruleId,
                    ruleName,
                    ruleOutcome,
                    sendActions,
                    travelerGroup,
                    travelerLeadConnectionId,
                    travelerLeadConnectionName,
                    travelerLeadEntityId,
                    travelerLeadEntityType,
                    travelerLeadIntegrationType,
                    travelerOutcome,
                    travelerStatus,
                    waveId)
                 VALUES (
                    S.orgId,
                    S.status,
                    S.actionId,
                    S.output,
                    S.statusDetail,
                    S.timestamp,
                    S.timestamp,
                    CAST(S.timestamp as DATE), 
                    'namespace unknown',             
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)            
               """
              df.sparkSession.sql(mergeSQL)
           })      
        })
        if(!tableExists(logDatabase + "." + actionLogTable)){
            df.sparkSession.sql(s"""CREATE TABLE $logDatabase.$actionLogTable PARTITIONED BY (processingDate) AS SELECT * FROM actionStreamContent""")    
        } else {
            df.sparkSession.sql(s"""INSERT INTO $logDatabase.$actionLogTable SELECT * FROM actionStreamContent""")         
        }
      () // return Unit otherwise it will crash, weird scala 2.12 issue
      })
    .trigger(Trigger.ProcessingTime(INTERVAL)) // without this it starts when the last batch ends
    .start()
}

// COMMAND ----------


