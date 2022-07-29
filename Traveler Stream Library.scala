// Databricks notebook source
// DBTITLE 1,Traveler Event Stream Library
// MAGIC %md
// MAGIC 
// MAGIC ## Usage Notes
// MAGIC - Usage
// MAGIC 
// MAGIC ## TODO
// MAGIC - Drop Rule fields (`ruleId`, `ruleName`, `ruleOutcome`), `sendActions`
// MAGIC - Need to check for duplicate updates?
// MAGIC - What makes a unique event? Id or another field?

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

val travelerTopic = "insights-travelers"
val travelerLogTable = "traveler_stream"

// COMMAND ----------

// DBTITLE 1,Format Stream
val travelerSchema = new StructType()
  .add("channelName", StringType)
  .add("creationDateUtc", StringType)
  .add("customerDateUtc", StringType)
  .add("entityName", StringType)
  .add("eventSubtype", StringType)
  .add("eventType", StringType)
  .add("executionType", StringType)
  .add("id", StringType)
  .add("jobId", StringType)
  .add("journeyId", StringType)
  .add("journeyName", StringType)
  .add("namespace", StringType)
  .add("objectId", StringType)
  .add("organizationName", StringType)
  .add("orgId", LongType)
  .add("schemaId", StringType)
  .add("sourceName", StringType)
  .add("waveId", StringType)

// not used
  .add("ruleId", LongType)
  .add("ruleName", StringType)
  .add("ruleOutcome", BooleanType)
  .add("sendActions", BooleanType)

// Values subdoc
  .add("values", new StructType()
       .add("milestone", StringType)
       .add("travelerGroup", LongType)
       .add("travelerLeadConnectionId", StringType)
       .add("travelerLeadConnectionName", StringType)
       .add("travelerLeadEntityId", StringType)
       .add("travelerLeadEntityType", StringType)
       .add("travelerLeadIntegrationType", StringType)
       .add("travelerOutcome", StringType)
       .add("travelerStatus", StringType)
     )

def extractTravelerKafkaStream(df:DataFrame) : DataFrame = {
  df.select($"topic", $"partition", $"offset", $"timestamp", from_json($"value".cast("string"), travelerSchema).alias("data"))
}

def formatTravelerStream(df:DataFrame) : DataFrame = {
  val formattedDf = df.select("data.*")
    .select($"*", $"values.*")
    .drop($"values")
  val convertedCreationDate = convertStringToTimestamp(formattedDf, "creationDateUtc")
  val convertedCustomerDate = convertStringToTimestamp(convertedCreationDate, "customerDateUtc")
  convertedCustomerDate.withColumn("creationDate", col("creationDateUtc").cast(DateType))
}

// COMMAND ----------

// DBTITLE 1,Create Table
def createTravelerTable(orgId:Long, dfUpdates: DataFrame) : Unit = {
  println("Creating Traveler Table for " + orgId)
  
  spark.sql(s"CREATE DATABASE IF NOT EXISTS org_$orgId")
  
  val path = pathForTable(orgId, 1)
  createDeltaTable(dfUpdates, path)
  addTableToUI(orgId, "travelerevent", path)
}

// COMMAND ----------

// DBTITLE 1,Start Traveler Stream
import scala.collection.convert.decorateAsScala._

def startTravelerStream(dfStream : DataFrame) : StreamingQuery = {
  dfStream
    .writeStream
    .foreachBatch ( (df: DataFrame, batchId: Long) => {
      //augment stream data and create a view for logging
       val current_time = current_timestamp()
      
      df.withColumn("batchId", lit(batchId))
          .withColumn("processingTimestamp", lit(current_time))
          .withColumn("processingDate", to_date(lit(current_time),"yyyy-MM-dd"))
          .createOrReplaceTempView("travelerStreamContent")
      
      //format stream to just contain traveleevent data
      val dfFormatted = formatTravelerStream(df)
      dfFormatted.createOrReplaceTempView("travelerUpdates")
      
      //branch by org
      df.sparkSession.sql(s"""SELECT DISTINCT orgId FROM travelerUpdates """)
        .collect()
        .map(_(0).asInstanceOf[Long])
        .toList
        .foreach(orgId => {
            if(!hasTable(orgId, "travelerevent")){
              //create travelerevent table for new org
              val dfByOrg = df.sparkSession.sql(s"""SELECT * FROM travelerUpdates WHERE orgId = $orgId""")
               createTravelerTable(orgId, dfByOrg)
            } else {
              //go through all namespaces and update one by one to improve performance
              df.sparkSession.sql(s"""REFRESH TABLE org_$orgId.usermind_travelerevent""")
            
              df.sparkSession.sql(s"""SELECT DISTINCT namespace FROM travelerUpdates WHERE orgId=$orgId""")
                .collect()
                .map(_(0).asInstanceOf[String])
                .toList
                .foreach(namespace => {
                  val dfUpdates = df.sparkSession.sql(s"""
                    SELECT *
                    FROM travelerUpdates 
                    WHERE orgId = $orgId AND namespace = '$namespace'
                    """)
                  dfUpdates.createOrReplaceTempView("travelerUpdatesByOrgAndNamespace")       

                  val mergeSQL = s"""
                        MERGE INTO org_$orgId.usermind_travelerevent T
                        USING travelerUpdatesByOrgAndNamespace U
                          ON T.namespace = '$namespace' AND T.id = U.id AND T.orgId = U.orgId AND T.namespace = U.namespace AND T.creationDate = U.creationDate
                        WHEN MATCHED THEN UPDATE SET *    
                        WHEN NOT MATCHED THEN INSERT *  """

                  df.sparkSession.sql(mergeSQL)
              })
            }         
        })
        //commit processed records so we can resume later
        if(!tableExists(logDatabase + "." + travelerLogTable)){
            df.sparkSession.sql(s"""CREATE TABLE $logDatabase.$travelerLogTable PARTITIONED BY (processingDate) AS SELECT * FROM travelerStreamContent""")    
        } else {
            df.sparkSession.sql(s"""INSERT INTO $logDatabase.$travelerLogTable SELECT * FROM travelerStreamContent""")         
        }
      () // return Unit otherwise it will crash, weird scala 2.12 issue
    })
    .trigger(Trigger.ProcessingTime(INTERVAL)) // without this it starts when the last batch ends
    .start()
}
