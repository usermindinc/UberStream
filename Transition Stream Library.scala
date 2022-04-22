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
       .add("transitionTime", StringType)
       .add("actionsGenerated", StringType)
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
          if(!hasTable(orgId, "transition")){
            val dfByOrg = df.sparkSession.sql(s"""SELECT * FROM transitionUpdates WHERE orgId = $orgId""")
            createTransitionTable(orgId, dfFormatted)
          } else {
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
