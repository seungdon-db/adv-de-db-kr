# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Deduplication
# MAGIC 
# MAGIC In this notebook, you'll learn how to eliminate duplicate records while working with Structured Streaming and Delta Lake. Spark Structured Streaming 은  exactly-once 프로세싱 보장을 지원하지만 많은 소스 시스템들에서 발생하는 중복 레코드들은 다운스트림 쿼리에서 올바른 결과값을 얻기 위해서는 중복 제거가 필요하게 됩니다. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Apply **`dropDuplicates`** to streaming data
# MAGIC - Use watermarking to manage state information
# MAGIC - Write an insert-only merge to prevent inserting duplicate records into a Delta table
# MAGIC - Use **`foreachBatch`** to perform a streaming upsert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Declare database and set all path variables.

# COMMAND ----------

# MAGIC %run ../Includes/module-2/setup-lesson-2.05-silver-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Duplicate Records
# MAGIC 
# MAGIC Kafka는 데이터 전송에 있어서 at-least-once 보장을 지원하므로, kakfa consumer입장에서는 중복 record에 대한 처리를 고려해야 한다.  
# MAGIC Because Kafka provides at-least-once guarantees on data delivery, all Kafka consumers should be prepared to handle duplicate records.
# MAGIC 
# MAGIC The de-duplication methods shown here can also be applied when necessary in other parts of your Delta Lake applications.
# MAGIC 
# MAGIC Let's start by identifying the number of duplicate records in our **`bpm`** topic of the bronze table.

# COMMAND ----------

total = (spark
    .read
    .table("bronze")
    .filter("topic = 'bpm'")
    .count())

print(f"Total: {total:,}")

# COMMAND ----------

from pyspark.sql import functions as F

old_total = (spark
    .read
    .table("bronze")
    .filter("topic = 'bpm'")
    .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
    .select("v.*")
    .dropDuplicates(["device_id", "time"])
    .count())

print(f"Old Total: {old_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC 약 10~20%는 중복이 있는 것으로 보인다. 여기서 우리는 bronze 레벨보다는 silver level 에 dedup을 적용할 것이다. 우리가 일부 중복 레코드를 저장하고 있지만, bronze 테이블은 streaming source의 추가적인 메타데이터와 함께 도착한 데이터들의  전체 기록을 가진다. 이를 통해 다운스트림 시스템에서 필요한데로 재생성하게 되며, 데이터 수집에 대한 latency를 최소화함과 동시에 지나치게 데이터에 대한 품질 관리를 강화해서 야기될 수 있는 데이터 유실에 대한 문제를 방지할 수 있다. 
# MAGIC 
# MAGIC It appears that around 10-20% of our records are duplicates. Note that here we're choosing to apply deduplication at the silver rather than the bronze level. While we are storing some duplicate records, our bronze table retains a history of the true state of our streaming source, presenting all records as they arrived (with some additional metadata recorded). This allows us to recreate any state of our downstream system, if necessary, and prevents potential data loss due to overly aggressive quality enforcement at the initial ingestion as well as minimizing latencies for data ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read on the Bronze BPM Records
# MAGIC 
# MAGIC Here we'll bring back in our final logic from our last notebook.

# COMMAND ----------

bpmDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Streaming dedup을 다루기 위해서는 일반적인 static한 데이터와 비교해서 좀 더 복잡한 고려사항들이 있다. 
# MAGIC 각 마이크로배치마다 우리는 
# MAGIC - 마이크로배치내에 중복 데이터가 없음을 확인
# MAGIC - 새로 insert되는 레코드가 타겟 테이블상에 없음을 확인 
# MAGIC 
# MAGIC 해야 한다. Spark Structured Streaming에서는 unique key에 대한 상태 정보를 트래킹해서 마이크로 배치내/배치간에 중복 데이터가 없음을 보장한다. 시간이 지남에 따라 이 상태 정보는 전체 정보만큼 커지게 되므로, 적당한 기간의 watermark를 적용하면  레코드들이 늦게 도착할 수 있는 시간 윈도우에 대해서만 상태 정보를 트래킹해도 된다. 여기서는 워터마크를 30초로 정의한다. 
# MAGIC 
# MAGIC 
# MAGIC When dealing with streaming deduplication, there is a level of complexity compared to static data.
# MAGIC 
# MAGIC As each micro-batch is processed, we need to ensure:
# MAGIC - No duplicate records exist in the microbatch
# MAGIC - Records to be inserted are not already in the target table
# MAGIC 
# MAGIC Spark Structured Streaming can track state information for the unique keys to ensure that duplicate records do not exist within or between microbatches. Over time, this state information will scale to represent all history. Applying a watermark of appropriate duration allows us to only track state information for a window of time in which we reasonably expect records could be delayed. Here, we'll define that watermark as 30 seconds.
# MAGIC 
# MAGIC The cell below updates our previous query.

# COMMAND ----------

dedupedDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
  .withWatermark("time", "30 seconds")
  .dropDuplicates(["device_id", "time"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Only Merge
# MAGIC 
# MAGIC Delta Lake는 인서트만 되는 머지에 최적화된 함수를 가지고 있다. 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Delta Lake has optimized functionality for insert-only merges. This operation is ideal for de-duplication: define logic to match on unique keys, and only insert those records for keys that don't already exist.
# MAGIC 
# MAGIC Note that in this application, we proceed in this fashion because we know two records with the same matching keys represent the same information. If the later arriving records indicated a necessary change to an existing record, we would need to change our logic to include a **`WHEN MATCHED`** clause.
# MAGIC 
# MAGIC A merge into query is defined in SQL below against a view titled **`stream_updates`**.

# COMMAND ----------

sql_query = """
  MERGE INTO heart_rate_silver a
  USING stream_updates b
  ON a.device_id=b.device_id AND a.time=b.time
  WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Microbatch Function for **`foreachBatch`**
# MAGIC 
# MAGIC The Spark Structured Streaming **`foreachBatch`** method allows users to define custom logic when writing.
# MAGIC 
# MAGIC The logic applied during **`foreachBatch`** addresses the present microbatch as if it were a batch (rather than streaming) data.
# MAGIC 
# MAGIC The class defined in the following cell defines simple logic that will allow us to register any SQL **`MERGE INTO`** query for use in a Structured Streaming write.

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we're using SQL to write to our Delta table, we'll need to make sure this table exists before we begin.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS heart_rate_silver;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS heart_rate_silver 
# MAGIC (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC Now pass the previously define **`sql_query`** to the **`Upsert`** class.

# COMMAND ----------

streamingMerge=Upsert(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC And then use this class in our **`foreachBatch`** logic.

# COMMAND ----------

query = (dedupedDF
    .writeStream
   .foreachBatch(streamingMerge.upsertToDelta)
   .outputMode("update")
   .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings.chk")
   .trigger(once=True)
   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our number of unique entries that have been processed to the **`heart_rate_silver`** table matches our batch de-duplication query from above.

# COMMAND ----------

new_total = spark.read.table("heart_rate_silver").count()

print(f"Old Total: {old_total:,}")
print(f"New Total: {new_total:,}")

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
