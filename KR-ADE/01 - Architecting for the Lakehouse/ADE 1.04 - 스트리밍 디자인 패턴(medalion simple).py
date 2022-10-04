# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Design Patterns
# MAGIC Lakehouse는 처음부터 무한하게 커지는 데이터셋을 효율적으로 다루기 위해서 다지인되었습니다. 근실시간(near real-time) 솔루션인 Spark Structured Streaming 과 Delta Lake가 합쳐저서 점진적으로 증가하는 데이터에 대한 배치프로세싱을 손쉽게 변경하고, 데이터 변경분에 대한 tracking 처리작업양를 매우 쉽게 줄여줄 수 있습니다. 
# MAGIC 
# MAGIC 이 노트북에서는 Spark Structured Streaming + Delta Lake를 통한 incremental data pipeline/processing과정의 간편화 방법에 대해서 알아봅시다.  
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, student will be able to:
# MAGIC - Structured Streaming 를 사용해서 간단한 incremental ETL을 구현해 보기
# MAGIC - 여러 테이블 대상으로 incremental write 수행하기
# MAGIC - Incrementally update values in a key value store
# MAGIC - Change Data Capture (CDC) 데이터를  **`MERGE`** 문을 써서 Delta Table 에 넣기
# MAGIC - 두개의 incremental table 조인 
# MAGIC - incremental table과 batch table 조인 

# COMMAND ----------

# DBTITLE 1,설정 
# MAGIC %run ../Includes/module-1/setup-lesson-1.04

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple Incremental ETL
# MAGIC 
# MAGIC 대부분의 조직내에서 발생하는 대량의 데이터 처리는 아주 간단하게 말하면 데이터에 대해서 간단한 변경 작업과 데이터 검증 작업을 수행하면서 원래 위치에서 다른 위치로 데이터를 옮기는 과정입니다. 
# MAGIC 
# MAGIC 대부분의 소스 데이터는 시간이 지남에 따라 지속적으로 증가하므로, 이를 Incremental(또는 Streaming) 데이터라고 부릅니다. 
# MAGIC 
# MAGIC Structured Streaming 와 Delta Lake 는 이러한 incremental ETL 작업을 손쉽게 구현할 수 있게 합니다. 
# MAGIC 
# MAGIC 아래 코드에서 우리는 간단한 테이블을 만들고 값을 넣어 보겠습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE bronze 
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC 
# MAGIC INSERT INTO bronze
# MAGIC VALUES (1, "Yve", 1.0),
# MAGIC        (2, "Omar", 2.5),
# MAGIC        (3, "Elia", 3.3)

# COMMAND ----------

# MAGIC %md
# MAGIC 다음 cell은 앞의 테이블에 대한 Structured Streaming 을 사용해서 incremental 읽기를 수행하면서 언제 해당 record가 처리되었는지에 대한 필드를 추가하고 이를 새로운 테이블로 쓰는 작업을 single batch로 수행하는 코드입니다. 

# COMMAND ----------

from pyspark.sql import functions as F

def update_silver():
    query = (spark.readStream
                  .table("bronze")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
                  .trigger(once=True)
                  .table("silver"))
    
    query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 위 코드는 추가된 변경사항을 적용하기 위해 triggered batch 모드로 데이터를 처리하는 예제이다. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png">
# MAGIC 여기서는 structured stream 데모를 위해서 **`trigger(once=True)`** 와 **`query.awaitTermination()`**  를 써서 프로세싱 진행을 늦게 진행되도록 막는다.<br/>

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC 예상했던데로 stream은 매우 짧은 시간내에 끝나고  **`silver`** 테이블에는 **`bronze`** 에서 기록되었던 모든 사항들이 적용되게 된다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver

# COMMAND ----------

# MAGIC %md
# MAGIC 새로운 레코드를 silver table까지 적용하는 것은  **`bronze`** 에 새로운 레코드를 넣기만 하면 된다.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (4, "Ted", 4.7),
# MAGIC        (5, "Tiffany", 5.5),
# MAGIC        (6, "Vini", 6.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ... 그리고 incremental batch processing code를 재수행

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake 는 여러 테이블 시리즈들에 대해서 추가되는 데이터에 대해 전파시키거나 이 진도를 tracking하는것에 매우 적합한 구조를 가지고 있습니다.  
# MAGIC 이러한 패턴은  "medallion", "multi-hop", "Delta",  "bronze/silver/gold" 아키텍처등의 다양한 이름으로 불리웁니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Multiple Tables
# MAGIC 
# MAGIC Structured Streaming의 **`foreachBatch`** 메소드는 각 마이크로배치 수행시마다 custom data writing 로직을 수행할 수 있는 옵션을 제공합니다.  
# MAGIC 새로운 DBR 기능으로 여러 테이블에 writing작업을 해도 각 write작업은 멱등성(idempotent)을 보장하여, 특히 하나의 레코드에 여러 테이블의 데이터가 포함되어 있을때 유용합니다.   
# MAGIC 아래의 코드는 두개의 새로운 테이블에 레코드들을 추가하는 커스텀 라이터 로직을 보여주며 이를 **`foreachBatch`** 에서 이 함수를 쓰도록 하겠습니다. 

# COMMAND ----------

def write_twice(microBatchDF, batchId):
    appId = "write_twice"
    
    microBatchDF.select("id", "name", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_name")
    
    microBatchDF.select("id", "value", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_value")


def split_stream():
    query = (spark.readStream.table("bronze")
                 .writeStream
                 .foreachBatch(write_twice)
                 .outputMode("update")
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/split_stream")
                 .trigger(once=True)
                 .start())
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC 스트림이 트리거링 될떄  **`write_twice`** 함수내에 포함된 두개의 write 작업은 스파크 배치 문법을 ㅉ따릅니다. This will always be the case for writers called by **`foreachBatch`**.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC 위 셋에서 두 테이블로 최초 데이터가 잘 들어가게 됩니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name

# COMMAND ----------

# MAGIC %md
# MAGIC 각 테이블의 **`processed_time`** 은 약간씩 다릅니다. 위에 정의된 로직은 각 write 가 수행될때의 timestamp 값을 캡쳐받아 수행되게 되며 두 write 작업이 같은 스트리밍 마이크로배치 프로세스에서 수행되나 각각은 완전히 독립적인 트렌젝션임을 보여주기 위한 예제입니다. 
# MAGIC (그러므로, 약간이 비동기 업데이트에 대해서도 다운스트림 앱에서 문제가 없도록 로직이 짜여져야 합니다.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value

# COMMAND ----------

# MAGIC %md
# MAGIC  **`bronze`** 테이블에 더 값을 넣습니다

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (7, "Viktor", 7.4),
# MAGIC        (8, "Hiro", 8.2),
# MAGIC        (9, "Shana", 9.9)

# COMMAND ----------

# MAGIC %md
# MAGIC And we can now pick up these new records and write to two tables.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC 기대했던데로 새롭게 추가된 값들만 두개의 테이블에 들어가게 됩니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Aggregates in a Key-Value Store
# MAGIC 
# MAGIC 증분 집계(Incremental aggregation)는 대시보드상의 지표등을 가장 최신의 서머리 데이터로 업데이트하는 등 여러가지 용도로 유용합니다.  
# MAGIC 아래의 로직은 **`silver`** 테이블에 여러 집계작업을 수행합니다. 

# COMMAND ----------

def update_key_value():
    query = (spark.readStream
                  .table("silver")
                  .groupBy("id")
                  .agg(F.sum("value").alias("total_value"), 
                       F.mean("value").alias("avg_value"),
                       F.count("value").alias("record_count"))
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/key_value")
                  .outputMode("complete")
                  .trigger(once=True)
                  .table("key_value"))
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: 위의 transformation작업은 data shuffle이 필요하므로 우리 스트림 클러스터의 코어수와 맞게 파티션수를 설정하는 것이 보다 효율적인 성능을 위해서 좋습니다.  (If the cluster size will be scaled up for production, the maximum number of cores that will be present in the cluster should be used when configuring this setting.)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value

# COMMAND ----------

# MAGIC %md
# MAGIC **`silver`** 테이블에 더 데이터를 추가해 봅시다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver
# MAGIC VALUES (1, "Yve", 1.0, current_timestamp()),
# MAGIC        (2, "Omar", 2.5, current_timestamp()),
# MAGIC        (3, "Elia", 3.3, current_timestamp()),
# MAGIC        (7, "Viktor", 7.4, current_timestamp()),
# MAGIC        (8, "Hiro", 8.2, current_timestamp()),
# MAGIC        (9, "Shana", 9.9, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 위의 로직은 write 수행시마다 결과 테이블을 overwrite하는 형식으로 되어 있습니다. 다음 세션에서는 **`MERGE`** 문과  **`foreachBatch`** 를 써서 기존의 레코드를 업데이트하는 방법을 살펴보겠습니다. 이 패턴은 key-value stores에도 적용될 수 있습니다.

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Change Data Capture Data
# MAGIC change data capture (CDC) 데이터를 incremental하게 적용하는 예제를 살펴보겠습니다. 
# MAGIC **`bronze_status`** 테이블의 데이터는 CDC 원본 정보입니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_status 
# MAGIC (user_id INT, status STRING, update_type STRING, processed_timestamp TIMESTAMP);
# MAGIC 
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "new", "insert", current_timestamp()),
# MAGIC         (2, "repeat", "update", current_timestamp()),
# MAGIC         (3, "at risk", "update", current_timestamp()),
# MAGIC         (4, "churned", "update", current_timestamp()),
# MAGIC         (5, null, "delete", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC The **`silver_status`** 테이블은 주어진 **`user_id`** 에 대한 현재 **`status`** 를 tracking하기 위한 용도입니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_status (user_id INT, status STRING, updated_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC  **`MERGE`** 명령어를 사용해서 DML타입에 따라 CDC 변경분을 적용합니다. 
# MAGIC 
# MAGIC **`upsert_cdc`** 의 나머지 메소드에는 PySpark DataStreamWriter 에서 마이크로배치로 SQL 쿼리를 수행하기 위한 로직들이 포함되어 있습니다.

# COMMAND ----------

def upsert_cdc(microBatchDF, batchID):
    microBatchDF.createTempView("bronze_batch")
    
    query = """
        MERGE INTO silver_status s
        USING bronze_batch b
        ON b.user_id = s.user_id
        WHEN MATCHED AND b.update_type = "update"
          THEN UPDATE SET user_id=b.user_id, status=b.status, updated_timestamp=b.processed_timestamp
        WHEN MATCHED AND b.update_type = "delete"
          THEN DELETE
        WHEN NOT MATCHED AND b.update_type = "update" OR b.update_type = "insert"
          THEN INSERT (user_id, status, updated_timestamp)
          VALUES (b.user_id, b.status, b.processed_timestamp)
    """
    
    microBatchDF._jdf.sparkSession().sql(query)
    
def streaming_merge():
    query = (spark.readStream
                  .table("bronze_status")
                  .writeStream
                  .foreachBatch(upsert_cdc)
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/silver_status")
                  .outputMode("update")
                  .trigger(once=True)
                  .start())
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC 새로 추가된 데이터에 대해서 incremental하게 프로세싱을 수행합니다. 

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC 새로운 CDC record가 추가되었을때 이 변경사항들을 silver 데이터에 적용합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "repeat", "update", current_timestamp()),
# MAGIC         (2, "at risk", "update", current_timestamp()),
# MAGIC         (3, "churned", "update", current_timestamp()),
# MAGIC         (4, null, "delete", current_timestamp()),
# MAGIC         (6, "new", "insert", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %md
# MAGIC 이 노트북에서는 중복데이터나 순서없이 들어오는 로직처리들이 포함되어 있지 않습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Two Incremental Tables
# MAGIC 
# MAGIC incremental join을 다루기 위해서는 watermark와 windows관련해서 여러가지 복잡한 점이 많아서, 모든 조인 타입이 지원되지는 않습니다. 

# COMMAND ----------

def stream_stream_join():
    nameDF = spark.readStream.table("silver_name")
    valueDF = spark.readStream.table("silver_value")
    
    return (nameDF.join(valueDF, nameDF.id == valueDF.id, "inner")
                  .select(nameDF.id, 
                          nameDF.name, 
                          valueDF.value, 
                          F.current_timestamp().alias("joined_timestamp"))
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/joined")
                  .queryName("joined_streams_query")
                  .table("joined_streams")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC 위의 로직에서는  **`trigger`** option 을 포함하고 있지 않습니다. .
# MAGIC 
# MAGIC 따라서 스트림은 계속 continous execution mode로 수행될 것이며 매 500ms 마다 triggering될것 입니다. 

# COMMAND ----------

query = stream_stream_join()

# COMMAND ----------

# MAGIC %md 
# MAGIC 이것은 또한 데이터가 들어가기 전에 새로운 데이터를 읽을 수 있다는 의미이기도 합니다.   
# MAGIC Stream 은 멈추지 않기 때문에 trigger-once 스트림이 **`awaitTermination()`** 로 종료되기 전까지 멈출 수 없습니다. 대신에 **`query.recentProgress`** 를 사용해서 일부 데이터가 처리될때까지 블록할 수 있습니다. 

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming Table에  **`display()`** 를 써서 개발중에 준실시간 (near-real-time)으로 테이블 업데이트를 확인할 수 있습니다. 

# COMMAND ----------

display(spark.readStream.table("joined_streams"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> 별도의 Stream이 시작되었습니다.   
# MAGIC 1. 원래 파이프라인의 일부로 데이터를 처리하는 Stream과   
# MAGIC 2. 가장 최근의 결과값을 확인하기 위해 **`display()`** 함수를 업데이트하기 위한 stream 

# COMMAND ----------

for stream in spark.streams.active:
    print(stream.name)

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 **`bronze`** 에 값을 추가합니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (10, "Pedro", 10.5),
# MAGIC        (11, "Amelia", 11.5),
# MAGIC        (12, "Diya", 12.3),
# MAGIC        (13, "Li", 13.4),
# MAGIC        (14, "Daiyu", 14.2),
# MAGIC        (15, "Jacques", 15.9)

# COMMAND ----------

# MAGIC %md
# MAGIC Stream-Stream 조인은 The stream-stream join is configured against the tables resulting from the **`split_stream`** function; run this again and data should quickly process through the streaming join running above.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC Interactive streams should always be stopped before leaving a notebook session, as they can keep clusters from timing out and incur unnecessary cloud costs.

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping {stream.name}")
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Incremental and Static Data
# MAGIC 
# MAGIC Incremental 테이블이 계속 데이터를 appending하는 반면, Static 테이블은 보통 변경되거나 덮어 씌워지는 데이터를 담고 있습니다.  
# MAGIC 
# MAGIC Delta Lake가 트렌젝션을 보장하고 캐싱하기 때문에 Databricks는 정적인 테이블과 조인되는 각 스트리밍 데이터의 마이크로배치가 static 테이블의 가장 최신버전을 가지고 작업되는(조인되는) 것을 보장합니다. 

# COMMAND ----------

statusDF = spark.read.table("silver_status")
bronzeDF = spark.readStream.table("bronze")

query = (bronzeDF.alias("bronze")
                 .join(statusDF.alias("status"), bronzeDF.id==statusDF.user_id, "inner")
                 .select("bronze.*", "status.status")
                 .writeStream
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/join_status")
                 .queryName("joined_status_query")
                 .table("joined_status")
)

# COMMAND ----------

# MAGIC %md 더 진행하기 전에 처리할 데이터가 준비될때까지 기다립니다. 

# COMMAND ----------

block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md
# MAGIC **`joined_status`** 에는 스트림 프로세싱이 될때 **`id`** 값이 일치하는 레코드들만 쓰여지게 됩니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC **`silver_status`** 에 새로운 값들을 넣는다고 해도 stream-static 조인의 결과가 자동으로 업데이트 되지는 않습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (11, "repeat", "update", current_timestamp()),
# MAGIC         (12, "at risk", "update", current_timestamp()),
# MAGIC         (16, "new", "insert", current_timestamp()),
# MAGIC         (17, "repeat", "update", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md
# MAGIC 새로운 Only new data appearing on the streaming side of the query will trigger records to process using this pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (16, "Marissa", 1.9),
# MAGIC        (17, "Anne", 2.7)

# COMMAND ----------

# MAGIC %md
# MAGIC The incremental data in a stream-static join "drives" the stream, guaranteeing that each microbatch of data joins with the current values present in the valid version of the static table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md 
# MAGIC 아래 셀을 돌리면 이 노트북에서 사용한 테이블과 파일들을 정리합니다.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
