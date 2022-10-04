# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Data with Auto Loader
# MAGIC 
# MAGIC Databricks Auto Loader 는 클라우드 오브젝트 스토리지에 적재되는 raw data를 streaming할때 권장되는 방법입니다.  Auto Loader 는 클라우드 스토리지에 있는 데이터 파일들에 대한 efficient, incremental, idempotent 한 데이터 프로세싱 방법을 제공합니다.  Several enhancements to this ingestion method make it greatly preferred to directly streaming from the file source using open source Structured Streaming APIs.
# MAGIC 
# MAGIC For small datasets, the default **directory listing** execution mode will provide provide exceptional performance and cost savings. As the size of your data scales, the preferred execution method is **file notification**, which requires configuring a connection to your storage queue service and event notification, which will allow Databricks to idempotently track and process all files as they arrive in your storage account.
# MAGIC 
# MAGIC In this notebook, we'll go through the basic configuration to ingest the log files for device MAC addresses from partner gyms.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_gym_logs.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Use Auto Loader to incrementally, idempotently load data from object storage to Delta Tables
# MAGIC - Locate operation metrics using the **`DESCRIBE HISTORY`** command

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Detection Modes
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/autoloader-detection-modes.png)
# MAGIC 
# MAGIC Auto Loader can be configured with two different file detection modes. While directory listing mode is the default and works well for small datasets and testing, file notification mode is preferred for large scale production applications.
# MAGIC 
# MAGIC | **Directory Listing Mode** | **File Notification Mode** |
# MAGIC | --- | --- |
# MAGIC | Default mode | Requires some security permissions to other cloud services |
# MAGIC | Easily stream files from object storage without configuration | Uses cloud storage queue service and event notifications to track files |
# MAGIC | Creates file queue through parallel listing of input directory | Configurations handled automatically by Databricks |
# MAGIC | Good for smaller source directories | Scales well as data grows |
# MAGIC 
# MAGIC **NOTE**: Only directory listing mode will be shown in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Inference and Evolution
# MAGIC AutoLoader has advanced support for working with data sources with unknown or changing schemas, including the ability to:
# MAGIC * Identify schema on stream initialization
# MAGIC * Auto-detect changes and evolve schema to capture new fields
# MAGIC * Add type hints for enforcement when schema is known
# MAGIC * Rescue data that does not meet expectations
# MAGIC 
# MAGIC Full documentation of this functionality is available <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">here</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC The notebook below defines a function to allow us to manually trigger new data landing in our source container. 
# MAGIC 
# MAGIC This will allow us to see Auto Loader in action.

# COMMAND ----------

# MAGIC %run ../Includes/module-2/setup-lesson-2.02-gym-mac-log-prep

# COMMAND ----------

# MAGIC %md
# MAGIC Our source directory contains a number of JSON files representing about a week of data.

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.gym_mac_logs_json)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's read in a file and grab the schema.

# COMMAND ----------

schema = spark.read.json(files[0].path).schema
schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using CloudFiles
# MAGIC 
# MAGIC Configuring Auto Loader requires using the **`cloudFiles`** format. The syntax for this format differs only slightly from a standard streaming read.
# MAGIC 
# MAGIC All we need to do is replace our file format with **`cloudFiles`**, and add the file type as a string for the option **`cloudFiles.format`**.
# MAGIC 
# MAGIC Additional <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html#configuration" target="_blank">configuration options</a> are available.

# COMMAND ----------

def load_gym_logs():
    query = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # .option("cloudFiles.useNotifications","true") # Set this option for file notification mode
        .schema(schema)
        .load(DA.paths.gym_mac_logs_json)
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/gym_mac_logs.chk")
        .trigger(once=True)
        .table("gym_mac_logs")
        .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC 여기서는 배치 수행을 위해서 trigger once 모드를 사용했음.  While we may not have the latency requirements of a Structured Streaming workload, Auto Loader prevents any CDC on our file source, allowing us to simply trigger a cron job daily to process all new data that's arrived.

# COMMAND ----------

load_gym_logs()

# COMMAND ----------

# MAGIC %md
# MAGIC As always, each batch of newly processed data will create a new version of our table.

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY gym_mac_logs

# COMMAND ----------

# MAGIC %md
# MAGIC The helper method below will load an additional day of data.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC Cloud Files will ignore previously processed data; only those newly written files will be processed.

# COMMAND ----------

load_gym_logs()

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY gym_mac_logs

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to process the remainder of the data provided.

# COMMAND ----------

DA.data_factory.load(continuous=True)
load_gym_logs()

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY gym_mac_logs

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use SQL to examine our data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gym_mac_logs

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
