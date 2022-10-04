# Databricks notebook source
# MAGIC %run ../Includes/module-1/setup-lesson-1.02

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE no_part_table
# MAGIC LOCATION "${da.paths.working_dir}/no_part_table"
# MAGIC AS SELECT * FROM raw_data

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/no_part_table/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.json(f"{DA.paths.working_dir}/no_part_table/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Partitioning Delta Table 

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE date_part_table (
      key STRING,
      value BINARY,
      topic STRING,
      partition LONG,
      offset LONG,
      timestamp LONG,
      p_date DATE GENERATED ALWAYS AS (CAST(CAST(timestamp/1000 AS timestamp) AS DATE))
    )
    PARTITIONED BY (p_date)
    LOCATION '${da.paths.working_dir}/date_part_table'
""")

(spark.table("raw_data")
      .write.mode("append")
      .saveAsTable("date_part_table"))

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/date_part_table")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC When running a query that filters data on a column used for partitioning, partitions not matching a conditional statement will be skipped entirely. Delta Lake also have several operations (including **`OPTIMIZE`** commands) that can be applied at the partition level.
# MAGIC 
# MAGIC Note that because data files will be separated into different directories based on partition values, files cannot be combined or compacted across these partition boundaries. Depending on the size of data in a given table, the "right size" for a partition will vary, but if most partitions in a table will not contain at least 1GB of data, the table is likely over-partitioned, which will lead to slowdowns for most general queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p_date, COUNT(*) FROM date_part_table GROUP BY p_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Computing Stats
# MAGIC 
# MAGIC Users can <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-analyze-table.html" target="_blank">manually specify relational entities for which statistics should be calculated with **`ANALYZE`**</a>. While analyzing a table or a subset of columns for a table is not equivalent to indexing, it can allow the query optimizer to select more efficient plans for operations such as joins.
# MAGIC 
# MAGIC Statistics can be collected for all tables in a database, a specific table, a partition of a table, or a subset of columns in a table.
# MAGIC 
# MAGIC Below, statistics are computed for the **`timestamp`** column.

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE no_part_table COMPUTE STATISTICS FOR COLUMNS timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED no_part_table timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Compaction
# MAGIC Delta Lake supports the **`OPTIMIZE`** operation, which performs file compaction. The <a href="https://docs.databricks.com/delta/optimizations/file-mgmt.html#autotune-based-on-table-size" target="_blank">target file size can be auto-tuned</a> by Databricks, and is typically between 256 MB and 1 GB depending on overall table size.
# MAGIC 
# MAGIC Note that data files cannot be combined across partitions. As such, some tables will benefits from not using partitions to minimize storage costs and total number of files to scan.
# MAGIC 
# MAGIC **NOTE**: Optimization schedules will vary depending on the nature of the data and how it will be used downstream. Optimization can be scheduled for off-hours to reduce competition for resources with important workloads. Delta Live Tables has added functionality to automatically optimize tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Order Indexing
# MAGIC 
# MAGIC Z-order indexing is a technique to collocate related information in the same set of files. This co-locality is automatically used by Databricks data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Don't worry about <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">the math</a> (tl;dr: Z-order maps multidimensional data to one dimension while preserving locality of the data points).
# MAGIC 
# MAGIC Multiple columns can be used for Z-ordering, but the algorithm loses some efficiency with each additional column. The best columns for Z-order are high cardinality columns that will be used commonly in queries.
# MAGIC 
# MAGIC Z-order must be executed at the same time as **`OPTIMIZE`**, as it requires rewriting data files.
# MAGIC 
# MAGIC Below is the code to Z-order and optimize the **`date_part_table`** by **`timestamp`** (this might be useful for regular queries within granular time ranges).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE date_part_table
# MAGIC ZORDER BY (timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY date_part_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bloom Filter Indexes
# MAGIC 
# MAGIC While Z-order provides useful data clustering for high cardinality data, it's often most effective when working with queries that filter against continuous numeric variables.
# MAGIC 
# MAGIC Bloom filters provide an efficient algorithm for probabilistically identifying files that may contain data using fields containing arbitrary text. Appropriate fields would include hashed values, alphanumeric codes, or free-form text fields.
# MAGIC 
# MAGIC Bloom filters calculate indexes that indicate the likelihood a given value **could** be in a file; the size of the calculated index will vary based on the number of unique values present in the field being indexed and the configured tolerance for false positives.
# MAGIC 
# MAGIC **NOTE**: A false positive would be a file that the index thinks could have a matching record but does not. Files containing data matching a selective filter will never be skipped; false positives just mean that extra time was spent scanning files without matching records.
# MAGIC 
# MAGIC Looking at the distribution for the **`key`** field, this is an ideal candidate for this technique.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key, count(*) FROM no_part_table GROUP BY key ORDER BY count(*) ASC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from date_part_table where key=43810

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE date_part_table
# MAGIC FOR COLUMNS(key OPTIONS (fpp=0.1, numItems=200))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from date_part_table where key=43810

# COMMAND ----------

# DBTITLE 1,Lab Cleanup
DA.cleanup()
