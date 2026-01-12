-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Engineering / Platform Review
-- MAGIC
-- MAGIC This notebook is to show the following Delta Lake features:
-- MAGIC * micropartitioning, 
-- MAGIC * compaction, 
-- MAGIC * clustering, 
-- MAGIC * time travel,  
-- MAGIC * ACID compliance, 
-- MAGIC * history/retention + VACUUM, and 
-- MAGIC * quick transaction-log peeking.
-- MAGIC
-- MAGIC For more info, check out the Comprehensive Guide to Optimize Databricks, Spark and Delta Lake Workloads ([link](https://www.databricks.com/discover/pages/optimize-data-workloads-guide)).

-- COMMAND ----------

-- MAGIC %run ./Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic profiling query

-- COMMAND ----------

USE CATALOG main_jcg;
USE SCHEMA default;

-- COMMAND ----------

SELECT property_state, delinquency_status, COUNT(*) AS loans, AVG(interest_rate) AS avg_rate
FROM main_jcg.default.mortgage_loans_delta
GROUP BY property_state, delinquency_status
ORDER BY loans DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Show history for time travel demo

-- COMMAND ----------

DESCRIBE HISTORY main_jcg.default.mortgage_loans_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time travel: view table as of an earlier version (e.g., version 0 or 1)

-- COMMAND ----------

SELECT COUNT(*) AS loans_v1
FROM main_jcg.default.mortgage_loans_delta VERSION AS OF 1;

-- COMMAND ----------

SELECT property_state, delinquency_status, COUNT(*) AS loans_v3
FROM main_jcg.default.mortgage_loans_delta VERSION AS OF 3
GROUP BY property_state, delinquency_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Demonstrate ACID: run UPDATE / DELETE and then show consistent reads.

-- COMMAND ----------

UPDATE main_jcg.default.mortgage_loans_delta
SET delinquency_status = 'Current'
WHERE property_state = 'CA' AND delinquency_status = '30';

DELETE FROM main_jcg.default.mortgage_loans_delta
WHERE delinquency_status = 'REO';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compaction and clustering optimization (OPTIMIZE rewrites many small files into fewer large ones)

-- COMMAND ----------

-- Compaction and clustering optimization (OPTIMIZE rewrites many small files into fewer large ones).
OPTIMIZE main_jcg.default.mortgage_loans_delta;

-- Or full reclustering if you change keys later:
-- OPTIMIZE ${catalog}.${schema}.${table_name} FULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query to highlight clustering benefits:
-- MAGIC
-- MAGIC Filter and group on clustering keys to show efficient access.

-- COMMAND ----------

SELECT property_state, delinquency_status, COUNT(*) AS loans
FROM main_jcg.default.mortgage_loans_delta
WHERE property_state IN ('CA', 'TX', 'NY')
GROUP BY property_state, delinquency_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## History retention and VACUUM

-- COMMAND ----------

-- Check current table history retention behavior
DESCRIBE HISTORY main_jcg.default.mortgage_loans_delta;


-- COMMAND ----------

-- Show current retention-related table properties (if any)
DESCRIBE EXTENDED main_jcg.default.mortgage_loans_delta;


-- COMMAND ----------

-- (Optional) Set explicit retention policies for this demo table.
-- deletedFileRetentionDuration controls how long data files are kept for time travel.
-- logRetentionDuration controls how long transaction log history is kept. 

ALTER TABLE main_jcg.default.mortgage_loans_delta
SET TBLPROPERTIES (
  delta.deletedFileRetentionDuration = '7 days',
  delta.logRetentionDuration = '30 days'
);


-- COMMAND ----------

-- VACUUM removes data files older than the specified retention period.
-- After this, you cannot time travel to versions that depend on removed files.

-- For demo purposes, keep the default 7 days:
VACUUM main_jcg.default.mortgage_loans_delta;

-- Or be explicit:
-- VACUUM main_jcg.default.mortgage_loans_delta RETAIN 168 HOURS;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quick Transaction Log Peek
-- MAGIC
-- MAGIC These cells tie ACID and time travel back to the _delta_log folder and log metadata.

-- COMMAND ----------

-- See detailed table metadata, including the storage location and file stats. 
DESCRIBE DETAIL main_jcg.default.mortgage_loans_delta;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Replace this with the actual 'location' value from DESCRIBE DETAIL
-- MAGIC
-- MAGIC # The .json file is one committed transaction versino of the Delta table that lists all of the actions for that commit. For instance, that's files removed/added, metadata changes, protocol changes, commit info, etc. It's now Delta reconstructs the table state for ACID, time travel, and schema enforcement. 
-- MAGIC
-- MAGIC # The .crc file is a checksum/validation file for the json log file that makes suer that the json file isn't corrupted and make sure that it's not partially written during an interrupted write. 
-- MAGIC
-- MAGIC table_path = "s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/9f6f4e4a-1e1b-47ec-9cf0-30a75b8b3dc7"
-- MAGIC
-- MAGIC display(dbutils.fs.ls(f"{table_path}/_delta_log"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If your cluster has permission to read Delta log files in the S3 bucket, you can peek at the latest JSON commit (transaction log entry) show atomic actions.
-- MAGIC
-- MAGIC In the demo environment, we don't have access to do so but you can on your environment.