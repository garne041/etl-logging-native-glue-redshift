-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Engineering / Platform Review on Glue Connection
-- MAGIC
-- MAGIC This notebook is to show how Glue can be connected to Databricks through Lakehouse Federation. 
-- MAGIC
-- MAGIC
-- MAGIC For more info, check out the documentation on running federated queries on AWS Glue Hive ([link](https://docs.databricks.com/aws/en/query-federation/hms-federation-glue)).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Glue Connectivity
-- MAGIC
-- MAGIC
-- MAGIC ![](images/glue-catalog-overview.png)
-- MAGIC
-- MAGIC ![](images/glue-foreign-schema.png)
-- MAGIC
-- MAGIC ![](images/glue-connection.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic profiling query

-- COMMAND ----------

USE CATALOG `joy-foreign-glue`;
USE SCHEMA joy_db;

-- COMMAND ----------

SELECT *
FROM `joy-foreign-glue`.joy_db.joy_bronze_customer_data
--ORDER BY order_date DESC;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use the foreign catalog and database that mirrors Glue
-- MAGIC spark.sql("USE CATALOG `joy-foreign-glue`")
-- MAGIC spark.sql("USE joy_db")
-- MAGIC
-- MAGIC df = spark.table("joy_bronze_customer_data")   # this is the Glue CSV-classified table
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time travel for external catalogs
-- MAGIC
-- MAGIC A foreign catalog is just a virtual mirror of an external database. Databricks does not manage that storage or keep a Delta-style history for it.
-- MAGIC
-- MAGIC Lakehouse Federation provides read-only access. That being said, Databricks is not the system of record and does not rewrite or version data on Glue.
-- MAGIC
-- MAGIC Because Databricks never creates its own snapshots/versions for those foreign tables, there is no Delta/UC history to show, so DESCRIBE HISTORY and Delta time travel are not supported for foreign catalogs like Redshift or Glue.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## UPDATE / DELETE on Foreign Catalogs
-- MAGIC
-- MAGIC Lakehouse Federation foreign catalogs (Redshift, Glue, etc.) are read-only from Databricks’ perspective. So updating and deleting would be prohibited. Also this is true for clustering, compaction, history retention, vacuum, and optimization.
-- MAGIC
-- MAGIC Only SELECT (and some metadata operations like SHOW TABLES) are supported on foreign catalogs.
-- MAGIC