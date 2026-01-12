-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Engineering / Platform Review on Redshift Connection
-- MAGIC
-- MAGIC This notebook is to show how Redshift can be connected to Databricks through Lakehouse Federation. 
-- MAGIC
-- MAGIC
-- MAGIC For more info, check out the documentation on running federated queries on Amazon Redshift ([link](https://docs.databricks.com/aws/en/query-federation/redshift)).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/Catalog_Redshift_Foreign.png)
-- MAGIC
-- MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/Redshift_Table_in_DBX.png)
-- MAGIC
-- MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/Redshift_Connection_Details.png)
-- MAGIC
-- MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/Redshift_Catalog_Details.png)
-- MAGIC
-- MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/External_Data_Catalog_image.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic profiling query

-- COMMAND ----------

USE CATALOG joy_redshift_catalog;
USE SCHEMA amenon;

-- COMMAND ----------

SELECT *
FROM joy_redshift_catalog.amenon.nyctaxi_yellow_100k_daily_revenue_vw
ORDER BY order_date DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Time travel for external catalogs
-- MAGIC
-- MAGIC A Redshift foreign catalog is just a virtual mirror of an external database. Databricks does not manage that storage or keep a Delta-style history for it.
-- MAGIC
-- MAGIC Lakehouse Federation provides read-only access. That being said, Databricks is not the system of record and does not rewrite or version Redshift data.
-- MAGIC
-- MAGIC Because Databricks never creates its own snapshots/versions for those foreign tables, there is no Delta/UC history to show, so DESCRIBE HISTORY and Delta time travel are not supported for foreign catalogs like Redshift or Glue.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## UPDATE / DELETE on Foreign Catalogs
-- MAGIC
-- MAGIC Lakehouse Federation foreign catalogs (Redshift, etc.) are read-only from Databricks’ perspective. So updating and deleting would be prohibited. Also this is true for clustering, compaction, history retention, vacuum, and optimization.
-- MAGIC
-- MAGIC Only SELECT (and some metadata operations like SHOW TABLES) are supported on foreign catalogs.
-- MAGIC
-- MAGIC “Writes and ACID are handled by Redshift; Databricks, via Lakehouse Federation, always reads a fully committed snapshot, not partial changes.”
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Writing back to Redshift
-- MAGIC
-- MAGIC Writing back to Redshift can be done with a jdbc connection. I'll include an example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # This is only an example. 
-- MAGIC
-- MAGIC jdbc_url = (
-- MAGIC     "jdbc:redshift:iam://mycluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev"
-- MAGIC     "?DbUser=etl_user&ssl=true"
-- MAGIC )
-- MAGIC
-- MAGIC temp_s3_dir = "s3a://my-redshift-temp-bucket/tmp/"
-- MAGIC aws_iam_role_arn = "arn:aws:iam::123456789012:role/redshift-s3-access-role"
-- MAGIC
-- MAGIC (
-- MAGIC     df_curated.write
-- MAGIC       .format("io.github.spark_redshift_community.spark.redshift")
-- MAGIC       .option("url", jdbc_url)              # IAM URL, no static password
-- MAGIC       .option("dbtable", "public.sales_curated")
-- MAGIC       .option("tempdir", temp_s3_dir)
-- MAGIC       .option("aws_iam_role", aws_iam_role_arn)  # role Redshift assumes to read/write S3
-- MAGIC       .mode("overwrite")
-- MAGIC       .save()
-- MAGIC )
-- MAGIC