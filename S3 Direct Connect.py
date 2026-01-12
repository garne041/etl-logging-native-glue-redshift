# Databricks notebook source
# MAGIC %md
# MAGIC # Reading and Writing Back to an S3 Bucket
# MAGIC
# MAGIC This image shows the originals and the files that were written back using the code in this notebook.
# MAGIC
# MAGIC ![](/Workspace/Users/joy.garnett@databricks.com/Customer Questions/FNMA/ETL Logging Native, Glue, and Redshift/images/S3-direct-read-write.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Directly Connecting to a CSV in an S3 bucket

# COMMAND ----------

# Reading

df = spark.read.format("csv").option("header", True).load("s3a://jgarnett-us-east-1-bucket/s3_direct/MNQ_Stock.csv")
display(df)

# COMMAND ----------

# Write back to the S3 bucket
prefix = "renamed_and_saved_"
new_cols = [f'{prefix}({c})' for c in df.columns]

df_renamed = df.toDF(*new_cols)  # rename all columns in one step


tmp_path = "dbfs:/tmp/MNQ_Stock_single_tmp"
final_path = "s3a://jgarnett-us-east-1-bucket/s3_direct/MNQ_Stock_Renamed_and_Saved.csv"

#1 Writing as a single part folder
df_renamed.write.mode("overwrite").option('header', 'true').csv(tmp_path)

#2 Find single part file
files = dbutils.fs.ls(tmp_path)
part_file = [f.path for f in files if f.name.startswith("part-")][0]

#3 Copy to final S3 file and clean up

dbutils.fs.cp(part_file, final_path)
dbutils.fs.rm(tmp_path, recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Directly Connecting to a JSON File in an S3 Bucket

# COMMAND ----------

# Reading from a json in S3

df2 = spark.read.format("json").option("multiLine", True).load("s3a://jgarnett-us-east-1-bucket/s3_direct/zoneinfo_data.json")
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col

flat_cols = []
for c in df2.columns:
    # For nested fields like "parent.child", Spark shows them as "parent.child" in schema.
    # Select with alias to replace dots in the output name.
    flat_cols.append(col(c).alias(c.replace(".", "_")))

df2_flat = df2.select(*flat_cols)

# Rename all columns with a prefix
prefix = "renamed_and_saved_"
new_cols = [f'{prefix}({c})' for c in df2_flat.columns]
df2_renamed = df2_flat.toDF(*new_cols)  # rename all columns at once

# COMMAND ----------

# Writing to a json in S3
tmp_path2 = "dbfs:/tmp/my_json_single_tmp"
final_path = "s3a://jgarnett-us-east-1-bucket/s3_direct/renamed_zone_info.json"

# 1) Write to a temporary folder as a single-part JSON dataset
(
    df2_renamed
      .coalesce(1)              # force single output file
      .write
      .mode("overwrite")
      .json(tmp_path2)
)

# 2) Find the single part file
files = dbutils.fs.ls(tmp_path2)
part_file = [f.path for f in files if f.name.startswith("part-")][0]

# 3) Copy to final S3 key and clean up
dbutils.fs.cp(part_file, final_path)
dbutils.fs.rm(tmp_path2, recurse=True)

