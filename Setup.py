# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Setup for Delta Demo
# MAGIC
# MAGIC This notebook is to generate a dataset so we can walk through the Delta features together.

# COMMAND ----------

# Notebook: 01_fannie_mortgage_delta_demo

from pyspark.sql import functions as F
from pyspark.sql import types as T

catalog = "main_jcg"          # adjust if using Unity Catalog
schema = "default"    # adjust as needed
table_name = "mortgage_loans_delta"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Synthetic Mortgage Data
# MAGIC

# COMMAND ----------

# Define a schema that looks like Fannie Mae single-family fixed-rate loans (simplified).
# Public Fannie Mae datasets include origination info plus dynamic monthly performance. [web:1][web:3][web:14]

loan_schema = T.StructType([
    T.StructField("loan_id", T.StringType(), False),
    T.StructField("origination_date", T.DateType(), False),
    T.StructField("first_payment_date", T.DateType(), False),
    T.StructField("maturity_date", T.DateType(), False),
    T.StructField("origination_unpaid_principal", T.DoubleType(), False),
    T.StructField("current_unpaid_principal", T.DoubleType(), False),
    T.StructField("interest_rate", T.DoubleType(), False),
    T.StructField("borrower_credit_score", T.IntegerType(), True),
    T.StructField("debt_to_income_ratio", T.DoubleType(), True),
    T.StructField("loan_to_value_ratio", T.DoubleType(), True),
    T.StructField("occupancy_type", T.StringType(), True),      # Owner, Investor, SecondHome
    T.StructField("channel", T.StringType(), True),             # Retail, Correspondent, Broker
    T.StructField("property_state", T.StringType(), True),
    T.StructField("property_type", T.StringType(), True),       # SF, Condo, 2-4 Unit
    T.StructField("number_of_units", T.IntegerType(), True),
    T.StructField("msa", T.StringType(), True),
    T.StructField("loan_purpose", T.StringType(), True),        # Purchase, Refi
    T.StructField("product_type", T.StringType(), True),        # FRM30, FRM15
    T.StructField("delinquency_status", T.StringType(), True),  # Current, 30, 60, 90, Foreclosed, REO
    T.StructField("status_as_of", T.DateType(), False),         # snapshot date (supports time travel demos)
])


# COMMAND ----------

# Generating Synthetic Mortgage Data
# Define a schema that looks like Fannie Mae single-family fixed-rate loans (simplified).
# Public Fannie Mae datasets include origination info plus dynamic monthly performance. [web:1][web:3][web:14]

loan_schema = T.StructType([
    T.StructField("loan_id", T.StringType(), False),
    T.StructField("origination_date", T.DateType(), False),
    T.StructField("first_payment_date", T.DateType(), False),
    T.StructField("maturity_date", T.DateType(), False),
    T.StructField("origination_unpaid_principal", T.DoubleType(), False),
    T.StructField("current_unpaid_principal", T.DoubleType(), False),
    T.StructField("interest_rate", T.DoubleType(), False),
    T.StructField("borrower_credit_score", T.IntegerType(), True),
    T.StructField("debt_to_income_ratio", T.DoubleType(), True),
    T.StructField("loan_to_value_ratio", T.DoubleType(), True),
    T.StructField("occupancy_type", T.StringType(), True),      # Owner, Investor, SecondHome
    T.StructField("channel", T.StringType(), True),             # Retail, Correspondent, Broker
    T.StructField("property_state", T.StringType(), True),
    T.StructField("property_type", T.StringType(), True),       # SF, Condo, 2-4 Unit
    T.StructField("number_of_units", T.IntegerType(), True),
    T.StructField("msa", T.StringType(), True),
    T.StructField("loan_purpose", T.StringType(), True),        # Purchase, Refi
    T.StructField("product_type", T.StringType(), True),        # FRM30, FRM15
    T.StructField("delinquency_status", T.StringType(), True),  # Current, 30, 60, 90, Foreclosed, REO
    T.StructField("status_as_of", T.DateType(), False),         # snapshot date (supports time travel demos)
])


# COMMAND ----------

import random
import datetime as dt

num_loans = 200_000       # big enough to show file layout / compaction
snap_date = dt.date(2025, 1, 31)

states = ["CA", "TX", "NY", "FL", "IL", "GA", "NC", "VA", "WA", "CO"]
occupancies = ["Owner", "Investor", "SecondHome"]
channels = ["Retail", "Correspondent", "Broker"]
prop_types = ["SF", "Condo", "2-4 Unit"]
purposes = ["Purchase", "Refi"]
products = ["FRM30", "FRM15"]
dqs = ["Current", "30", "60", "90", "Foreclosed", "REO"]

random.seed(42)

rows = []
for i in range(num_loans):
    # Origination dates over 5-year window
    orig_year = random.randint(2018, 2023)
    orig_month = random.randint(1, 12)
    orig_day = random.randint(1, 28)
    orig_date = dt.date(orig_year, orig_month, orig_day)
    
    product = random.choice(products)
    term_months = 360 if product == "FRM30" else 180
    maturity = orig_date + dt.timedelta(days=term_months * 30)

    upb = random.randint(100_000, 800_000)
    coupon = round(random.uniform(2.0, 7.0), 3)
    fico = random.randint(620, 800)
    dti = round(random.uniform(20, 50), 1)
    ltv = round(random.uniform(60, 97), 1)
    occ = random.choice(occupancies)
    ch = random.choice(channels)
    st = random.choice(states)
    pt = random.choice(prop_types)
    units = 1 if pt in ["SF", "Condo"] else random.randint(2, 4)
    msa = f"{random.randint(10000, 49999)}"
    purpose = random.choice(purposes)

    # Simple performance logic: lower FICO/higher DTI -> higher delinquency probability. [web:1][web:14]
    risk_score = (700 - fico) / 100 + (dti - 30) / 10 + (ltv - 80) / 10
    if risk_score < 0:
        dq = "Current"
    elif risk_score < 1:
        dq = random.choices(["Current", "30"], [0.9, 0.1])[0]
    elif risk_score < 2:
        dq = random.choices(["Current", "30", "60"], [0.8, 0.15, 0.05])[0]
    else:
        dq = random.choices(["Current", "30", "60", "90", "Foreclosed", "REO"],
                            [0.6, 0.15, 0.1, 0.05, 0.05, 0.05])[0]

    # crude amortization: reduce UPB if not foreclosed/REO
    months_elapsed = max(0, (snap_date.year - orig_date.year) * 12 + (snap_date.month - orig_date.month))
    paydown_factor = min(1.0, months_elapsed / term_months)
    cur_upb = upb * (1 - 0.6 * paydown_factor)   # assume 60% principal reduction by maturity

    rows.append((
        f"L{i:09d}",
        orig_date,
        orig_date + dt.timedelta(days=30),
        maturity,
        float(upb),
        float(round(cur_upb, 2)),
        coupon,
        fico,
        dti,
        ltv,
        occ,
        ch,
        st,
        pt,
        units,
        msa,
        purpose,
        product,
        dq,
        snap_date
    ))

loan_df = spark.createDataFrame(rows, schema=loan_schema)
loan_df.printSchema()
loan_df.show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Delta table (clustering-ready)

# COMMAND ----------

# Drop table if re-running
spark.sql(
    f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
)

# Create a managed Delta table with liquid clustering (DBR 13.3+)
(
    loan_df.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(
            f"{catalog}.{schema}.{table_name}"
        )
)

# Enable clustering
spark.sql(
    f"""
    ALTER TABLE {catalog}.{schema}.{table_name}
    CLUSTER BY (property_state, delinquency_status)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate multiple versions for time travel

# COMMAND ----------

from delta.tables import DeltaTable

delta_tbl = DeltaTable.forName(spark, f"{catalog}.{schema}.{table_name}")

# Version 1 already written above.

# Version 2: simulate a month later with some cures and new delinquencies
snap_date_v2 = dt.date(2025, 2, 28)

df_v2 = (
    loan_df
    .withColumn("status_as_of", F.lit(snap_date_v2))
    .withColumn(
        "delinquency_status",
        F.when(F.col("delinquency_status").isin("30", "60"), "Current")
         .when((F.col("delinquency_status") == "Current") & (F.rand() < 0.02), "30")
         .otherwise(F.col("delinquency_status"))
    )
    .withColumn(
        "current_unpaid_principal",
        (F.col("current_unpaid_principal") * F.lit(0.995)).cast("double")
    )
)

# Overwrite using MERGE (upsert) to show ACID behavior. [web:15][web:18]
delta_tbl.alias("t").merge(
    df_v2.alias("s"),
    "t.loan_id = s.loan_id"
).whenMatchedUpdateAll().execute()

# Version 3: introduce some foreclosures and REO
snap_date_v3 = dt.date(2025, 3, 31)

df_v3 = (
    df_v2
    .withColumn("status_as_of", F.lit(snap_date_v3))
    .withColumn(
        "delinquency_status",
        F.when((F.col("delinquency_status").isin("90")) & (F.rand() < 0.3), "Foreclosed")
         .when((F.col("delinquency_status") == "Foreclosed") & (F.rand() < 0.5), "REO")
         .otherwise(F.col("delinquency_status"))
    )
)

delta_tbl.alias("t").merge(
    df_v3.alias("s"),
    "t.loan_id = s.loan_id"
).whenMatchedUpdateAll().execute()
