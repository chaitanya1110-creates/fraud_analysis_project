# Databricks notebook source
#MAGIC -- used this to reload the .py file when it was giving 3 - 4 same outputs
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Import your custom class
from pipeline_logger import Logger

# Initialize your CUSTOM class instance
# We pass __name__ as the first argument (the name of the logger)
log = Logger() 

# To let log.safe_run work perfectly
storage_account = "abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net"
checkpoint_path = f"{storage_account}/checkpoints/bronze"
source_path     = f"{storage_account}/bronze/"

# --- STEP 1: SETUP ---

spark.sql("CREATE CATALOG IF NOT EXISTS fraudanalysis2026")
spark.sql("USE CATALOG fraudanalysis2026")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# --- STEP 2: INGEST ---
def ingest_bronze():
    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema_evolution")
        .load(source_path))

    query = (df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("fraudanalysis2026.bronze.bronze_fraud_analysis"))
    
    query.awaitTermination()

log.safe_run("Bronze Incremental Ingestion", ingest_bronze)

# COMMAND ----------

