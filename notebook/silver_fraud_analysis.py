# Databricks notebook source
from pipeline_logger import Logger
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType

# 1. Initialize Logger
log = Logger()

# 2. Configuration
storage_account = "abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net"
checkpoint_path = f"{storage_account}/checkpoints/silver"

# --- STEP 1: SETUP ENVIRONMENT ---
def setup_silver_env():
    spark.sql("USE CATALOG fraudanalysis2026")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

log.safe_run("Setup Silver Schema", setup_silver_env)

# --- STEP 2: TRANSFORMATION & CLEANING ---
def process_silver_layer():
    # Read from Managed Bronze Table
    df_raw = spark.readStream.table("fraudanalysis2026.bronze.bronze_fraud_analysis")

    # A. Drop the unwanted column
    # B. Cast columns to correct types (based on your screenshot)
    # C. Fill NA values (Unknown for strings, 0 for numbers)
    df_transformed = (df_raw
        .drop("rescued_data")
        .withColumn("amt", F.col("amt").cast(DoubleType()))
        .withColumn("zip", F.col("zip").cast(StringType()))
        .withColumn("city_pop", F.col("city_pop").cast(IntegerType()))
        .withColumn("trans_date_trans_time", F.col("trans_date_trans_time").cast(TimestampType()))
        .fillna("Unknown") # Fills all String nulls
        .fillna(0, subset=["amt", "city_pop", "lat", "long", "merch_lat", "merch_long"]) # Fills numeric nulls with 0
        .fillna("00000", subset=["cc_num", "zip", "merch_zipcode"]) # Fills specific codes
    )

    # D. Deduplication using the transaction unique ID
    df_final = df_transformed.dropDuplicates(["trans_num"])

   
    query = (df_final.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("fraudanalysis2026.silver.silver_fraud_analysis"))
    
    query.awaitTermination()

log.safe_run("Silver Cleaning and Type Casting", process_silver_layer)