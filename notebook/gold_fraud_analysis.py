# Databricks notebook source
from pipeline_logger import Logger
from pyspark.sql import Window
import pyspark.sql.functions as F

# 1. Initialize Logger
log = Logger()

# 2. Configuration - External Volumes mapped to existing external locations
volume_gold_path = "/Volumes/fraudanalysis2026/gold/gold_data"
gold_external_path = f"{volume_gold_path}/"
checkpoint_path = "/Volumes/fraudanalysis2026/gold/checkpoints/gold"
json_file_path = f"{gold_external_path}flagged_personnel.json"

# --- STEP 1: SETUP ---

spark.sql("USE CATALOG fraudanalysis2026")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraudanalysis2026.gold.gold_data
    LOCATION 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/gold'
""")
spark.sql("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS fraudanalysis2026.gold.checkpoints
    LOCATION 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/checkpoints'
""")



# --- STEP 2: ANALYTICS & COLUMN SELECTION ---
def process_gold_batch(batch_df, batch_id):
    # Window for calculating individual transaction risk
    window_spec = Window.partitionBy("cc_num").orderBy("trans_date_trans_time")
    
    # A. Calculate Metrics
    enriched_df = batch_df.withColumn(
        "avg_amt_per_user", F.avg("amt").over(window_spec)
    ).withColumn(
        "prev_trans_time", F.lag("trans_date_trans_time").over(window_spec)
    )

    # B. Flagging Logic
    flagged_df = enriched_df.withColumn(
        "is_fraud_flag",
        F.when(
            (F.col("amt") > (F.col("avg_amt_per_user") * 15)) |
            (F.unix_timestamp("trans_date_trans_time") - F.unix_timestamp("prev_trans_time") < 60),
            1
        ).otherwise(0)
    ).filter(F.col("is_fraud_flag") == 1)

    # C. RISK RANKING & CLEAN COLUMN SELECTION
    main_columns = [
        "cc_num", "first", "last", "trans_date_trans_time", 
        "amt", "city", "state", "job", "trans_num", "total_flags_count"
    ]

    risk_window = Window.partitionBy("cc_num")
    
    final_gold_df = (flagged_df
        .withColumn("total_flags_count", F.count("is_fraud_flag").over(risk_window))
        .select(*[c for c in main_columns if c in flagged_df.columns or c == "total_flags_count"])
        .orderBy(F.col("total_flags_count").desc())
    )

    # D. Aggregate new batch to one row per cardholder
    new_summary_df = (final_gold_df
        .groupBy("cc_num", "first", "last", "city", "state", "job")
        .agg(
            F.count("*").alias("total_flags_count"),
            F.round(F.sum("amt"), 2).alias("total_amt"),
            F.round(F.avg("amt"), 2).alias("avg_amt"),
            F.round(F.max("amt"), 2).alias("max_amt"),
            F.round(F.min("amt"), 2).alias("min_amt"),
            F.max("trans_date_trans_time").alias("latest_transaction"),
            F.min("trans_date_trans_time").alias("earliest_transaction")
        )
    )

    # E. Merge with existing flagged_personnel.json (append/update logic)
    try:
        existing_df = spark.read.json(json_file_path)
        merged_df = (existing_df.unionByName(new_summary_df)
            .groupBy("cc_num", "first", "last", "city", "state", "job")
            .agg(
                F.sum("total_flags_count").cast("long").alias("total_flags_count"),
                F.round(F.sum("total_amt"), 2).alias("total_amt"),
                F.round(F.avg("avg_amt"), 2).alias("avg_amt"),
                F.greatest(F.max("max_amt"), F.max("max_amt")).alias("max_amt"),
                F.least(F.min("min_amt"), F.min("min_amt")).alias("min_amt"),
                F.max("latest_transaction").alias("latest_transaction"),
                F.min("earliest_transaction").alias("earliest_transaction")
            )
        )
    except Exception:
        merged_df = new_summary_df

    summary_df = merged_df.orderBy(F.col("total_flags_count").desc())

    # Save as a single named JSON file: flagged_personnel.json
    temp_path = f"{volume_gold_path}/_temp_json"
    (summary_df.coalesce(1).write
        .mode("overwrite")
        .format("json")
        .save(temp_path))

    # Rename the Spark part file to flagged_personnel.json
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    part_file = [f.path for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")][0]
    dbutils.fs.cp(part_file, json_file_path)
    dbutils.fs.rm(temp_path, recurse=True)

    # F. Save detailed transactions to Unity Catalog Table
    (final_gold_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("fraudanalysis2026.gold.gold_fraud_analysis"))

# --- STEP 3: EXECUTION ---
def run_gold_pipeline():
    df_silver_stream = spark.readStream.table("fraudanalysis2026.silver.silver_fraud_analysis")

    query = (df_silver_stream.writeStream
        .foreachBatch(process_gold_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start())
    
    query.awaitTermination()

log.safe_run("Gold High-Risk Clean Report Export", run_gold_pipeline)