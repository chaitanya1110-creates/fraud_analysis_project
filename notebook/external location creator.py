# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `bronze`
# MAGIC URL 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/bronze/'
# MAGIC WITH (STORAGE CREDENTIAL `bronze`)
# MAGIC COMMENT 'for the raw bronze data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `silver`
# MAGIC URL 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/silver/'
# MAGIC WITH (STORAGE CREDENTIAL `silver`)
# MAGIC COMMENT 'for the cleaned silver data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `gold`
# MAGIC URL 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/gold/'
# MAGIC WITH (STORAGE CREDENTIAL `gold`)
# MAGIC COMMENT 'for the gold data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `checkpoint`
# MAGIC URL 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/checkpoints/'
# MAGIC WITH (STORAGE CREDENTIAL `checkpoint`)
# MAGIC COMMENT 'for the checkpoint data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fix: /checkpoint/ (singular) -> /checkpoints/ (plural) to match notebook paths
# MAGIC ALTER EXTERNAL LOCATION `checkpoint` SET URL 'abfss://lakehouse@fraudanalysis2026.dfs.core.windows.net/checkpoints/';