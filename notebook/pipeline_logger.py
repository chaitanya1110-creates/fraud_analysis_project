import logging
import os
import tempfile

class Logger:
    def __init__(self, log_path="/Volumes/fraudanalysis2026/gold/checkpoints/parquet_log/fraud_analysis.log"):
        self.adls_log_path = log_path

        # Local temp file for Python's FileHandler (can't write to abfss:// directly)
        self._local_path = os.path.join(tempfile.gettempdir(), "pipeline_fraud_analysis.log")

        # Setup the standard Python logger
        self.logger = logging.getLogger("SimpleLogger")
        self.logger.setLevel(logging.INFO)
        
        # Clear any stale handlers from previous imports/reloads
        self.logger.handlers.clear()
        file_handler = logging.FileHandler(self._local_path, mode='a')
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _flush_to_adls(self):
        """Flush buffered logs to ADLS Gen2 via Volume path."""
        for handler in self.logger.handlers:
            handler.flush()
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        dbutils.fs.cp(f"file:{self._local_path}", self.adls_log_path)

    def safe_run(self, stage_name, task_fn, *args, **kwargs):
        """
        Runs any function inside a try-except block and logs the result.
        """
        try:
            self.logger.info(f"STARTING: {stage_name}")
            
            # Execute the function
            result = task_fn(*args, **kwargs)
            
            self.logger.info(f"SUCCESS: {stage_name}")
            return result
            
        except Exception as e:
            # Catch and log any error that occurs
            self.logger.error(f"FAILED: {stage_name} | ERROR: {str(e)}")
            raise e
        finally:
            # Persist logs to ADLS Gen2 after each stage
            self._flush_to_adls()
