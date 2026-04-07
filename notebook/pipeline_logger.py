import logging
import os
import tempfile

class Logger:
    def __init__(self, log_path="/Volumes/fraudanalysis2026/gold/checkpoints/parquet_log/fraud_analysis.log"):
        self.adls_log_path = log_path

        # Using local temp file for Python's FileHandler because we can't write to abfss:// directly
        self._local_path = os.path.join(tempfile.gettempdir(), "pipeline_fraud_analysis.log")

        # Setup the standard Python logger using Info over here to just see if it works
        self.logger = logging.getLogger("SimpleLogger")
        self.logger.setLevel(logging.INFO)
        
        # Clear any stale handlers from previous imports to get out of the loop and not let it print twice or thrice
        self.logger.handlers.clear()
        file_handler = logging.FileHandler(self._local_path, mode='a')
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # This is used to directly send the file to the abfss path using .flush()
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
            
            # Execute the function here *args takes the path and **kwargs take directories
            result = task_fn(*args, **kwargs)
            
            self.logger.info(f"SUCCESS: {stage_name}")
            return result
            
        except Exception as e:
            # Catch and log any error that occurs
            self.logger.error(f"FAILED: {stage_name} | ERROR: {str(e)}")
            raise e
        finally:
            # Sends logs to ADLS Gen2 after each stage
            self._flush_to_adls()
