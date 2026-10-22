"""
Spark Data Ingestion Module
Loads raw agricultural crop dataset from HDFS storage layer into a distributed PySpark DataFrame.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from config.config import get_spark_session, RAW_DATA_PATH
from hadoop.hdfs_storage import HDFSStorageManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("DataIngestion")

# Explicit Schema Definition matching actual dataset
CROP_DATA_SCHEMA = StructType([
    StructField("Year", IntegerType(), True),
    StructField("State", StringType(), True),
    StructField("Crop", StringType(), True),
    StructField("Season", StringType(), True),
    StructField("Area", DoubleType(), True),
    StructField("Production", DoubleType(), True),
    StructField("Annual_Rainfall", DoubleType(), True),
    StructField("Fertilizer", DoubleType(), True),
    StructField("Pesticide", DoubleType(), True),
    StructField("Yield", DoubleType(), True)
])

def ingest_data(spark=None, source_path=None):
    """
    Ingests data from HDFS/local into a distributed PySpark DataFrame.
    """
    if spark is None:
        spark = get_spark_session(app_name="DataIngestion")

    hdfs = HDFSStorageManager()
    path_to_read = source_path or hdfs.get_hdfs_path("raw", "crop_yield.csv")
    logger.info(f"Ingesting raw dataset from: {path_to_read}")

    df = spark.read.csv(
        path_to_read,
        header=True,
        schema=CROP_DATA_SCHEMA,
        mode="DROPMALFORMED"
    )

    record_count = df.count()
    col_count = len(df.columns)
    logger.info(f"Data Ingestion Complete: {record_count} records across {col_count} columns.")

    return df

if __name__ == "__main__":
    spark = get_spark_session("IngestionTest")
    df = ingest_data(spark)
    df.show(5)
    spark.stop()
