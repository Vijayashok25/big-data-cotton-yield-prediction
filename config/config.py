"""
Configuration Module for Big Data Cotton Yield Prediction System
Provides centralized settings, environment variable overrides,
and SparkSession bootstrapping for reproducible pipeline execution.
"""

import os
import sys
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "crop_yield.csv"
COTTON_DATA_PATH = DATA_DIR / "cotton_yield.csv"

# Models & Outputs
MODELS_DIR = PROJECT_ROOT / "models"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
EDA_OUTPUT_DIR = OUTPUTS_DIR / "eda"
METRICS_OUTPUT_DIR = OUTPUTS_DIR / "metrics"
PREDICTIONS_OUTPUT_DIR = OUTPUTS_DIR / "predictions"

# HDFS Configuration
HDFS_SIMULATED_DIR = DATA_DIR / "hdfs"
HDFS_RAW_DIR = HDFS_SIMULATED_DIR / "raw"
HDFS_PROCESSED_DIR = HDFS_SIMULATED_DIR / "processed"
HDFS_FEATURES_DIR = HDFS_SIMULATED_DIR / "features"
HDFS_PREDICTIONS_DIR = HDFS_SIMULATED_DIR / "predictions"
HDFS_URI = os.getenv("HDFS_URI", "")  # e.g. "hdfs://localhost:9000/cotton_yield"

# Hive Configuration
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "cotton_yield_db")
HIVE_TABLE_RAW = "crop_yield_raw"
HIVE_TABLE_COTTON = "cotton_yield_cleaned"
SPARK_WAREHOUSE_DIR = str(PROJECT_ROOT / "spark-warehouse")

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "cotton_yield")

# SQLite Fallback (ensures working storage layer even if MySQL daemon is offline)
SQLITE_DB_PATH = OUTPUTS_DIR / "cotton_yield_storage.db"

# Data Pipeline & ML Feature Specifications
# Note: 'Production' is explicitly EXCLUDED from model features to avoid target leakage,
# because Yield = Production / Area (reported post-harvest).
CATEGORICAL_FEATURES = ["State", "Season"]
NUMERICAL_FEATURES = ["Year", "Area", "Annual_Rainfall", "Fertilizer", "Pesticide"]
ENGINEERED_FEATURES = ["Fertilizer_per_Area", "Pesticide_per_Area"]
TARGET_VARIABLE = "Yield"
EXCLUDED_FEATURES = {
    "Production": "Post-harvest aggregate directly tied to Yield (Yield = Production / Area); causes target leakage.",
    "Crop": "Constant for single-crop Cotton models; used only when running cross-crop comparison."
}

TRAIN_SPLIT_RATIO = 0.8
RANDOM_SEED = 42

# Environment Bootstrapping for PySpark & Java
def setup_environment():
    """Sets required environment variables for PySpark, Python workers, and Java/Hadoop."""
    py_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = py_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exec
    
    # Configure JAVA_HOME if not defined
    if not os.getenv("JAVA_HOME"):
        default_jdk = r"C:\Program Files\Java\jdk-21"
        if os.path.exists(default_jdk):
            os.environ["JAVA_HOME"] = default_jdk

    # Configure HADOOP_HOME to local hadoop directory with winutils.exe
    local_hadoop = str(PROJECT_ROOT / "hadoop")
    winutils_bin = PROJECT_ROOT / "hadoop" / "bin" / "winutils.exe"
    if winutils_bin.exists():
        os.environ["HADOOP_HOME"] = local_hadoop
        bin_dir = str(PROJECT_ROOT / "hadoop" / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    elif "HADOOP_HOME" in os.environ and not os.path.exists(os.environ["HADOOP_HOME"]):
        del os.environ["HADOOP_HOME"]

def get_spark_session(app_name="CottonYieldPrediction", enable_hive=False):
    """
    Initializes and returns an optimized SparkSession with appropriate driver memory
    and warehouse configuration.
    """
    setup_environment()
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
