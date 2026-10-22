"""
Master Pipeline Orchestrator: Big Data Cotton Yield Prediction System
Executes the complete end-to-end Big Data & Machine Learning workflow:
Dataset -> HDFS -> Hive -> PySpark -> Feature Engineering -> MLlib -> MySQL -> Predictions
"""

import sys
from pathlib import Path

# Set up project path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
import logging
from config.config import setup_environment, get_spark_session
from hadoop.hdfs_storage import HDFSStorageManager
from hive.hive_runner import run_hive_pipeline
from scripts.run_eda import run_eda
from spark.train_model import train_and_evaluate_models
from spark.evaluate_model import generate_evaluation_diagnostics
from mysql.store_results import store_pipeline_results
from spark.predict import CottonYieldPredictor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("PipelineOrchestrator")

def execute_full_pipeline():
    start_time = time.time()
    logger.info("================================================================================")
    logger.info("Starting Big Data Cotton Yield Prediction Pipeline Execution")
    logger.info("================================================================================")

    # Step 1: Environment Setup
    logger.info("[Step 1/8] Bootstrapping Runtime Environment & Spark Configuration...")
    setup_environment()

    # Step 2: HDFS Storage Layer
    logger.info("[Step 2/8] Initializing HDFS Distributed Storage & Ingesting Raw Data...")
    hdfs_manager = HDFSStorageManager()
    hdfs_manager.init_directories()
    hdfs_manager.upload_dataset()
    hdfs_manager.verify_upload("raw", "crop_yield.csv")
    hdfs_manager.verify_upload("raw", "cotton_yield.csv")

    # Step 3: Hive Database & Analytics
    logger.info("[Step 3/8] Creating Hive Database, Raw Tables, and Running Big Data Queries...")
    run_hive_pipeline()

    # Step 4: Exploratory Data Analysis
    logger.info("[Step 4/8] Running Exploratory Data Analysis & Generating Visual Analytics...")
    run_eda()

    # Step 5: Spark MLlib Model Training & Evaluation
    logger.info("[Step 5/8] Running Spark ML Feature Pipeline & Training Regressors...")
    spark = get_spark_session("CottonYieldMasterPipeline")
    best_model_name, df_metrics = train_and_evaluate_models(spark)

    # Step 6: Evaluation Diagnostics
    logger.info("[Step 6/8] Generating Evaluation Residual Diagnostics...")
    generate_evaluation_diagnostics()

    # Step 7: Relational Database Storage (MySQL / SQLite fallback)
    logger.info("[Step 7/8] Syncing Pipeline Metadata, Metrics, & Predictions to Relational Storage...")
    store_pipeline_results()

    # Step 8: Live Prediction Smoke Test
    logger.info("[Step 8/8] Executing Live Inference Smoke Test with Spark PipelineModel...")
    predictor = CottonYieldPredictor(spark=spark)
    sample_input = {
        "State": "Gujarat",
        "Season": "Kharif",
        "Year": 2026,
        "Area": 650000.0,
        "Annual_Rainfall": 780.0,
        "Fertilizer": 88000000.0,
        "Pesticide": 160000.0
    }
    pred_result = predictor.predict_single(sample_input)

    spark.stop()
    elapsed = round(time.time() - start_time, 2)

    logger.info("================================================================================")
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("================================================================================")
    logger.info(f"Total Execution Time: {elapsed} seconds")
    logger.info(f"Selected Best Model: {best_model_name}")
    print("\nModel Evaluation Performance Table:")
    print(df_metrics.to_string(index=False))
    print("\nLive Smoke Test Prediction Output:")
    for k, v in pred_result.items():
        print(f"  {k}: {v}")
    logger.info("================================================================================")
    logger.info("To start the web dashboard and REST API, run:")
    logger.info("  python dashboard/app.py")
    logger.info("================================================================================")

if __name__ == "__main__":
    execute_full_pipeline()
