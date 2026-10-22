"""
Database Data Loader
Populates model registry, evaluation metrics, and state-level processed summary
into the relational database layer (MySQL or SQLite fallback).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import logging
import pandas as pd
from config.config import MODELS_DIR, METRICS_OUTPUT_DIR, PREDICTIONS_OUTPUT_DIR
from mysql.connection import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("StoreResults")

def store_pipeline_results():
    db = DatabaseManager()
    logger.info("Storing Spark Machine Learning results into relational database...")

    # 1. Store Model Metadata & Registry
    metadata_file = MODELS_DIR / "model_metadata.json"
    if metadata_file.exists():
        with open(metadata_file) as f:
            meta = json.load(f)

        best_model = meta.get("best_model_name", "Random Forest Regressor")
        train_samples = meta.get("train_samples", 0)
        test_samples = meta.get("test_samples", 0)

        insert_model_query = """
            INSERT INTO model_registry (model_name, algorithm, train_samples, test_samples, is_best_model, model_path)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        db.execute_insert(insert_model_query, (
            best_model,
            "Spark MLlib RandomForestRegressionModel",
            train_samples,
            test_samples,
            1,
            str(MODELS_DIR / "best_model")
        ))
        logger.info(f"Registered model '{best_model}' into model_registry.")

    # 2. Store Evaluation Metrics
    metrics_file = METRICS_OUTPUT_DIR / "evaluation_summary.csv"
    if metrics_file.exists():
        df_metrics = pd.read_csv(metrics_file)
        insert_metric_query = """
            INSERT INTO model_metrics (model_name, rmse, mae, r2)
            VALUES (%s, %s, %s, %s)
        """
        for _, row in df_metrics.iterrows():
            db.execute_insert(insert_metric_query, (
                row["model_name"],
                float(row["rmse"]),
                float(row["mae"]),
                float(row["r2"])
            ))
            logger.info(f"Recorded metrics for {row['model_name']} (RMSE={row['rmse']}, R2={row['r2']}).")

    # 3. Store Sample Predictions
    preds_file = PREDICTIONS_OUTPUT_DIR / "sample_predictions.csv"
    if preds_file.exists():
        df_preds = pd.read_csv(preds_file)
        insert_pred_query = """
            INSERT INTO cotton_predictions (
                state, season, harvest_year, area_hectares, rainfall_mm,
                fertilizer_kg, pesticide_kg, predicted_yield, estimated_production_tonnes, model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in df_preds.head(20).iterrows():
            db.execute_insert(insert_pred_query, (
                str(row["State"]),
                str(row["Season"]),
                int(row["Year"]),
                float(row["Area"]),
                float(row["Annual_Rainfall"]),
                float(row["Fertilizer"]),
                float(row["Pesticide"]),
                float(row["Predicted_Yield"]),
                float(row["Predicted_Yield"] * row["Area"]),
                "1.0-RF"
            ))
        logger.info(f"Stored 20 sample predictions into cotton_predictions table.")

    # 4. Check contents
    rows = db.execute_query("SELECT COUNT(*) AS total_models FROM model_registry")
    logger.info(f"Database sync complete. Total models recorded: {rows}")

if __name__ == "__main__":
    store_pipeline_results()
