"""
Spark MLlib Model Training & Evaluation Pipeline
Trains Linear Regression, Random Forest, and Gradient-Boosted Trees,
evaluates on test split using RMSE, MAE, R2, and saves the best model.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import logging
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.regression import (
    LinearRegression,
    RandomForestRegressor,
    GBTRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator

from config.config import (
    get_spark_session,
    MODELS_DIR,
    METRICS_OUTPUT_DIR,
    TRAIN_SPLIT_RATIO,
    RANDOM_SEED
)
from spark.data_ingestion import ingest_data
from spark.preprocessing import preprocess_cotton_data
from spark.feature_engineering import build_feature_pipeline, get_feature_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ModelTraining")

def train_and_evaluate_models(spark=None):
    if spark is None:
        spark = get_spark_session(app_name="CottonYieldModelTraining")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Ingest & Preprocess
    raw_df = ingest_data(spark)
    cotton_df = preprocess_cotton_data(raw_df)

    # 2. Train / Test Split (Strictly 80/20, fixed seed for reproducibility)
    logger.info(f"Splitting data into {int(TRAIN_SPLIT_RATIO*100)}% Train / {int((1-TRAIN_SPLIT_RATIO)*100)}% Test (seed={RANDOM_SEED})...")
    train_df, test_df = cotton_df.randomSplit([TRAIN_SPLIT_RATIO, 1.0 - TRAIN_SPLIT_RATIO], seed=RANDOM_SEED)

    train_count = train_df.count()
    test_count = test_df.count()
    logger.info(f"Dataset split completed: {train_count} training samples, {test_count} testing samples.")

    # 3. Base Feature Transformation Pipeline
    feature_pipeline = build_feature_pipeline()

    # 4. Define Candidate Regressors
    candidate_models = {
        "Linear Regression": LinearRegression(
            featuresCol="features",
            labelCol="Yield",
            maxIter=100,
            regParam=0.05,
            elasticNetParam=0.2
        ),
        "Random Forest Regressor": RandomForestRegressor(
            featuresCol="features",
            labelCol="Yield",
            numTrees=60,
            maxDepth=8,
            seed=RANDOM_SEED
        ),
        "Gradient-Boosted Trees": GBTRegressor(
            featuresCol="features",
            labelCol="Yield",
            maxIter=50,
            maxDepth=5,
            seed=RANDOM_SEED
        )
    }

    evaluator_rmse = RegressionEvaluator(labelCol="Yield", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="Yield", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="Yield", predictionCol="prediction", metricName="r2")

    comparison_results = []
    trained_pipelines = {}

    logger.info("=================================================================")
    logger.info("Beginning Spark MLlib Model Training & Evaluation...")
    logger.info("=================================================================")

    for name, regressor in candidate_models.items():
        logger.info(f"--- Training Model: {name} ---")
        full_pipeline = Pipeline(stages=feature_pipeline.getStages() + [regressor])
        
        # Fit on TRAINING set only (prevents data leakage)
        fitted_pipeline = full_pipeline.fit(train_df)
        trained_pipelines[name] = fitted_pipeline

        # Predict on TEST set
        predictions = fitted_pipeline.transform(test_df)

        # Compute Metrics
        rmse = float(evaluator_rmse.evaluate(predictions))
        mae = float(evaluator_mae.evaluate(predictions))
        r2 = float(evaluator_r2.evaluate(predictions))

        logger.info(f"  -> {name} Results: RMSE = {rmse:.4f}, MAE = {mae:.4f}, R² = {r2:.4f}")
        comparison_results.append({
            "model_name": name,
            "rmse": round(rmse, 4),
            "mae": round(mae, 4),
            "r2": round(r2, 4)
        })

    # Convert to DataFrame for analysis and ranking
    df_metrics = pd.DataFrame(comparison_results)
    # Best model has lowest RMSE and highest R²
    df_metrics = df_metrics.sort_values(by=["rmse", "r2"], ascending=[True, False]).reset_index(drop=True)
    best_model_name = df_metrics.iloc[0]["model_name"]
    best_rmse = df_metrics.iloc[0]["rmse"]
    best_r2 = df_metrics.iloc[0]["r2"]

    logger.info("=================================================================")
    logger.info("Model Comparison Table:")
    print("\n" + df_metrics.to_string(index=False) + "\n")
    logger.info(f"Selected Best Model: {best_model_name} (RMSE={best_rmse}, R²={best_r2})")
    logger.info("=================================================================")

    # 5. Persist Best Model Pipeline
    best_pipeline = trained_pipelines[best_model_name]
    best_model_dir = MODELS_DIR / "best_model"
    logger.info(f"Persisting best fitted pipeline to {best_model_dir}...")
    
    # Overwrite if exists
    best_pipeline.write().overwrite().save(str(best_model_dir))

    # 6. Save Evaluation Metrics and Artifacts
    csv_metrics_path = METRICS_OUTPUT_DIR / "evaluation_summary.csv"
    df_metrics.to_csv(csv_metrics_path, index=False)
    logger.info(f"Saved evaluation metrics CSV to {csv_metrics_path}")

    # Generate sample predictions CSV for auditing
    best_predictions = best_pipeline.transform(test_df)
    preds_pd = best_predictions.select(
        "Year", "State", "Season", "Area", "Annual_Rainfall", "Fertilizer", "Pesticide", "Yield", "prediction"
    ).limit(50).toPandas()
    preds_pd.rename(columns={"prediction": "Predicted_Yield", "Yield": "Actual_Yield"}, inplace=True)
    preds_csv = PROJECT_ROOT / "outputs" / "predictions" / "sample_predictions.csv"
    preds_csv.parent.mkdir(parents=True, exist_ok=True)
    preds_pd.to_csv(preds_csv, index=False)
    logger.info(f"Saved sample test predictions to {preds_csv}")

    metadata = {
        "best_model_name": best_model_name,
        "selection_criteria": "Lowest Root Mean Squared Error (RMSE) and highest Coefficient of Determination (R²) on holdout test set.",
        "train_samples": train_count,
        "test_samples": test_count,
        "metrics_summary": comparison_results,
        "feature_details": get_feature_metadata()
    }

    metadata_path = MODELS_DIR / "model_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)
    logger.info(f"Saved model metadata to {metadata_path}")

    return best_model_name, df_metrics

if __name__ == "__main__":
    spark = get_spark_session("ModelTrainingRunner")
    train_and_evaluate_models(spark)
    spark.stop()
