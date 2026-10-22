"""
Flask Dashboard & REST API Application
Provides interactive monitoring, model evaluation comparison, EDA visualizations,
and a production REST API for real-time Cotton Yield Prediction.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import logging
from flask import Flask, render_template, request, jsonify, send_from_directory
import pandas as pd

from config.config import (
    MODELS_DIR,
    METRICS_OUTPUT_DIR,
    EDA_OUTPUT_DIR,
    PREDICTIONS_OUTPUT_DIR,
    RAW_DATA_PATH,
    COTTON_DATA_PATH
)
from mysql.connection import DatabaseManager
from spark.predict import CottonYieldPredictor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("DashboardApp")

app = Flask(
    __name__,
    template_folder=str(PROJECT_ROOT / "dashboard" / "templates"),
    static_folder=str(PROJECT_ROOT / "dashboard" / "static")
)

# Global lazy-loaded predictor and database manager
predictor_instance = None
db_manager = DatabaseManager()

def get_predictor():
    global predictor_instance
    if predictor_instance is None:
        try:
            logger.info("Initializing Cotton Yield Predictor instance...")
            predictor_instance = CottonYieldPredictor()
        except Exception as e:
            logger.error(f"Failed to initialize predictor: {e}")
            predictor_instance = None
    return predictor_instance

@app.route("/")
def index():
    """Renders main dashboard overview page."""
    # 1. Load Metrics
    metrics_file = METRICS_OUTPUT_DIR / "evaluation_summary.csv"
    metrics_list = []
    if metrics_file.exists():
        df_m = pd.read_csv(metrics_file)
        metrics_list = df_m.to_dict(orient="records")

    # 2. Load Model Metadata
    metadata_file = MODELS_DIR / "model_metadata.json"
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file) as f:
            metadata = json.load(f)

    # 3. Load EDA Summary
    eda_file = EDA_OUTPUT_DIR / "eda_summary_report.json"
    eda_summary = {}
    if eda_file.exists():
        with open(eda_file) as f:
            eda_summary = json.load(f)

    # 4. Fetch recent predictions from database
    recent_preds = []
    try:
        recent_preds = db_manager.execute_query(
            "SELECT state, season, harvest_year, area_hectares, predicted_yield, estimated_production_tonnes, predicted_at "
            "FROM cotton_predictions ORDER BY prediction_id DESC LIMIT 8"
        )
    except Exception as e:
        logger.warning(f"Could not load recent predictions from DB: {e}")

    # Available Indian Cotton States
    states = [
        "Andhra Pradesh", "Gujarat", "Haryana", "Karnataka", "Madhya Pradesh",
        "Maharashtra", "Odisha", "Punjab", "Rajasthan", "Tamil Nadu",
        "Telangana", "Uttar Pradesh", "West Bengal"
    ]
    seasons = ["Kharif", "Whole Year", "Summer", "Rabi"]

    return render_template(
        "index.html",
        metrics=metrics_list,
        metadata=metadata,
        eda=eda_summary,
        recent_predictions=recent_preds,
        states=states,
        seasons=seasons,
        db_mode="MySQL" if db_manager.use_mysql else "SQLite (Local Mirror)"
    )

@app.route("/eda-images/<filename>")
def eda_images(filename):
    """Serves generated EDA chart images."""
    return send_from_directory(str(EDA_OUTPUT_DIR), filename)

# ==============================================================================
# REST API Endpoints
# ==============================================================================

@app.route("/api/health", methods=["GET"])
def api_health():
    """Health check endpoint."""
    return jsonify({
        "status": "UP",
        "service": "Big Data Cotton Yield Prediction System",
        "spark_available": True,
        "database_mode": "MySQL" if db_manager.use_mysql else "SQLite",
        "best_model_loaded": (get_predictor() is not None)
    }), 200

@app.route("/api/model-metrics", methods=["GET"])
def api_model_metrics():
    """Returns evaluation metrics for all trained models."""
    metrics_file = METRICS_OUTPUT_DIR / "evaluation_summary.csv"
    if not metrics_file.exists():
        return jsonify({"error": "Model metrics not found. Run model training first."}), 404

    df_m = pd.read_csv(metrics_file)
    metadata_file = MODELS_DIR / "model_metadata.json"
    meta = {}
    if metadata_file.exists():
        with open(metadata_file) as f:
            meta = json.load(f)

    return jsonify({
        "comparison": df_m.to_dict(orient="records"),
        "best_model": meta.get("best_model_name"),
        "selection_criteria": meta.get("selection_criteria"),
        "train_samples": meta.get("train_samples"),
        "test_samples": meta.get("test_samples")
    }), 200

@app.route("/api/predict", methods=["POST"])
def api_predict():
    """
    Accepts JSON input features and returns predicted cotton yield and production.
    Expected JSON payload:
    {
        "state": "Punjab",
        "season": "Kharif",
        "year": 2026,
        "area": 520000.0,
        "rainfall": 510.0,
        "fertilizer": 71000000.0,
        "pesticide": 135000.0
    }
    """
    data = request.get_json(silent=True) or request.form.to_dict()
    if not data:
        return jsonify({"error": "Invalid request. JSON or Form payload required."}), 400

    # Key normalization
    state = data.get("state") or data.get("State")
    season = data.get("season") or data.get("Season")
    year = data.get("year") or data.get("Year")
    area = data.get("area") or data.get("Area")
    rainfall = data.get("rainfall") or data.get("Annual_Rainfall")
    fertilizer = data.get("fertilizer") or data.get("Fertilizer")
    pesticide = data.get("pesticide") or data.get("Pesticide")

    # Validate presence
    missing = []
    for k, v in [("State", state), ("Season", season), ("Year", year), ("Area", area),
                 ("Annual_Rainfall", rainfall), ("Fertilizer", fertilizer), ("Pesticide", pesticide)]:
        if v is None:
            missing.append(k)
    if missing:
        return jsonify({"error": f"Missing required fields: {missing}"}), 400

    try:
        year = int(year)
        area = float(area)
        rainfall = float(rainfall)
        fertilizer = float(fertilizer)
        pesticide = float(pesticide)

        if area <= 0:
            return jsonify({"error": "Area must be strictly positive."}), 422
        if rainfall < 0 or fertilizer < 0 or pesticide < 0:
            return jsonify({"error": "Numerical inputs cannot be negative."}), 422

        input_payload = {
            "State": str(state),
            "Season": str(season),
            "Year": year,
            "Area": area,
            "Annual_Rainfall": rainfall,
            "Fertilizer": fertilizer,
            "Pesticide": pesticide
        }

        predictor = get_predictor()
        if predictor is None:
            return jsonify({"error": "Prediction engine is unavailable."}), 503

        result = predictor.predict_single(input_payload)

        # Store prediction into Relational DB Layer
        try:
            insert_query = """
                INSERT INTO cotton_predictions (
                    state, season, harvest_year, area_hectares, rainfall_mm,
                    fertilizer_kg, pesticide_kg, predicted_yield, estimated_production_tonnes, model_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            db_manager.execute_insert(insert_query, (
                state, season, year, area, rainfall, fertilizer, pesticide,
                result["predicted_yield_tonnes_per_ha"],
                result["estimated_total_production_tonnes"],
                "1.0-RF"
            ))
        except Exception as e:
            logger.warning(f"Failed to record prediction in DB: {e}")

        return jsonify(result), 200

    except ValueError as ve:
        return jsonify({"error": f"Invalid numerical value: {ve}"}), 422
    except Exception as e:
        logger.error(f"Inference error: {e}", exc_info=True)
        return jsonify({"error": f"Prediction failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
