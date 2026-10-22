"""
Cotton Yield Prediction System
Loads the trained end-to-end Spark MLlib PipelineModel and produces yield predictions
guaranteeing identical preprocessing and feature transformations as training.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import logging
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from config.config import get_spark_session, MODELS_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Predictor")

class CottonYieldPredictor:
    def __init__(self, model_path=None, spark=None):
        self.spark = spark or get_spark_session(app_name="CottonYieldInference")
        self.model_path = Path(model_path) if model_path else (MODELS_DIR / "best_model")
        
        if not self.model_path.exists():
            raise FileNotFoundError(f"Trained model not found at {self.model_path}. Run train_model.py first.")

        logger.info(f"Loading trained Spark MLlib PipelineModel from: {self.model_path}")
        self.model = PipelineModel.load(str(self.model_path))
        logger.info("PipelineModel loaded successfully.")

    def predict_single(self, input_data: dict) -> dict:
        """
        Predicts cotton yield for a single input record dictionary.
        Required keys: State, Season, Year, Area, Annual_Rainfall, Fertilizer, Pesticide
        """
        # Input Validation
        required = ["State", "Season", "Year", "Area", "Annual_Rainfall", "Fertilizer", "Pesticide"]
        for key in required:
            if key not in input_data:
                raise ValueError(f"Missing required input feature: '{key}'")

        # Compute engineered features (same formula as training)
        area = float(input_data["Area"])
        if area <= 0:
            raise ValueError("Area must be strictly greater than 0.")

        fert = float(input_data["Fertilizer"])
        pest = float(input_data["Pesticide"])
        fert_per_area = fert / (area + 1e-5)
        pest_per_area = pest / (area + 1e-5)

        row_data = [(
            int(input_data["Year"]),
            str(input_data["State"]).strip(),
            str(input_data["Season"]).strip(),
            area,
            float(input_data["Annual_Rainfall"]),
            fert,
            pest,
            fert_per_area,
            pest_per_area
        )]

        schema = StructType([
            StructField("Year", IntegerType(), False),
            StructField("State", StringType(), False),
            StructField("Season", StringType(), False),
            StructField("Area", DoubleType(), False),
            StructField("Annual_Rainfall", DoubleType(), False),
            StructField("Fertilizer", DoubleType(), False),
            StructField("Pesticide", DoubleType(), False),
            StructField("Fertilizer_per_Area", DoubleType(), False),
            StructField("Pesticide_per_Area", DoubleType(), False)
        ])

        input_df = self.spark.createDataFrame(row_data, schema=schema)
        
        # Transform via fitted pipeline
        pred_df = self.model.transform(input_df)
        predicted_yield = float(pred_df.select("prediction").collect()[0]["prediction"])

        # Physical boundary clamp (yield cannot be negative)
        predicted_yield = max(0.0, round(predicted_yield, 4))

        estimated_production = round(predicted_yield * area, 2)

        return {
            "status": "success",
            "state": input_data["State"],
            "season": input_data["Season"],
            "year": int(input_data["Year"]),
            "area_hectares": area,
            "predicted_yield_tonnes_per_ha": predicted_yield,
            "estimated_total_production_tonnes": estimated_production,
            "model_used": "Spark MLlib Random Forest Regressor Pipeline"
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Predict Cotton Yield using trained Spark ML model")
    parser.add_argument("--state", type=str, default="Punjab", help="Indian State")
    parser.add_argument("--season", type=str, default="Kharif", help="Agricultural Season")
    parser.add_argument("--year", type=int, default=2026, help="Harvest Year")
    parser.add_argument("--area", type=float, default=520000.0, help="Area in hectares")
    parser.add_argument("--rainfall", type=float, default=510.0, help="Annual rainfall in mm")
    parser.add_argument("--fertilizer", type=float, default=71000000.0, help="Fertilizer in kg")
    parser.add_argument("--pesticide", type=float, default=135000.0, help="Pesticide in kg")
    args = parser.parse_args()

    predictor = CottonYieldPredictor()
    test_input = {
        "State": args.state,
        "Season": args.season,
        "Year": args.year,
        "Area": args.area,
        "Annual_Rainfall": args.rainfall,
        "Fertilizer": args.fertilizer,
        "Pesticide": args.pesticide
    }

    result = predictor.predict_single(test_input)
    print("\n--- COTTON YIELD PREDICTION RESULT ---")
    for k, v in result.items():
        print(f"  {k}: {v}")
