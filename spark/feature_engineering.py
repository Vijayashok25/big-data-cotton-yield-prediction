"""
Spark ML Feature Engineering Pipeline
Constructs distributed feature transformations using StringIndexer,
OneHotEncoder, VectorAssembler, and StandardScaler.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler
)
from config.config import (
    CATEGORICAL_FEATURES,
    NUMERICAL_FEATURES,
    ENGINEERED_FEATURES,
    TARGET_VARIABLE,
    EXCLUDED_FEATURES
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("FeatureEngineering")

def build_feature_pipeline():
    """
    Constructs a PySpark ML Pipeline stages list for feature transformation.
    """
    logger.info("Building PySpark ML Feature Transformation Pipeline...")
    stages = []

    # 1. String Indexing for Categorical Columns
    index_output_cols = [f"{col}_Index" for col in CATEGORICAL_FEATURES]
    string_indexer = StringIndexer(
        inputCols=CATEGORICAL_FEATURES,
        outputCols=index_output_cols,
        handleInvalid="keep"
    )
    stages.append(string_indexer)

    # 2. One-Hot Encoding
    vec_output_cols = [f"{col}_Vec" for col in CATEGORICAL_FEATURES]
    one_hot_encoder = OneHotEncoder(
        inputCols=index_output_cols,
        outputCols=vec_output_cols,
        handleInvalid="keep"
    )
    stages.append(one_hot_encoder)

    # 3. Vector Assembly
    assembled_inputs = vec_output_cols + NUMERICAL_FEATURES + ENGINEERED_FEATURES
    logger.info(f"Assembling Features: Categorical Encodings + {NUMERICAL_FEATURES} + {ENGINEERED_FEATURES}")
    vector_assembler = VectorAssembler(
        inputCols=assembled_inputs,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    stages.append(vector_assembler)

    # 4. Standard Scaling
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False  # withMean=False preserves sparsity of one-hot encoded vectors
    )
    stages.append(scaler)

    pipeline = Pipeline(stages=stages)
    logger.info("Feature engineering pipeline stages constructed successfully.")
    return pipeline

def get_feature_metadata():
    """Returns documentation dictionary detailing features and reasoning."""
    return {
        "target_variable": TARGET_VARIABLE,
        "categorical_features": CATEGORICAL_FEATURES,
        "numerical_features": NUMERICAL_FEATURES,
        "engineered_features": ENGINEERED_FEATURES,
        "pipeline_stages": [
            "StringIndexer (State, Season)",
            "OneHotEncoder (State_Index, Season_Index)",
            "VectorAssembler (State_Vec, Season_Vec, Numerical Features, Engineered Intensities)",
            "StandardScaler (Scaling assembled vectors with unit standard deviation)"
        ],
        "excluded_features": EXCLUDED_FEATURES
    }

if __name__ == "__main__":
    from config.config import get_spark_session
    from spark.data_ingestion import ingest_data
    from spark.preprocessing import preprocess_cotton_data

    spark = get_spark_session("FeatureEngineeringTest")
    raw_df = ingest_data(spark)
    clean_df = preprocess_cotton_data(raw_df)

    pipeline = build_feature_pipeline()
    pipeline_model = pipeline.fit(clean_df)
    features_df = pipeline_model.transform(clean_df)

    logger.info("Transformed Features Sample:")
    features_df.select("State", "Season", "features", "Yield").show(5, truncate=False)
    spark.stop()
