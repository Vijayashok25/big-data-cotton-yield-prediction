"""
Spark Data Preprocessing Module
Filters Cotton crops, handles missing and invalid values, removes duplicates,
performs safe column conversions, and engineers domain features in PySpark.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging
from pyspark.sql import functions as F
from config.config import get_spark_session, HDFS_PROCESSED_DIR, EXCLUDED_FEATURES
from spark.data_ingestion import ingest_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Preprocessing")

def preprocess_cotton_data(df, target_crop="cotton"):
    """
    Cleans raw DataFrame and prepares specialized Cotton yield dataset.
    """
    logger.info("Starting PySpark Data Cleaning & Preprocessing...")

    # 1. Filter Cotton crop
    logger.info(f"Filtering records matching crop: '{target_crop}'...")
    filtered_df = df.filter(F.lower(F.col("Crop")).like(f"%{target_crop}%"))
    cotton_count = filtered_df.count()
    logger.info(f"Filtered Cotton records: {cotton_count}")

    # 2. Trim string columns
    cleaned_df = filtered_df \
        .withColumn("State", F.trim(F.col("State"))) \
        .withColumn("Season", F.trim(F.col("Season")))

    # 3. Deduplication
    initial_rows = cleaned_df.count()
    deduped_df = cleaned_df.dropDuplicates(["Year", "State", "Season", "Area", "Yield"])
    deduped_rows = deduped_df.count()
    logger.info(f"Deduplication complete: {initial_rows} -> {deduped_rows} records ({initial_rows - deduped_rows} dropped).")

    # 4. Handle invalid and null values
    valid_df = deduped_df.filter(
        (F.col("Area").isNotNull()) & (F.col("Area") > 0) &
        (F.col("Yield").isNotNull()) & (F.col("Yield") >= 0) &
        (F.col("State").isNotNull()) &
        (F.col("Season").isNotNull()) &
        (F.col("Annual_Rainfall").isNotNull()) &
        (F.col("Fertilizer").isNotNull()) &
        (F.col("Pesticide").isNotNull())
    )

    # 5. Feature Engineering at distributed scale
    logger.info("Engineering input intensity features (Fertilizer/Area, Pesticide/Area)...")
    engineered_df = valid_df \
        .withColumn("Fertilizer_per_Area", F.col("Fertilizer") / (F.col("Area") + F.lit(1e-5))) \
        .withColumn("Pesticide_per_Area", F.col("Pesticide") / (F.col("Area") + F.lit(1e-5)))

    # 6. Explicit Data Leakage Elimination: Drop 'Production'
    logger.info(f"Preventing data leakage: Dropping columns {list(EXCLUDED_FEATURES.keys())}")
    for col_name, reason in EXCLUDED_FEATURES.items():
        if col_name in engineered_df.columns:
            logger.info(f"  -> Dropping '{col_name}': {reason}")
            engineered_df = engineered_df.drop(col_name)

    logger.info("Preprocessing complete. Final Schema:")
    engineered_df.printSchema()

    # Save to HDFS Processed layer in Parquet format
    output_path = HDFS_PROCESSED_DIR / "cotton_cleaned.parquet"
    logger.info(f"Persisting processed dataset to HDFS layer: {output_path}")
    engineered_df.write.mode("overwrite").parquet(str(output_path))

    return engineered_df

if __name__ == "__main__":
    spark = get_spark_session("PreprocessingTest")
    raw_df = ingest_data(spark)
    clean_df = preprocess_cotton_data(raw_df)
    clean_df.show(5)
    spark.stop()
