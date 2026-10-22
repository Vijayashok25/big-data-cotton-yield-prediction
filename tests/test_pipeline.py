"""
Unit and Integration Test Suite for Cotton Yield Prediction Pipeline
Verifies dataset loading, preprocessing rules, feature generation,
database resilience, prediction engine, and REST API validation.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import unittest
import pandas as pd
from config.config import RAW_DATA_PATH, COTTON_DATA_PATH, EXCLUDED_FEATURES, get_spark_session
from hadoop.hdfs_storage import HDFSStorageManager
from mysql.connection import DatabaseManager
from spark.preprocessing import preprocess_cotton_data
from spark.feature_engineering import get_feature_metadata
from dashboard.app import app

class TestCottonYieldPipeline(unittest.TestCase):

    def test_01_dataset_files_exist(self):
        """Verifies dataset files are present on disk."""
        self.assertTrue(RAW_DATA_PATH.exists(), f"Raw dataset missing: {RAW_DATA_PATH}")
        self.assertTrue(COTTON_DATA_PATH.exists(), f"Cotton dataset missing: {COTTON_DATA_PATH}")
        
        df_raw = pd.read_csv(RAW_DATA_PATH)
        self.assertGreater(len(df_raw), 20000, "Raw dataset should contain > 20,000 rows.")
        self.assertIn("Yield", df_raw.columns, "Target column 'Yield' must be present.")
        self.assertIn("Crop", df_raw.columns, "Column 'Crop' must be present.")

    def test_02_hdfs_storage_manager(self):
        """Tests HDFS storage directory creation and verification."""
        mgr = HDFSStorageManager()
        mgr.init_directories()
        ingested = mgr.upload_dataset()
        self.assertTrue(ingested.exists())
        self.assertTrue(mgr.verify_upload("raw", "crop_yield.csv"))

    def test_03_preprocessing_and_data_leakage_prevention(self):
        """Verifies that 'Production' is explicitly dropped to prevent target leakage."""
        spark = get_spark_session("TestPreprocessing")
        try:
            df = spark.read.csv(str(RAW_DATA_PATH), header=True, inferSchema=True)
            clean_df = preprocess_cotton_data(df)
            
            # Check crop filtering
            self.assertNotIn("Production", clean_df.columns, "'Production' must be removed to avoid data leakage!")
            self.assertIn("Yield", clean_df.columns, "Target 'Yield' must remain.")
            self.assertIn("Fertilizer_per_Area", clean_df.columns, "Engineered feature must be present.")
            self.assertIn("Pesticide_per_Area", clean_df.columns, "Engineered feature must be present.")
        finally:
            spark.stop()

    def test_04_feature_metadata(self):
        """Tests feature metadata schema documentation."""
        meta = get_feature_metadata()
        self.assertEqual(meta["target_variable"], "Yield")
        self.assertIn("Production", meta["excluded_features"])
        self.assertIn("State", meta["categorical_features"])
        self.assertIn("Area", meta["numerical_features"])

    def test_05_database_manager_operations(self):
        """Tests relational storage connectivity and parameterized insert/select."""
        db = DatabaseManager()
        test_state = "TestState_UnitTest"
        
        insert_q = """
            INSERT INTO processed_summary (state, record_count, avg_yield, avg_rainfall, avg_fertilizer_intensity)
            VALUES (%s, %s, %s, %s, %s)
        """
        row_id = db.execute_insert(insert_q, (test_state, 10, 2.5, 800.0, 120.0))
        self.assertGreater(row_id, 0)

        select_q = "SELECT * FROM processed_summary WHERE state = %s"
        records = db.execute_query(select_q, (test_state,))
        self.assertGreater(len(records), 0)
        self.assertEqual(records[0]["state"], test_state)

    def test_06_rest_api_health(self):
        """Tests /api/health endpoint."""
        client = app.test_client()
        res = client.get("/api/health")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "UP")
        self.assertTrue(data["spark_available"])

    def test_07_rest_api_validation_error(self):
        """Tests API error handling on invalid / missing input."""
        client = app.test_client()
        
        # Missing fields test
        res_missing = client.post("/api/predict", json={"state": "Punjab"})
        self.assertEqual(res_missing.status_code, 400)

        # Invalid negative area test
        res_invalid = client.post("/api/predict", json={
            "state": "Punjab", "season": "Kharif", "year": 2026,
            "area": -100.0, "rainfall": 500, "fertilizer": 1000, "pesticide": 50
        })
        self.assertEqual(res_invalid.status_code, 422)

if __name__ == "__main__":
    unittest.main(verbosity=2)
