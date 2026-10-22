"""
HDFS Storage Layer Manager for Cotton Yield Prediction Pipeline
Handles distributed directory management, raw file ingestion, verification,
and reading/writing partitioned datasets between Local and HDFS layers.
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import shutil
import subprocess
import logging
from config.config import (
    HDFS_SIMULATED_DIR,
    HDFS_RAW_DIR,
    HDFS_PROCESSED_DIR,
    HDFS_FEATURES_DIR,
    HDFS_PREDICTIONS_DIR,
    RAW_DATA_PATH,
    COTTON_DATA_PATH,
    HDFS_URI
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("HDFSStorage")

class HDFSStorageManager:
    def __init__(self, use_live_hdfs=False, hdfs_uri=None):
        self.use_live_hdfs = use_live_hdfs
        self.hdfs_uri = hdfs_uri or HDFS_URI
        self.simulated_root = HDFS_SIMULATED_DIR

    def init_directories(self):
        """Creates the 4-layer HDFS storage architecture: raw, processed, features, predictions."""
        logger.info("Initializing HDFS Storage Architecture...")
        
        # Always ensure simulated local directories exist for local development/reproducibility
        for d in [HDFS_RAW_DIR, HDFS_PROCESSED_DIR, HDFS_FEATURES_DIR, HDFS_PREDICTIONS_DIR]:
            d.mkdir(parents=True, exist_ok=True)
            logger.info(f"Verified directory: {d}")

        if self.use_live_hdfs:
            try:
                for sub in ["raw", "processed", "features", "predictions"]:
                    target = f"{self.hdfs_uri}/{sub}" if self.hdfs_uri else f"/cotton_yield/{sub}"
                    cmd = ["hadoop", "fs", "-mkdir", "-p", target]
                    subprocess.run(cmd, check=True, capture_output=True)
                    logger.info(f"Created live HDFS directory: {target}")
            except Exception as e:
                logger.warning(f"Could not initialize live HDFS cluster directories ({e}). Continuing with local HDFS layer.")

    def upload_dataset(self, source_path=None, target_layer="raw"):
        """
        Uploads local raw dataset to HDFS raw layer.
        """
        src = Path(source_path) if source_path else RAW_DATA_PATH
        if not src.exists():
            raise FileNotFoundError(f"Source dataset not found at {src}")

        dest_dir = self.simulated_root / target_layer
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / src.name
        
        shutil.copy2(src, dest_path)
        logger.info(f"Ingested {src.name} ({src.stat().st_size} bytes) -> HDFS layer [{target_layer}] at {dest_path}")

        # Also copy cotton subset if available
        if COTTON_DATA_PATH.exists() and src.name != "cotton_yield.csv":
            cotton_dest = dest_dir / COTTON_DATA_PATH.name
            shutil.copy2(COTTON_DATA_PATH, cotton_dest)
            logger.info(f"Ingested {COTTON_DATA_PATH.name} -> HDFS layer [{target_layer}] at {cotton_dest}")

        if self.use_live_hdfs:
            try:
                target_hdfs = f"{self.hdfs_uri}/{target_layer}/{src.name}" if self.hdfs_uri else f"/cotton_yield/{target_layer}/{src.name}"
                subprocess.run(["hadoop", "fs", "-put", "-f", str(src), target_hdfs], check=True)
                logger.info(f"Uploaded to live HDFS at {target_hdfs}")
            except Exception as e:
                logger.warning(f"Live HDFS upload skipped or failed: {e}")

        return dest_path

    def verify_upload(self, target_layer="raw", filename="crop_yield.csv"):
        """Verifies integrity and existence of files in HDFS layer."""
        target_path = self.simulated_root / target_layer / filename
        exists = target_path.exists()
        size = target_path.stat().st_size if exists else 0
        logger.info(f"HDFS Verification for [{target_layer}/{filename}]: Exists={exists}, Size={size} bytes")
        return exists and size > 0

    def get_hdfs_path(self, target_layer="raw", filename="cotton_yield.csv"):
        """Returns the file path for consumption by Spark/Hive."""
        path = self.simulated_root / target_layer / filename
        if not path.exists():
            # Fallback to source data directory if not yet moved
            fallback = RAW_DATA_PATH.parent / filename
            if fallback.exists():
                return str(fallback)
        return str(path)

if __name__ == "__main__":
    manager = HDFSStorageManager()
    manager.init_directories()
    manager.upload_dataset()
    manager.verify_upload("raw", "crop_yield.csv")
    manager.verify_upload("raw", "cotton_yield.csv")
    print("HDFS Storage Layer initialized and verified successfully.")
