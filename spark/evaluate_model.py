"""
Model Evaluation and Diagnostic Reporting
Calculates regression residuals, generates actual vs predicted plots,
and exports metric comparison tables.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from config.config import METRICS_OUTPUT_DIR, PREDICTIONS_OUTPUT_DIR, OUTPUTS_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("EvaluateModel")

def generate_evaluation_diagnostics():
    """Generates residual plots and actual vs predicted visual analytics."""
    logger.info("Generating model evaluation diagnostics...")
    preds_file = PREDICTIONS_OUTPUT_DIR / "sample_predictions.csv"
    if not preds_file.exists():
        logger.warning(f"Prediction file {preds_file} not found. Run train_model.py first.")
        return

    df = pd.read_csv(preds_file)
    actual = df["Actual_Yield"]
    predicted = df["Predicted_Yield"]
    residuals = actual - predicted

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # 1. Actual vs Predicted
    axes[0].scatter(actual, predicted, color="#1e40af", alpha=0.7, edgecolors="k", s=50)
    min_val = min(actual.min(), predicted.min())
    max_val = max(actual.max(), predicted.max())
    axes[0].plot([min_val, max_val], [min_val, max_val], color="crimson", linestyle="--", linewidth=2, label="Ideal 45° Line")
    axes[0].set_title("Actual vs Predicted Cotton Yield (Holdout Test Set)", fontsize=12, fontweight="bold")
    axes[0].set_xlabel("Actual Yield (tonnes/ha)")
    axes[0].set_ylabel("Predicted Yield (tonnes/ha)")
    axes[0].legend()
    axes[0].grid(True, linestyle=":", alpha=0.6)

    # 2. Residual Distribution
    sns.histplot(residuals, kde=True, ax=axes[1], color="#059669", bins=15)
    axes[1].axvline(0, color="crimson", linestyle="--", linewidth=1.5)
    axes[1].set_title("Model Residuals Distribution (Actual - Predicted)", fontsize=12, fontweight="bold")
    axes[1].set_xlabel("Residual (Yield Error)")
    axes[1].set_ylabel("Count")

    diag_path = METRICS_OUTPUT_DIR / "residual_diagnostics.png"
    fig.savefig(diag_path, dpi=300)
    plt.close(fig)
    logger.info(f"Saved residual diagnostic plots to {diag_path}")

if __name__ == "__main__":
    generate_evaluation_diagnostics()
