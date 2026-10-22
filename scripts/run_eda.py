"""
Exploratory Data Analysis (EDA) Pipeline for Cotton Yield Prediction
Generates statistical profiling and visual analytics based on the actual dataset.
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

from config.config import RAW_DATA_PATH, COTTON_DATA_PATH, EDA_OUTPUT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("EDA")

def run_eda():
    EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting Exploratory Data Analysis...")

    # Load dataset
    df_raw = pd.read_csv(RAW_DATA_PATH)
    logger.info(f"Loaded raw dataset with {len(df_raw)} records.")

    # Filter cotton subset
    df_cotton = df_raw[df_raw["Crop"].str.contains("cotton", case=False, na=False)].copy()
    logger.info(f"Filtered {len(df_cotton)} Cotton records across {df_cotton['State'].nunique()} states.")

    # Feature engineering for EDA insights
    df_cotton["Fertilizer_per_Area"] = df_cotton["Fertilizer"] / (df_cotton["Area"] + 1e-5)
    df_cotton["Pesticide_per_Area"] = df_cotton["Pesticide"] / (df_cotton["Area"] + 1e-5)

    sns.set_theme(style="whitegrid", palette="muted")
    plt.rcParams.update({"font.size": 11, "figure.autolayout": True})

    # 1. Target Yield Distribution & Outliers
    logger.info("Generating Chart 1: Target Yield Distribution...")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    sns.histplot(df_cotton["Yield"], kde=True, ax=axes[0], color="#2b5c8f", bins=25)
    mean_val = df_cotton["Yield"].mean()
    median_val = df_cotton["Yield"].median()
    axes[0].axvline(mean_val, color="red", linestyle="--", label=f"Mean: {mean_val:.2f}")
    axes[0].axvline(median_val, color="green", linestyle="-", label=f"Median: {median_val:.2f}")
    axes[0].set_title("Distribution of Cotton Yield", fontsize=13, fontweight="bold")
    axes[0].set_xlabel("Cotton Yield (Tonnes / Bales per Hectare)")
    axes[0].set_ylabel("Frequency")
    axes[0].legend()

    sns.boxplot(x=df_cotton["Yield"], ax=axes[1], color="#79addc")
    axes[1].set_title("Boxplot of Cotton Yield (Outlier Detection)", fontsize=13, fontweight="bold")
    axes[1].set_xlabel("Cotton Yield")
    
    fig.savefig(EDA_OUTPUT_DIR / "01_target_yield_distribution.png", dpi=300)
    plt.close(fig)

    # 2. Correlation Heatmap
    logger.info("Generating Chart 2: Correlation Heatmap...")
    numeric_cols = ["Year", "Area", "Annual_Rainfall", "Fertilizer", "Pesticide", "Fertilizer_per_Area", "Pesticide_per_Area", "Yield"]
    corr = df_cotton[numeric_cols].corr()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1, square=True, ax=ax, cbar_kws={"shrink": .8})
    ax.set_title("Feature Correlation Heatmap (Cotton Dataset)", fontsize=14, fontweight="bold", pad=12)
    fig.savefig(EDA_OUTPUT_DIR / "02_correlation_heatmap.png", dpi=300)
    plt.close(fig)

    # 3. State-wise Cotton Yield Ranking
    logger.info("Generating Chart 3: State-wise Cotton Yield Ranking...")
    state_yield = df_cotton.groupby("State")["Yield"].agg(["mean", "count"]).reset_index()
    state_yield = state_yield[state_yield["count"] >= 5].sort_values("mean", ascending=False)

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(data=state_yield, y="State", x="mean", palette="Blues_r", ax=ax)
    ax.set_title("Average Cotton Yield by Indian State (Min 5 Samples)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Mean Cotton Yield")
    ax.set_ylabel("State")
    for i, v in enumerate(state_yield["mean"]):
        ax.text(v + 0.05, i, f"{v:.2f}", va="center", fontsize=9, fontweight="bold")
    fig.savefig(EDA_OUTPUT_DIR / "03_state_wise_cotton_yield.png", dpi=300)
    plt.close(fig)

    # 4. Annual Rainfall vs Cotton Yield
    logger.info("Generating Chart 4: Annual Rainfall vs Cotton Yield...")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.regplot(data=df_cotton, x="Annual_Rainfall", y="Yield", scatter_kws={"alpha": 0.6, "color": "#1f77b4"}, line_kws={"color": "crimson", "linewidth": 2}, ax=ax)
    ax.set_title("Annual Rainfall (mm) vs Cotton Yield", fontsize=14, fontweight="bold")
    ax.set_xlabel("Annual Rainfall (mm)")
    ax.set_ylabel("Cotton Yield")
    fig.savefig(EDA_OUTPUT_DIR / "04_rainfall_vs_yield.png", dpi=300)
    plt.close(fig)

    # 5. Fertilizer Intensity vs Cotton Yield
    logger.info("Generating Chart 5: Fertilizer Intensity vs Cotton Yield...")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.scatterplot(data=df_cotton, x="Fertilizer_per_Area", y="Yield", hue="State", alpha=0.7, ax=ax, legend=False)
    ax.set_title("Fertilizer Intensity (kg/hectare) vs Cotton Yield", fontsize=14, fontweight="bold")
    ax.set_xlabel("Fertilizer per Hectare (kg/ha)")
    ax.set_ylabel("Cotton Yield")
    fig.savefig(EDA_OUTPUT_DIR / "05_fertilizer_intensity_vs_yield.png", dpi=300)
    plt.close(fig)

    # 6. Yearly Yield Progression (2000-2026)
    logger.info("Generating Chart 6: Yearly Progression of Cotton Yield...")
    yearly = df_cotton.groupby("Year")["Yield"].agg(["mean", "std"]).reset_index()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(yearly["Year"], yearly["mean"], marker="o", color="#2a9d8f", linewidth=2.5, label="National Mean Yield")
    ax.fill_between(yearly["Year"], yearly["mean"] - yearly["std"], yearly["mean"] + yearly["std"], color="#2a9d8f", alpha=0.2, label="±1 Std Deviation")
    ax.set_title("Temporal Progression of Cotton Yield in India (2000-2026)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Harvest Year")
    ax.set_ylabel("Average Yield")
    ax.set_xticks(yearly["Year"][::2])
    ax.legend()
    fig.savefig(EDA_OUTPUT_DIR / "06_yearly_yield_trend.png", dpi=300)
    plt.close(fig)

    # 7. Season vs Yield Boxplot
    logger.info("Generating Chart 7: Agricultural Season vs Cotton Yield...")
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.boxplot(data=df_cotton, x="Season", y="Yield", palette="Set2", ax=ax)
    ax.set_title("Cotton Yield Variation Across Agricultural Seasons", fontsize=13, fontweight="bold")
    ax.set_xlabel("Agricultural Season")
    ax.set_ylabel("Cotton Yield")
    fig.savefig(EDA_OUTPUT_DIR / "07_season_yield_distribution.png", dpi=300)
    plt.close(fig)

    # Summary JSON Metrics
    summary = {
        "dataset_name": "india-crop-yield-prediction",
        "total_crop_records": len(df_raw),
        "total_cotton_records": len(df_cotton),
        "distinct_states": int(df_cotton["State"].nunique()),
        "year_min": int(df_cotton["Year"].min()),
        "year_max": int(df_cotton["Year"].max()),
        "yield_stats": {
            "mean": float(df_cotton["Yield"].mean()),
            "std": float(df_cotton["Yield"].std()),
            "min": float(df_cotton["Yield"].min()),
            "p25": float(df_cotton["Yield"].quantile(0.25)),
            "median": float(df_cotton["Yield"].median()),
            "p75": float(df_cotton["Yield"].quantile(0.75)),
            "max": float(df_cotton["Yield"].max())
        },
        "top_5_states_by_yield": state_yield.head(5)[["State", "mean"]].to_dict(orient="records"),
        "seasons_distribution": df_cotton["Season"].value_counts().to_dict(),
        "missing_values": df_cotton.isnull().sum().to_dict()
    }

    summary_path = EDA_OUTPUT_DIR / "eda_summary_report.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=4)

    logger.info(f"EDA Completed! 7 charts and summary report saved to {EDA_OUTPUT_DIR}")

if __name__ == "__main__":
    run_eda()
