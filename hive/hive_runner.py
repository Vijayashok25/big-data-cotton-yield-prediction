"""
Hive SQL Execution Runner using PySpark SQL Engine
Enables execution of Hive DDL, DML, and Analytical Queries
matching exact dataset schema and persisting analytics tables.
"""

import sys
import shutil
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging
from config.config import get_spark_session, RAW_DATA_PATH, HIVE_DATABASE, OUTPUTS_DIR, SPARK_WAREHOUSE_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("HiveRunner")

def run_hive_pipeline():
    logger.info("Initializing SparkSession with Hive/Warehouse capabilities...")
    spark = get_spark_session(app_name="HiveAnalyticsRunner")

    # 1. Create Database
    logger.info(f"Creating Hive Database: {HIVE_DATABASE}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    spark.sql(f"USE {HIVE_DATABASE}")

    # 2. Ingest raw CSV into Hive Table
    logger.info(f"Ingesting dataset from {RAW_DATA_PATH} into Hive table 'crop_yield_raw'...")
    df_raw = spark.read.csv(str(RAW_DATA_PATH), header=True, inferSchema=True)
    df_raw.createOrReplaceTempView("crop_yield_raw")
    
    # Safely refresh managed table in database
    spark.sql("DROP TABLE IF EXISTS crop_yield_raw")
    table_dir = Path(SPARK_WAREHOUSE_DIR) / f"{HIVE_DATABASE}.db" / "crop_yield_raw"
    if table_dir.exists():
        shutil.rmtree(table_dir, ignore_errors=True)
    df_raw.write.mode("overwrite").saveAsTable("crop_yield_raw")

    logger.info("Verifying Hive Table 'crop_yield_raw' Schema:")
    df_raw.printSchema()

    # 3. Execute Analysis Queries
    sql_file = PROJECT_ROOT / "hive" / "analysis_queries.sql"
    logger.info(f"Executing Analytical Hive Queries from {sql_file.name}...")
    
    analytics_output = []
    
    # Query 1: Total records
    q1 = spark.sql("SELECT COUNT(*) AS total_raw_records FROM crop_yield_raw")
    total_records = q1.collect()[0]["total_raw_records"]
    logger.info(f"Query 1: Total Records in Hive = {total_records}")
    analytics_output.append(f"Query 1: Total Records in Hive = {total_records}")

    # Query 2: Summary of Cotton Yield
    q2 = spark.sql("""
        SELECT 
            COUNT(*) AS total_cotton_records,
            ROUND(AVG(Yield), 4) AS avg_cotton_yield,
            ROUND(MIN(Yield), 4) AS min_cotton_yield,
            ROUND(MAX(Yield), 4) AS max_cotton_yield,
            ROUND(STDDEV(Yield), 4) AS stddev_cotton_yield
        FROM crop_yield_raw
        WHERE LOWER(Crop) LIKE '%cotton%'
    """)
    logger.info("Query 2: Cotton Yield Summary Statistics:")
    q2.show()

    # Query 3: State-wise Cotton Yield Ranking
    q3 = spark.sql("""
        SELECT 
            State,
            COUNT(*) AS total_records,
            ROUND(AVG(Yield), 4) AS avg_yield,
            ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall,
            ROUND(AVG(Fertilizer / (Area + 0.001)), 2) AS avg_fertilizer_intensity
        FROM crop_yield_raw
        WHERE LOWER(Crop) LIKE '%cotton%'
        GROUP BY State
        HAVING COUNT(*) >= 5
        ORDER BY avg_yield DESC
        LIMIT 10
    """)
    logger.info("Query 3: Top 10 Cotton Producing States by Average Yield:")
    q3.show(truncate=False)

    # Query 4: Season-wise Analysis
    q4 = spark.sql("""
        SELECT 
            Season,
            COUNT(*) AS records_count,
            ROUND(AVG(Yield), 4) AS avg_yield,
            ROUND(AVG(Area), 2) AS avg_area,
            ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall
        FROM crop_yield_raw
        WHERE LOWER(Crop) LIKE '%cotton%'
        GROUP BY Season
        ORDER BY records_count DESC
    """)
    logger.info("Query 4: Season-wise Cotton Yield Analysis:")
    q4.show(truncate=False)

    # Query 5: Yearly progression
    q5 = spark.sql("""
        SELECT 
            Year,
            ROUND(AVG(Yield), 4) AS avg_yield,
            ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall,
            ROUND(SUM(Area), 2) AS total_cotton_area
        FROM crop_yield_raw
        WHERE LOWER(Crop) LIKE '%cotton%'
        GROUP BY Year
        ORDER BY Year ASC
    """)
    logger.info("Query 5: Yearly Progression of Cotton Yield (Sample):")
    q5.show(5)

    # Query 7: Missing Value Check
    q7 = spark.sql("""
        SELECT 
            SUM(CASE WHEN Year IS NULL THEN 1 ELSE 0 END) AS null_years,
            SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS null_states,
            SUM(CASE WHEN Season IS NULL THEN 1 ELSE 0 END) AS null_seasons,
            SUM(CASE WHEN Area IS NULL OR Area <= 0 THEN 1 ELSE 0 END) AS invalid_area,
            SUM(CASE WHEN Yield IS NULL OR Yield < 0 THEN 1 ELSE 0 END) AS invalid_yield
        FROM crop_yield_raw
        WHERE LOWER(Crop) LIKE '%cotton%'
    """)
    logger.info("Query 7: Data Quality & Missing Value Check:")
    q7.show()

    # Save summary to outputs
    out_file = OUTPUTS_DIR / "hive_analytics_summary.txt"
    with open(out_file, "w") as f:
        f.write("=== HIVE ANALYTICS QUERY RESULTS ===\n\n")
        f.write(f"Total Raw Records: {total_records}\n\n")
        f.write("Cotton Summary Statistics:\n")
        f.write(str(q2.toPandas().to_dict(orient="records")) + "\n\n")
        f.write("Top 10 States by Cotton Yield:\n")
        f.write(q3.toPandas().to_string(index=False) + "\n\n")
        f.write("Season-wise Cotton Breakdown:\n")
        f.write(q4.toPandas().to_string(index=False) + "\n\n")
        f.write("Data Quality Verification:\n")
        f.write(str(q7.toPandas().to_dict(orient="records")) + "\n")
    
    logger.info(f"Hive Analytics Results saved to {out_file}")
    spark.stop()
    logger.info("Hive pipeline completed successfully.")

if __name__ == "__main__":
    run_hive_pipeline()
