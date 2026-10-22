-- =============================================================================
-- Big Data Cotton Yield Prediction System
-- Script: analysis_queries.sql
-- Purpose: Analytical Hive SQL Queries over Actual India Crop / Cotton Dataset
-- =============================================================================

USE cotton_yield_db;

-- Query 1: Total Records in Raw Dataset
SELECT COUNT(*) AS total_raw_records FROM crop_yield_raw;

-- Query 2: Summary of Cotton Records specifically
SELECT 
    COUNT(*) AS total_cotton_records,
    ROUND(AVG(Yield), 4) AS avg_cotton_yield,
    ROUND(MIN(Yield), 4) AS min_cotton_yield,
    ROUND(MAX(Yield), 4) AS max_cotton_yield,
    ROUND(STDDEV(Yield), 4) AS stddev_cotton_yield
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%';

-- Query 3: State-wise Cotton Yield Ranking (Top 10 States by Average Yield)
SELECT 
    State,
    COUNT(*) AS total_records,
    ROUND(AVG(Yield), 4) AS avg_yield,
    ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall,
    ROUND(AVG(Fertilizer / Area), 2) AS avg_fertilizer_intensity
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%'
GROUP BY State
HAVING COUNT(*) >= 5
ORDER BY avg_yield DESC
LIMIT 10;

-- Query 4: Season-wise Cotton Yield Analysis
SELECT 
    Season,
    COUNT(*) AS records_count,
    ROUND(AVG(Yield), 4) AS avg_yield,
    ROUND(AVG(Area), 2) AS avg_area,
    ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%'
GROUP BY Season
ORDER BY records_count DESC;

-- Query 5: Yearly Progression of Average Cotton Yield (2000-2026)
SELECT 
    Year,
    ROUND(AVG(Yield), 4) AS avg_yield,
    ROUND(AVG(Annual_Rainfall), 2) AS avg_rainfall,
    ROUND(SUM(Area), 2) AS total_cotton_area
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%'
GROUP BY Year
ORDER BY Year ASC;

-- Query 6: Input Usage Statistics (Fertilizer & Pesticide Quartiles)
SELECT 
    ROUND(MIN(Fertilizer), 2) AS min_fertilizer,
    ROUND(AVG(Fertilizer), 2) AS avg_fertilizer,
    ROUND(MAX(Fertilizer), 2) AS max_fertilizer,
    ROUND(MIN(Pesticide), 2) AS min_pesticide,
    ROUND(AVG(Pesticide), 2) AS avg_pesticide,
    ROUND(MAX(Pesticide), 2) AS max_pesticide
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%';

-- Query 7: Missing Value Verification Check
SELECT 
    SUM(CASE WHEN Year IS NULL THEN 1 ELSE 0 END) AS null_years,
    SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS null_states,
    SUM(CASE WHEN Season IS NULL THEN 1 ELSE 0 END) AS null_seasons,
    SUM(CASE WHEN Area IS NULL OR Area <= 0 THEN 1 ELSE 0 END) AS invalid_area,
    SUM(CASE WHEN Yield IS NULL OR Yield < 0 THEN 1 ELSE 0 END) AS invalid_yield
FROM crop_yield_raw
WHERE LOWER(Crop) LIKE '%cotton%';
