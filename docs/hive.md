# Apache Hive Data Warehousing Layer

## Why Apache Hive?

Apache Hive provides data warehousing and schema-on-read querying over distributed storage. Rather than writing low-level MapReduce or Spark scripts for every ad-hoc query, Hive allows data analysts to execute familiar SQL (HiveQL) queries against massive datasets stored in HDFS.

---

## Database & Table Design

### 1. Database Creation
```sql
CREATE DATABASE IF NOT EXISTS cotton_yield_db
COMMENT 'Hive Database for Indian Agriculture Crop and Cotton Yield Analytics';
```

### 2. External Table: `crop_yield_raw`
Points directly to the HDFS raw directory without duplicating underlying data:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS crop_yield_raw (
    `Year` INT,
    `State` STRING,
    `Crop` STRING,
    `Season` STRING,
    `Area` DOUBLE,
    `Production` DOUBLE,
    `Annual_Rainfall` DOUBLE,
    `Fertilizer` DOUBLE,
    `Pesticide` DOUBLE,
    `Yield` DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/cotton_yield/raw/'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

---

## Analytical Hive Queries Executed

The system implements 7 core analytical queries:
1. **Total Records Count**: Validates ingestion of all 21,750 crop records.
2. **Cotton Statistical Baseline**: Computes count (582), average yield (1.5452), min (0.0), max (5.295), and standard deviation (1.1656).
3. **State-Level Yield Ranking**: Groups by `State` with `HAVING COUNT(*) >= 5` to identify high-productivity agricultural zones (Punjab: 3.54 t/ha, Gujarat: 3.06 t/ha, West Bengal: 3.01 t/ha).
4. **Seasonal Distribution**: Groups by `Season` (Kharif represents 94.5% of cotton output).
5. **Decadal Yield Progression**: Groups by `Year` (2000–2026) to track national productivity trends.
6. **Chemical Input Intensity**: Computes minimum, median, and maximum fertilizer and pesticide intensity.
7. **Data Quality Audit**: Validates that zero NULLs exist across critical primary features.
