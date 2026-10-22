-- =============================================================================
-- Big Data Cotton Yield Prediction System
-- Script: create_table.sql
-- Purpose: Define Hive Tables matching EXACT dataset schema
-- =============================================================================

USE cotton_yield_db;

-- 1. Raw Crop Ingestion Table (External Table)
CREATE TABLE IF NOT EXISTS crop_yield_raw (
    `Year` INT COMMENT 'Harvest calendar year (2000-2026)',
    `State` STRING COMMENT 'Indian State / Union Territory',
    `Crop` STRING COMMENT 'Crop name',
    `Season` STRING COMMENT 'Agricultural crop season (Kharif, Rabi, Summer, Whole Year)',
    `Area` DOUBLE COMMENT 'Cultivated land area (hectares)',
    `Production` DOUBLE COMMENT 'Gross harvest production',
    `Annual_Rainfall` DOUBLE COMMENT 'Annual rainfall (mm)',
    `Fertilizer` DOUBLE COMMENT 'Fertilizer consumption (kg)',
    `Pesticide` DOUBLE COMMENT 'Pesticide usage (kg)',
    `Yield` DOUBLE COMMENT 'Crop yield output'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/cotton_yield/raw/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 2. Processed Cotton Yield Table (Optimized Structured Storage)
CREATE TABLE IF NOT EXISTS cotton_yield_cleaned (
    `Year` INT,
    `State` STRING,
    `Season` STRING,
    `Area` DOUBLE,
    `Annual_Rainfall` DOUBLE,
    `Fertilizer` DOUBLE,
    `Pesticide` DOUBLE,
    `Fertilizer_per_Area` DOUBLE,
    `Pesticide_per_Area` DOUBLE,
    `Yield` DOUBLE
)
STORED AS PARQUET
LOCATION '/cotton_yield/processed/cotton_parquet/';
