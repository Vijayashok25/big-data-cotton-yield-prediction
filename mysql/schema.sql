-- =============================================================================
-- Big Data Cotton Yield Prediction System
-- Script: schema.sql
-- Purpose: Relational Storage & Reporting Schema for MySQL
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cotton_yield
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE cotton_yield;

-- 1. Model Registry Table
CREATE TABLE IF NOT EXISTS model_registry (
    model_id INT AUTO_INCREMENT PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    algorithm VARCHAR(100) NOT NULL,
    train_samples INT NOT NULL,
    test_samples INT NOT NULL,
    is_best_model BOOLEAN DEFAULT FALSE,
    model_path VARCHAR(255) NOT NULL,
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Model Evaluation Metrics Table
CREATE TABLE IF NOT EXISTS model_metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    rmse DOUBLE NOT NULL,
    mae DOUBLE NOT NULL,
    r2 DOUBLE NOT NULL,
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Inference & Production Predictions Table
CREATE TABLE IF NOT EXISTS cotton_predictions (
    prediction_id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100) NOT NULL,
    season VARCHAR(50) NOT NULL,
    harvest_year INT NOT NULL,
    area_hectares DOUBLE NOT NULL,
    rainfall_mm DOUBLE NOT NULL,
    fertilizer_kg DOUBLE NOT NULL,
    pesticide_kg DOUBLE NOT NULL,
    predicted_yield DOUBLE NOT NULL,
    estimated_production_tonnes DOUBLE NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Processed Analytics Summary Table
CREATE TABLE IF NOT EXISTS processed_summary (
    summary_id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100) NOT NULL,
    record_count INT NOT NULL,
    avg_yield DOUBLE NOT NULL,
    avg_rainfall DOUBLE NOT NULL,
    avg_fertilizer_intensity DOUBLE NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
