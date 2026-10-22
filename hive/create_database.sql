-- =============================================================================
-- Big Data Cotton Yield Prediction System
-- Script: create_database.sql
-- Purpose: Initialize Hive Database for Structured Big Data Storage & Querying
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cotton_yield_db
COMMENT 'Hive Database for Indian Agriculture Crop and Cotton Yield Big Data Analytics'
WITH DBPROPERTIES ('creator' = 'DataEngineeringTeam', 'version' = '1.0');

USE cotton_yield_db;
