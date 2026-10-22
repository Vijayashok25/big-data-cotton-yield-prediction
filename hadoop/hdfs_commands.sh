#!/usr/bin/env bash
# ==============================================================================
# HDFS Operational Reference Commands
# Big Data Cotton Yield Prediction System
# ==============================================================================

# 1. HDFS Directory Management
echo "--- 1. Creating directories ---"
hadoop fs -mkdir -p /cotton_yield/raw
hadoop fs -mkdir -p /cotton_yield/processed
hadoop fs -mkdir -p /cotton_yield/features
hadoop fs -mkdir -p /cotton_yield/predictions

# 2. Uploading Data
echo "--- 2. Uploading raw data ---"
hadoop fs -put -f data/crop_yield.csv /cotton_yield/raw/
hadoop fs -put -f data/cotton_yield.csv /cotton_yield/raw/

# 3. Verifying File Existence & Permissions
echo "--- 3. Listing HDFS contents ---"
hadoop fs -ls -R /cotton_yield

# 4. Checking Disk Usage & Replication
echo "--- 4. Disk Usage ---"
hadoop fs -du -h /cotton_yield

# 5. Reading Preview from HDFS
echo "--- 5. Reading first 10 rows from HDFS ---"
hadoop fs -cat /cotton_yield/raw/cotton_yield.csv | head -n 10

# 6. Copying Processed Results from HDFS to Local
echo "--- 6. Exporting processed layer to local ---"
hadoop fs -get -f /cotton_yield/processed/ /tmp/cotton_processed_export/
