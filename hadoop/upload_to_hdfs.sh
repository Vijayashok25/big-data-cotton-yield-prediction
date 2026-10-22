#!/usr/bin/env bash
# ==============================================================================
# Big Data Cotton Yield Prediction System
# Script: upload_to_hdfs.sh
# Purpose: Create HDFS directory hierarchy and ingest raw dataset into HDFS raw layer
# ==============================================================================

set -e

# Configuration
HDFS_ROOT="/cotton_yield"
LOCAL_DATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../data" && pwd)"
RAW_FILE="${LOCAL_DATA_DIR}/crop_yield.csv"
COTTON_FILE="${LOCAL_DATA_DIR}/cotton_yield.csv"

echo "=========================================================="
echo "HDFS Ingestion: Big Data Cotton Yield Prediction Pipeline"
echo "=========================================================="

# Check Hadoop CLI availability
if ! command -v hadoop &> /dev/null; then
    echo "[!] 'hadoop' command not found in PATH."
    echo "[!] If running in standalone local environment, use python hadoop/hdfs_storage.py"
    echo "    which manages both live HDFS and simulated HDFS storage layers."
    exit 1
fi

echo "[1/4] Creating HDFS Directory Hierarchy..."
hadoop fs -mkdir -p ${HDFS_ROOT}/raw
hadoop fs -mkdir -p ${HDFS_ROOT}/processed
hadoop fs -mkdir -p ${HDFS_ROOT}/features
hadoop fs -mkdir -p ${HDFS_ROOT}/predictions

echo "[2/4] Uploading raw datasets to HDFS raw/ layer..."
if [ -f "${RAW_FILE}" ]; then
    hadoop fs -put -f "${RAW_FILE}" ${HDFS_ROOT}/raw/crop_yield.csv
    echo "  -> Uploaded crop_yield.csv to ${HDFS_ROOT}/raw/"
else
    echo "  [ERROR] ${RAW_FILE} does not exist!"
    exit 1
fi

if [ -f "${COTTON_FILE}" ]; then
    hadoop fs -put -f "${COTTON_FILE}" ${HDFS_ROOT}/raw/cotton_yield.csv
    echo "  -> Uploaded cotton_yield.csv to ${HDFS_ROOT}/raw/"
fi

echo "[3/4] Verifying uploaded files in HDFS..."
hadoop fs -ls -h ${HDFS_ROOT}/raw/

echo "[4/4] Ingestion status check..."
hadoop fs -count ${HDFS_ROOT}/raw/*

echo "=========================================================="
echo "HDFS Ingestion Complete Successfully!"
echo "=========================================================="
