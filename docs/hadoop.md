# Hadoop Distributed File System (HDFS) Architecture

## Why Hadoop HDFS?

In enterprise Big Data architectures, Hadoop HDFS is the standard distributed, scalable, and fault-tolerant storage layer. It provides:
- **Block-level replication**: Protects datasets against hardware failures.
- **Data Locality**: Allows Apache Spark executors to process partitions directly where data blocks reside.
- **Horizontal Scalability**: Seamlessly scales from gigabytes to petabytes as more state agricultural records are collected.

---

## 4-Tier HDFS Directory Hierarchy

The project defines a dedicated HDFS directory layout:

```text
/cotton_yield/
├── raw/             <- Ingestion layer for unaltered raw CSV datasets
│   ├── crop_yield.csv
│   └── cotton_yield.csv
├── processed/       <- Cleaned, deduplicated, columnar Parquet partitions
│   └── cotton_cleaned.parquet/
├── features/        <- Pre-computed feature vectors and intermediate matrices
└── predictions/     <- Batch inference scores and audit exports
    └── sample_predictions.csv
```

---

## HDFS Operations & CLI Commands

### 1. Create HDFS Directories
```bash
hadoop fs -mkdir -p /cotton_yield/raw
hadoop fs -mkdir -p /cotton_yield/processed
hadoop fs -mkdir -p /cotton_yield/features
hadoop fs -mkdir -p /cotton_yield/predictions
```

### 2. Ingest Local Dataset into HDFS Raw Layer
```bash
hadoop fs -put -f data/crop_yield.csv /cotton_yield/raw/
hadoop fs -put -f data/cotton_yield.csv /cotton_yield/raw/
```

### 3. Verify Files in HDFS
```bash
hadoop fs -ls -h /cotton_yield/raw/
hadoop fs -count /cotton_yield/raw/*
```

### 4. Read from HDFS
```bash
hadoop fs -cat /cotton_yield/raw/cotton_yield.csv | head -n 10
```

---

## Programmatic HDFS Bridge (`hadoop/hdfs_storage.py`)

The project includes an intelligent Python HDFS manager:
- In a **live Hadoop cluster**, it executes native `hadoop fs` commands over distributed nodes.
- In a **developer workstation / Windows environment**, it maintains an isolated local HDFS hierarchy (`data/hdfs/raw/`, `processed/`, etc.) with full verification checks, ensuring 100% reproducibility across operating systems.
