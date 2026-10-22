# End-to-End System Architecture

## Overview

The **Big Data Cotton Yield Prediction System** represents an enterprise-grade, distributed data engineering and machine learning platform. It automates the lifecycle of agricultural data from raw ingestion to real-time serving.

```mermaid
flowchart TD
    subgraph S1["1. Raw Ingestion Layer"]
        A["Hugging Face Dataset (dhyann2815/india-crop-yield-prediction)"] --> B["Local Storage (data/crop_yield.csv)"]
    end

    subgraph S2["2. Distributed Storage Layer"]
        B --> C["Hadoop HDFS Ingestion (/cotton_yield/raw/)"]
        C --> D["HDFS Storage Manager (Local / Distributed)"]
    end

    subgraph S3["3. Big Data Querying Layer"]
        D --> E["Apache Hive Database (cotton_yield_db)"]
        E --> F["External Table: crop_yield_raw"]
        E --> G["Optimized Analytical Queries (State/Season/Year)"]
    end

    subgraph S4["4. Distributed Processing & Feature Engineering Layer"]
        F --> H["Apache Spark Ingestion Engine"]
        H --> I["Data Cleaning & Deduplication"]
        I --> J["Leakage Prevention: Drop Production"]
        J --> K["Feature Engineering (StringIndexer + OneHotEncoder + VectorAssembler + StandardScaler)"]
    end

    subgraph S5["5. Machine Learning at Scale"]
        K --> L["80/20 Reproducible Split (Seed 42)"]
        L --> M1["Linear Regression (ElasticNet)"]
        L --> M2["Random Forest Regressor (60 Trees)"]
        L --> M3["Gradient-Boosted Trees (50 Iters)"]
        M1 & M2 & M3 --> N["Model Evaluation (RMSE, MAE, R²)"]
        N --> O["Best Model Selection (Random Forest: RMSE=0.6687, R²=0.6160)"]
        O --> P["Persist PipelineModel (models/best_model/)"]
    end

    subgraph S6["6. Relational & Serving Layer"]
        O --> Q["MySQL Database (cotton_yield) / SQLite Resilient Mirror"]
        Q --> R["Tables: model_registry, model_metrics, cotton_predictions"]
        P & Q --> S["Flask Web Dashboard & REST API (/api/predict)"]
        S --> T["End User / Precision Agriculture Stakeholders"]
    end
```

## Architectural Rationale

| Layer | Technology | Primary Role & Rationale |
| :--- | :--- | :--- |
| **Distributed Storage** | Hadoop HDFS | Provides fault-tolerant, horizontal data storage with multi-tiered directory abstraction (`raw/`, `processed/`, `features/`, `predictions/`). |
| **Data Warehousing** | Apache Hive | Enables declarative SQL-based querying and schema-on-read querying over raw datasets without driver memory pressure. |
| **Distributed Compute** | Apache Spark (PySpark) | In-memory distributed engine for parallel data transformation, feature scaling, and ETL processing across cluster partitions. |
| **Scalable ML** | Spark MLlib | Distributed machine learning pipelines that bundle preprocessing transformers and tree-based regressors into an exportable single pipeline artifact. |
| **Relational Storage** | MySQL / SQLite | Provides ACID relational persistence for model registry tracking, experiment evaluation metrics, and audit logs. |
| **Presentation & API**| Flask + HTML5/CSS3 | Decoupled lightweight web dashboard and JSON REST API for real-time model inference and agricultural monitoring. |
