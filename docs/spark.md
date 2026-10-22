# Apache Spark (PySpark) Processing & Engineering

## Why Apache Spark?

Apache Spark is the premier in-memory distributed compute engine for large-scale data processing. Compared to traditional MapReduce, Spark operates up to 100x faster by keeping intermediate transformation states in distributed memory (Resilient Distributed Datasets / DataFrames).

---

## Spark Processing Architecture

The PySpark pipeline is structured across modular stages:

### 1. Data Ingestion (`spark/data_ingestion.py`)
- Employs an **explicit StructType schema** rather than costly runtime schema inference.
- Operates in `DROPMALFORMED` mode to reject corrupt records.

### 2. Preprocessing & Distributed Cleaning (`spark/preprocessing.py`)
- Filters target cotton records (`Crop == 'Cotton(lint)'`).
- Cleans whitespace and string variations across state names.
- Deduplicates across composite keys `[Year, State, Season, Area, Yield]`.
- Implements boundary checks: `Area > 0`, `Yield >= 0`.
- Engineers distributed intensity ratios:
  $$\text{Fertilizer\_per\_Area} = \frac{\text{Fertilizer}}{\text{Area} + 10^{-5}}$$
  $$\text{Pesticide\_per\_Area} = \frac{\text{Pesticide}}{\text{Area} + 10^{-5}}$$
- **Eliminates Target Leakage**: Drops `Production` from the training DataFrame.
- Persists cleaned data in columnar **Apache Parquet format** for fast downstream reads.

---

## Spark ML Pipeline Construction (`spark/feature_engineering.py`)

A unified PySpark `Pipeline` orchestrates four transformations:

1. **`StringIndexer`**: Encodes categorical string columns (`State`, `Season`) into discrete numerical indices.
2. **`OneHotEncoder`**: Converts category indices into sparse binary vectors, preventing false ordinal relationships between states.
3. **`VectorAssembler`**: Combines categorical vectors, numerical inputs, and engineered features into a unified dense/sparse feature vector `features_raw`.
4. **`StandardScaler`**: Scales features to unit standard deviation (`withStd=True`, `withMean=False` to preserve sparse vector efficiency).

```text
Raw Features
  ├── Categorical: [State, Season]  ──> StringIndexer ──> OneHotEncoder ──┐
  ├── Numerical:   [Year, Area, Rainfall, Fertilizer, Pesticide] ──────────┼──> VectorAssembler ──> StandardScaler ──> features
  └── Engineered:  [Fertilizer_per_Area, Pesticide_per_Area] ─────────────┘
```
