# MySQL Relational Storage & Integration Layer

## Why MySQL?

While Hadoop HDFS and Apache Hive handle massive batch datasets and analytical scans, downstream operational applications, dashboards, and APIs require **low-latency, ACID-compliant relational transactions**. 

MySQL serves as the serving and audit layer:
- Stores registered model versions and hyperparameters.
- Records benchmark evaluation metrics.
- Logs live inference predictions in real-time.
- Supports indexed queries for dashboard analytics.

---

## Relational Schema Architecture

```text
cotton_yield
├── model_registry       (model_id, model_name, algorithm, train_samples, test_samples, is_best_model, model_path, trained_at)
├── model_metrics        (metric_id, model_name, rmse, mae, r2, evaluated_at)
├── cotton_predictions   (prediction_id, state, season, harvest_year, area_hectares, rainfall_mm, fertilizer_kg, pesticide_kg, predicted_yield, estimated_production_tonnes, model_version, predicted_at)
└── processed_summary    (summary_id, state, record_count, avg_yield, avg_rainfall, avg_fertilizer_intensity, updated_at)
```

---

## Security & Parameterized Querying

All database interactions use **parameterized queries** via `pymysql` and `sqlite3`:
```python
# Parameterized query protects against SQL Injection
insert_query = """
    INSERT INTO cotton_predictions (
        state, season, harvest_year, area_hectares, rainfall_mm,
        fertilizer_kg, pesticide_kg, predicted_yield, estimated_production_tonnes, model_version
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
db_manager.execute_insert(insert_query, (state, season, year, area, rainfall, fert, pest, yield_val, prod_val, version))
```

Database credentials are never hard-coded; they are parsed via environment variables:
- `MYSQL_HOST` (default: `localhost`)
- `MYSQL_PORT` (default: `3306`)
- `MYSQL_USER` (default: `root`)
- `MYSQL_PASSWORD` (configured in `.env`)
- `MYSQL_DATABASE` (default: `cotton_yield`)

---

## Graceful Fallback Mechanism

If a live MySQL server daemon is unreachable (e.g. during offline development or testing without a running MySQL service), the `DatabaseManager` transparently activates an identical **SQLite mirror** at `outputs/cotton_yield_storage.db`. When MySQL is started, all tables can be synced directly.
