"""
Database Connection and Integration Layer
Provides parameterized querying for MySQL with automatic graceful fallback
to local SQLite relational storage when MySQL server daemon is offline.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import os
import sqlite3
import logging
from typing import List, Dict, Any, Tuple
import pymysql

from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    SQLITE_DB_PATH
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("DBConnection")

class DatabaseManager:
    def __init__(self):
        self.host = MYSQL_HOST
        self.port = MYSQL_PORT
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = MYSQL_DATABASE
        self.sqlite_path = SQLITE_DB_PATH
        self.use_mysql = False

        self._check_connection()

    def _check_connection(self):
        """Attempts to connect to MySQL; falls back to SQLite if offline."""
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=2
            )
            conn.close()
            self.use_mysql = True
            logger.info(f"Connected successfully to MySQL database '{self.database}' at {self.host}:{self.port}")
        except Exception as e:
            self.use_mysql = False
            logger.warning(
                f"MySQL server is offline or unreachable ({e}). "
                f"Activating resilient SQLite storage layer at: {self.sqlite_path}"
            )
            self._init_sqlite()

    def _init_sqlite(self):
        """Initializes equivalent relational tables in SQLite for graceful fallback."""
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.sqlite_path)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_registry (
                model_id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                algorithm TEXT NOT NULL,
                train_samples INTEGER NOT NULL,
                test_samples INTEGER NOT NULL,
                is_best_model BOOLEAN DEFAULT 0,
                model_path TEXT NOT NULL,
                trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_metrics (
                metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                rmse REAL NOT NULL,
                mae REAL NOT NULL,
                r2 REAL NOT NULL,
                evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS cotton_predictions (
                prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                season TEXT NOT NULL,
                harvest_year INTEGER NOT NULL,
                area_hectares REAL NOT NULL,
                rainfall_mm REAL NOT NULL,
                fertilizer_kg REAL NOT NULL,
                pesticide_kg REAL NOT NULL,
                predicted_yield REAL NOT NULL,
                estimated_production_tonnes REAL NOT NULL,
                model_version TEXT NOT NULL,
                predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                avg_yield REAL NOT NULL,
                avg_rainfall REAL NOT NULL,
                avg_fertilizer_intensity REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()
        conn.close()
        logger.info("Relational SQLite tables verified.")

    def execute_query(self, query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        """Executes a parameterized SELECT query and returns records as dictionaries."""
        if self.use_mysql:
            conn = pymysql.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            finally:
                conn.close()
        else:
            conn = sqlite3.connect(self.sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.cursor()
                # Adapt %s to ? for SQLite compatibility if needed
                adapted_query = query.replace("%s", "?")
                cur.execute(adapted_query, params)
                rows = cur.fetchall()
                return [dict(row) for row in rows]
            finally:
                conn.close()

    def execute_insert(self, query: str, params: Tuple = ()) -> int:
        """Executes a parameterized INSERT/UPDATE/DELETE statement."""
        if self.use_mysql:
            conn = pymysql.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, database=self.database
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()
        else:
            conn = sqlite3.connect(self.sqlite_path)
            try:
                cur = conn.cursor()
                adapted_query = query.replace("%s", "?")
                cur.execute(adapted_query, params)
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()

if __name__ == "__main__":
    db = DatabaseManager()
    print(f"Database active storage mode: {'MySQL' if db.use_mysql else 'SQLite (Relational Fallback)'}")
