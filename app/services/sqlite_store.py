import json
import os
import threading
from typing import Any, Optional


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), "data", "mpt_store.db")


class SQLiteStore:
    """SQLite-backed key-value store for configuration and task data."""

    def __init__(self, db_path: str = DB_PATH):
        self._db_path = db_path
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            conn.execute(
                """CREATE TABLE IF NOT EXISTS kv (
                    category TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (category, key)
                )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )"""
            )
            conn.commit()
            conn.close()

    def _connect(self):
        import sqlite3
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def set(self, category: str, key: str, value: Any):
        with self._lock:
            conn = self._connect()
            conn.execute(
                "INSERT OR REPLACE INTO kv (category, key, value, updated_at) VALUES (?, ?, ?, ?)",
                (category, key, json.dumps(value), __import__("time").time()),
            )
            conn.commit()
            conn.close()

    def get(self, category: str, key: str, default: Any = None) -> Any:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT value FROM kv WHERE category = ? AND key = ?",
                (category, key),
            ).fetchone()
            conn.close()
            if row is None:
                return default
            return json.loads(row["value"])

    def delete(self, category: str, key: str):
        with self._lock:
            conn = self._connect()
            conn.execute(
                "DELETE FROM kv WHERE category = ? AND key = ?",
                (category, key),
            )
            conn.commit()
            conn.close()

    def list_keys(self, category: str) -> list:
        with self._lock:
            conn = self._connect()
            rows = conn.execute(
                "SELECT key FROM kv WHERE category = ?", (category,)
            ).fetchall()
            conn.close()
            return [row["key"] for row in rows]

    def list_by_prefix(self, category: str, prefix: str = "") -> dict:
        with self._lock:
            conn = self._connect()
            if prefix:
                rows = conn.execute(
                    "SELECT key, value FROM kv WHERE category = ? AND key LIKE ? || '%'",
                    (category, prefix),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT key, value FROM kv WHERE category = ?", (category,)
                ).fetchall()
            conn.close()
            return {row["key"]: json.loads(row["value"]) for row in rows}

    def save_task(self, task_id: str, data: dict):
        with self._lock:
            conn = self._connect()
            conn.execute(
                """INSERT OR REPLACE INTO tasks (task_id, data, created_at, updated_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    task_id,
                    json.dumps(data),
                    data.get("created_at", __import__("time").time()),
                    __import__("time").time(),
                ),
            )
            conn.commit()
            conn.close()

    def get_task(self, task_id: str) -> Optional[dict]:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT data FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            conn.close()
            if row is None:
                return None
            return json.loads(row["data"])

    def delete_task(self, task_id: str):
        with self._lock:
            conn = self._connect()
            conn.execute(
                "DELETE FROM tasks WHERE task_id = ?", (task_id,)
            )
            conn.commit()
            conn.close()

    def get_all_tasks(self, page: int = 1, page_size: int = 20) -> tuple:
        with self._lock:
            conn = self._connect()
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            offset = (page - 1) * page_size
            rows = conn.execute(
                "SELECT task_id, data FROM tasks ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (page_size, offset),
            ).fetchall()
            conn.close()
            tasks = [json.loads(row["data"]) for row in rows]
            return tasks, total

    def patch_task(self, task_id: str, **kwargs) -> bool:
        with self._lock:
            conn = self._connect()
            row = conn.execute(
                "SELECT data FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                conn.close()
                return False
            data = json.loads(row["data"])
            data.update(kwargs)
            data["updated_at"] = __import__("time").time()
            conn.execute(
                "UPDATE tasks SET data = ?, updated_at = ? WHERE task_id = ?",
                (json.dumps(data), data["updated_at"], task_id),
            )
            conn.commit()
            conn.close()
            return True


_store: Optional[SQLiteStore] = None


def get_store() -> SQLiteStore:
    global _store
    if _store is None:
        _store = SQLiteStore()
    return _store
