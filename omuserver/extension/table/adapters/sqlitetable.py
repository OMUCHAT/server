import sqlite3
from pathlib import Path
from typing import Dict, List

from .tableadapter import Json, TableAdapter, json


class SqliteTableAdapter(TableAdapter):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn = sqlite3.connect(str(path / "data.db"))
        self._table = "data"
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} (key TEXT PRIMARY KEY, value BLOB)"
        )

    @classmethod
    def create(cls, path: Path) -> "TableAdapter":
        return cls(path)

    async def store(self) -> None:
        self._conn.commit()

    async def load(self) -> None:
        pass

    async def get(self, key: str) -> Json | None:
        cursor = self._conn.execute(
            f"SELECT value FROM {self._table} WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    async def get_all(self, keys: List[str]) -> Dict[str, Json]:
        cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key IN ({','.join(['?'] * len(keys))})",
            keys,
        )
        rows = cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def set(self, key: str, value: Json) -> None:
        self._conn.execute(
            f"INSERT OR REPLACE INTO {self._table} (key, value) VALUES (?, ?)",
            (key, json.dumps(value)),
        )

    async def set_all(self, items: Dict[str, Json]) -> None:
        self._conn.executemany(
            f"INSERT OR REPLACE INTO {self._table} (key, value) VALUES (?, ?)",
            [(key, json.dumps(value)) for key, value in items.items()],
        )

    async def remove(self, key: str) -> None:
        self._conn.execute(f"DELETE FROM {self._table} WHERE key = ?", (key,))

    async def remove_all(self, keys: List[str]) -> None:
        self._conn.execute(
            f"DELETE FROM {self._table} WHERE key IN ({','.join(['?'] * len(keys))})",
            keys,
        )

    async def fetch_forward(self, limit: int, cursor: str) -> Dict[str, Json]:
        _cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key > ? ORDER BY key LIMIT ?",
            (cursor, limit),
        )
        rows = _cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def fetch_backward(self, limit: int, cursor: str) -> Dict[str, Json]:
        _cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key < ? ORDER BY key DESC LIMIT ?",
            (cursor, limit),
        )
        rows = _cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def first(self) -> str | None:
        cursor = self._conn.execute(
            f"SELECT key FROM {self._table} ORDER BY key LIMIT 1"
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return row[0]

    async def last(self) -> str | None:
        cursor = self._conn.execute(
            f"SELECT key FROM {self._table} ORDER BY key DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return row[0]

    async def clear(self) -> None:
        self._conn.execute(f"DELETE FROM {self._table}")

    async def size(self) -> int:
        cursor = self._conn.execute(f"SELECT COUNT(*) FROM {self._table}")
        row = cursor.fetchone()
        return row[0]
