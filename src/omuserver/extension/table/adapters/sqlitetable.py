import sqlite3
from pathlib import Path
from typing import Dict

from .tableadapter import Json, TableAdapter, json


class SqliteTableAdapter(TableAdapter):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn = sqlite3.connect(str(path / "data.db"))
        self._table = "data"
        self._conn.execute(
            # index, key, value
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "key TEXT UNIQUE,"
            "value TEXT"
            ")"
        )
        self._conn.commit()

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

    async def get_all(self, keys: list[str]) -> Dict[str, Json]:
        cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key IN ({','.join('?' for _ in keys)})",
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
            ((key, json.dumps(value)) for key, value in items.items()),
        )

    async def remove(self, key: str) -> None:
        self._conn.execute(f"DELETE FROM {self._table} WHERE key = ?", (key,))

    async def remove_all(self, keys: list[str]) -> None:
        self._conn.execute(
            f"DELETE FROM {self._table} WHERE key IN ({','.join('?' for _ in keys)})",
            keys,
        )

    async def fetch_forward(self, limit: int, cursor: str) -> Dict[str, Json]:
        _cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key => ? ORDER BY id LIMIT ?",
            (cursor, limit),
        )
        rows = _cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def fetch_backward(self, limit: int, cursor: str) -> Dict[str, Json]:
        _cursor = self._conn.execute(
            f"SELECT key, value FROM {self._table} WHERE key <= ? ORDER BY id DESC LIMIT ?",
            (cursor, limit),
        )
        rows = _cursor.fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def first(self) -> str | None:
        _cursor = self._conn.execute(
            f"SELECT key FROM {self._table} ORDER BY id LIMIT 1"
        )
        row = _cursor.fetchone()
        if row is None:
            return None
        return row[0]

    async def last(self) -> str | None:
        _cursor = self._conn.execute(
            f"SELECT key FROM {self._table} ORDER BY id DESC LIMIT 1"
        )
        row = _cursor.fetchone()
        if row is None:
            return None
        return row[0]

    async def clear(self) -> None:
        self._conn.execute(f"DELETE FROM {self._table}")

    async def size(self) -> int:
        _cursor = self._conn.execute(f"SELECT COUNT(*) FROM {self._table}")
        row = _cursor.fetchone()
        if row is None:
            return 0
        return row[0]
