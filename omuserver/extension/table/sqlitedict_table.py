from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List

import sqlitedict
from omu.extension.table.model.table_info import TableInfo
from omu.extension.table.table_extension import (
    TableItemAddEvent,
    TableItemClearEvent,
    TableItemRemoveEvent,
    TableItemsReq,
    TableItemUpdateEvent,
    TableReq,
)

from .table import ServerTable

if TYPE_CHECKING:
    from omu.interface import Serializable

    from omuserver.server import Server
    from omuserver.session import Session

    from .table import TableListener

from .session_table_handler import SessionTableHandler


class SqlitedictTable[T](ServerTable[T]):
    def __init__(
        self,
        server: Server,
        path: Path,
        info: TableInfo,
        serializer: Serializable[T, Any],
    ):
        self._server = server
        self._path = path
        self._info = info
        self._serializer = serializer
        self._db = sqlitedict.SqliteDict(path / "data.db", autocommit=True)
        self._use_cache = info.cache or False
        self._cache: Dict[str, T] = {}
        self._cache_size = info.cache_size or 1000
        self._listeners: List[TableListener] = []
        self._handlers: Dict[Session, SessionTableHandler] = {}
        server.events.add_listener(
            TableItemAddEvent,
            self._on_table_item_add,
        )
        server.events.add_listener(
            TableItemUpdateEvent,
            self._on_table_item_update,
        )
        server.events.add_listener(
            TableItemRemoveEvent,
            self._on_table_item_remove,
        )
        server.events.add_listener(
            TableItemClearEvent,
            self._on_table_item_clear,
        )

    async def _on_table_item_add(self, session: Session, items: TableItemsReq) -> None:
        if items["type"] != self._info.key():
            return
        await self.add(
            {
                key: self._serializer.deserialize(item)
                for key, item in items["items"].items()
            }
        )

    async def _on_table_item_update(
        self, session: Session, items: TableItemsReq
    ) -> None:
        if items["type"] != self._info.key():
            return
        await self.update(
            {
                key: self._serializer.deserialize(item)
                for key, item in items["items"].items()
            }
        )

    async def _on_table_item_remove(
        self, session: Session, items: TableItemsReq
    ) -> None:
        if items["type"] != self._info.key():
            return
        await self.remove(list(items["items"].keys()))

    async def _on_table_item_clear(self, session: Session, req: TableReq) -> None:
        if req["type"] != self._info.key():
            return
        await self.clear()

    async def save(self) -> None:
        self._db.commit()

    async def load(self) -> None:
        pass

    @property
    def cache(self) -> Dict[str, T]:
        return self._cache

    @property
    def serializer(self) -> Serializable[T, Any]:
        return self._serializer

    def attach_session(self, session: Session) -> None:
        if session in self._handlers:
            raise ValueError("Session already attached")
        handler = SessionTableHandler(self._info, session, self._serializer)
        self._handlers[session] = handler
        self.add_listener(handler)

    def detach_session(self, session: Session) -> None:
        if session not in self._handlers:
            return
        handler = self._handlers.pop(session)
        self.remove_listener(handler)

    async def _add_to_cache(self, items: Dict[str, T]) -> None:
        if not self._use_cache:
            return
        for key, item in items.items():
            self._cache[key] = item
            if len(self._cache) > self._cache_size:
                del self._cache[next(iter(self._cache))]
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)

    async def get(self, key: str) -> T | None:
        if key in self._cache:
            return self._cache[key]
        if key in self._db:
            item = self._serializer.deserialize(self._db[key])
            await self._add_to_cache({key: item})
            return item
        return None

    async def get_all(self, keys: List[str]) -> Dict[str, T]:
        items: Dict[str, T] = {}
        for key in keys:
            if key in self._cache:
                items[key] = self._cache[key]
            elif key in self._db:
                item = self._serializer.deserialize(self._db[key])
                items[key] = item
        await self._add_to_cache(items)
        return items

    async def add(self, items: Dict[str, T]) -> None:
        for key, item in items.items():
            self._db[key] = self._serializer.serialize(item)
        await self._add_to_cache(items)
        for listener in self._listeners:
            await listener.on_add(items)

    async def update(self, items: Dict[str, T]) -> None:
        for key, item in items.items():
            self._db[key] = self._serializer.serialize(item)
        await self._add_to_cache(items)
        for listener in self._listeners:
            await listener.on_update(items)

    async def remove(self, items: list[str]) -> None:
        removed_items: Dict[str, T] = {}
        for key in items:
            if key in self._db:
                item = self._serializer.deserialize(self._db[key])
                del self._db[key]
                removed_items[key] = item
            if key in self._cache:
                del self._cache[key]
        for listener in self._listeners:
            await listener.on_remove(removed_items)

    async def clear(self) -> None:
        self._db.clear()
        self._cache.clear()
        for listener in self._listeners:
            await listener.on_clear()

    async def fetch(self, limit: int = 100, cursor: str | None = None) -> Dict[str, T]:
        items: Dict[str, T] = {}
        keys = list(self._db.keys())
        if cursor is not None:
            cursor_index = keys.index(cursor)
            keys = keys[:cursor_index]
        for key in keys[-limit:]:
            item = self._serializer.deserialize(self._db[key])
            items[key] = item

        await self._add_to_cache(items)
        return items

    async def iterator(self) -> AsyncIterator[T]:
        cursor: str | None = None
        while True:
            items = await self.fetch(100, cursor)
            if len(items) == 0:
                break
            for item in items.values():
                yield item
            *_, cursor = items.keys()

    async def size(self) -> int:
        return len(self._db)

    async def close(self) -> None:
        self._db.close()

    def add_listener(self, listener: TableListener[T]) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: TableListener[T]) -> None:
        self._listeners.remove(listener)
