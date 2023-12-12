from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, AsyncIterator, Dict, List

from omu.extension.table.model import TableInfo

from omuserver.session import SessionListener

from .server_table import ServerTable, TableListener
from .session_table_handler import SessionTableHandler
from .table import Json, Table

if TYPE_CHECKING:
    from omu.interface import Serializable

    from omuserver.server import Server
    from omuserver.session import Session


class CachedTable[T](ServerTable[T], SessionListener):
    def __init__(
        self,
        server: Server,
        info: TableInfo,
        serializer: Serializable[T, Json],
        table: Table,
    ):
        self._server = server
        self._info = info
        self._serializer = serializer
        self._table = table
        self._cache: Dict[str, T] = {}
        self._use_cache = info.cache or False
        self._cache: Dict[str, T] = {}
        self._cache_size = info.cache_size or 512
        self._listeners: list[TableListener[T]] = []
        self._handlers: Dict[Session, SessionTableHandler] = {}
        self._changed = False
        self._save_task: asyncio.Task | None = None

    async def save(self) -> None:
        await self._table.save()

    async def load(self) -> None:
        await self._table.load()

    @property
    def cache(self) -> Dict[str, T]:
        return self._cache

    @property
    def serializer(self) -> Serializable[T, Json]:
        return self._serializer

    def attach_session(self, session: Session) -> None:
        handler = SessionTableHandler(self._info, session, self._serializer)
        self._handlers[session] = handler
        self.add_listener(handler)

    def detach_session(self, session: Session) -> None:
        if session not in self._handlers:
            return
        handler = self._handlers.pop(session)
        self.remove_listener(handler)

    async def get(self, key: str) -> T | None:
        if key in self._cache:
            return self._cache[key]
        data = await self._table.get(key)
        if data is None:
            return None
        item = self._serializer.deserialize(data)
        await self.update_cache({key: item})
        return item

    async def get_all(self, keys: List[str]) -> Dict[str, T]:
        items = {}
        for key in keys:
            if key in self._cache:
                items[key] = self._cache[key]
        if len(items) == len(keys):
            return items
        data = await self._table.get_all(keys)
        for key, value in data.items():
            item = self._serializer.deserialize(value)
            items[key] = item
        await self.update_cache(items)
        return items

    async def add(self, items: Dict[str, T]) -> None:
        await self._table.set_all(
            {key: self._serializer.serialize(value) for key, value in items.items()}
        )
        for listener in self._listeners:
            await listener.on_add(items)
        self.mark_changed()

    async def update(self, items: Dict[str, T]) -> None:
        await self._table.set_all(
            {key: self._serializer.serialize(value) for key, value in items.items()}
        )
        for listener in self._listeners:
            await listener.on_update(items)
        self.mark_changed()

    async def remove(self, items: list[str]) -> None:
        data = await self._table.get_all(items)
        removed = {
            key: self._serializer.deserialize(value) for key, value in data.items()
        }
        await self._table.remove_all(items)
        for listener in self._listeners:
            await listener.on_remove(removed)

    async def clear(self) -> None:
        await self._table.clear()
        for listener in self._listeners:
            await listener.on_clear()
        self._cache.clear()
        self.mark_changed()

    async def fetch(self, limit: int, cursor: str | None = None) -> Dict[str, T]:
        if limit == 0:
            return {}
        if cursor is None:
            cursor = (
                await self._table.first() if limit > 0 else await self._table.last()
            )
        if cursor is None:
            return {}
        data = (
            await self._table.fetch_forward(limit, cursor)
            if limit > 0
            else await self._table.fetch_backward(limit, cursor)
        )
        items = {
            key: self._serializer.deserialize(value) for key, value in data.items()
        }
        await self.update_cache(items)
        return items

    async def iterator(self) -> AsyncIterator[T]:
        cursor: str | None = None
        while True:
            items = await self.fetch(self._cache_size, cursor)
            if len(items) == 0:
                break
            for item in items.values():
                yield item
            *_, cursor = items.keys()

    async def size(self) -> int:
        return len(self._cache)

    def add_listener(self, listener: TableListener[T]) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: TableListener[T]) -> None:
        self._listeners.remove(listener)

    async def save_task(self) -> None:
        while self._changed:
            self._changed = False
            await self.save()
            await asyncio.sleep(30)

    def mark_changed(self) -> None:
        self._changed = True
        if self._save_task is None:
            self._save_task = asyncio.create_task(self.save_task())

    async def update_cache(self, items: Dict[str, T]) -> None:
        if not self.cache:
            return
        for key, item in items.items():
            self._cache[key] = item
            if len(self._cache) > self._cache_size:
                del self._cache[next(iter(self._cache))]
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)
