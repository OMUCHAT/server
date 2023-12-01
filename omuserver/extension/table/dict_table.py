import json
from pathlib import Path
from typing import AsyncIterator, Dict

from omu.extension.table.model.table_info import TableInfo
from omu.extension.table.table_extension import (
    TableItemAddEvent,
    TableItemClearEvent,
    TableItemRemoveEvent,
    TableItemsReq,
    TableItemUpdateEvent,
    TableReq,
)
from omu.interface.serializable import Serializable

from omuserver.extension.table.session_table_handler import SessionTableHandler
from omuserver.server import Server
from omuserver.session.session import Session, SessionListener

from .table import TableListener, TableServer


class DictTable[T](TableServer[T], SessionListener):
    def __init__(
        self,
        server: Server,
        path: Path,
        info: TableInfo,
        serializer: Serializable[T, dict],
    ):
        self._server = server
        self._path = path
        self._info = info
        self._serializer = serializer
        self._cache: Dict[str, T] = {}
        self._listeners: list[TableListener[T]] = []
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
                key: self._serializer.deserialize(value)
                for key, value in items["items"].items()
            }
        )

    async def _on_table_item_update(
        self, session: Session, items: TableItemsReq
    ) -> None:
        if items["type"] != self._info.key():
            return
        await self.set(
            {
                key: self._serializer.deserialize(value)
                for key, value in items["items"].items()
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
        path = self._path / "data.json"
        path.write_text(
            json.dumps(
                {
                    key: self._serializer.serialize(value)
                    for key, value in self._cache.items()
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    async def load(self) -> None:
        if not self._path.exists():
            self._cache = {}
            return
        path = self._path / "data.json"
        if not path.exists():
            self._cache = {}
            return
        data = path.read_text(encoding="utf-8")
        self._cache = {
            key: self._serializer.deserialize(value)
            for key, value in json.loads(data).items()
        }
        pass

    @property
    def cache(self) -> Dict[str, T]:
        return self._cache

    @property
    def serializer(self) -> Serializable[T, dict]:
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
        return self._cache.get(key, None)

    async def add(self, items: Dict[str, T]) -> None:
        self._cache.update(items)
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)
            await listener.on_add(items)

    async def set(self, items: Dict[str, T]) -> None:
        self._cache.update(items)
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)
            await listener.on_update(items)

    async def remove(self, items: list[str]) -> None:
        removed_items: Dict[str, T] = {}
        for key in items:
            if key in self._cache:
                removed_items[key] = self._cache[key]
                del self._cache[key]
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)
            await listener.on_remove(removed_items)

    async def clear(self) -> None:
        self._cache.clear()
        for listener in self._listeners:
            await listener.on_cache_update(self._cache)
            await listener.on_clear()

    async def fetch(self, limit: int = 100, cursor: str | None = None) -> Dict[str, T]:
        items = {}
        keys = list(self._cache.keys())
        if cursor is not None:
            cursor_index = keys.index(cursor)
            keys = keys[cursor_index:]
        for key in keys[:limit]:
            items[key] = self._cache[key]
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
        return len(self._cache)

    def add_listener(self, listener: TableListener[T]) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: TableListener[T]) -> None:
        self._listeners.remove(listener)
