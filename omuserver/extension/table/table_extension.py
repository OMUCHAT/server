from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from loguru import logger
from omu.extension.table.model.table_info import TableInfo
from omu.extension.table.table_extension import (
    TableFetchReq,
    TableItemAddEvent,
    TableItemClearEvent,
    TableItemFetchEndpoint,
    TableItemGetEndpoint,
    TableItemRemoveEvent,
    TableItemSizeEndpoint,
    TableItemsReq,
    TableItemUpdateEvent,
    TableKeysReq,
    TableListenEvent,
    TableRegisterEvent,
    TableReq,
    TableType,
)
from omu.interface import Keyable, Serializer

from omuserver.extension import Extension
from omuserver.network import NetworkListener
from omuserver.server import Server, ServerListener
from omuserver.session import Session

from .cached_table import CachedTable
from .server_table import ServerTable
from .table import DictTable, SqliteTable


class TableExtension(Extension, NetworkListener, ServerListener):
    def __init__(self, server: Server) -> None:
        self._server = server
        self._tables: Dict[str, ServerTable] = {}
        server.events.register(
            TableRegisterEvent,
            TableListenEvent,
            TableItemAddEvent,
            TableItemUpdateEvent,
            TableItemRemoveEvent,
            TableItemClearEvent,
        )
        server.events.add_listener(TableRegisterEvent, self._on_table_register)
        server.events.add_listener(TableListenEvent, self._on_table_listen)
        server.events.add_listener(TableItemAddEvent, self._on_table_item_add)
        server.events.add_listener(TableItemUpdateEvent, self._on_table_item_update)
        server.events.add_listener(TableItemRemoveEvent, self._on_table_item_remove)
        server.events.add_listener(TableItemClearEvent, self._on_table_item_clear)
        server.network.add_listener(self)
        server.endpoints.bind_endpoint(TableItemGetEndpoint, self._on_table_item_get)
        server.endpoints.bind_endpoint(
            TableItemFetchEndpoint, self._on_table_item_fetch
        )
        server.endpoints.bind_endpoint(TableItemSizeEndpoint, self._on_table_item_size)
        server.add_listener(self)

    @classmethod
    def create(cls, server: Server) -> TableExtension:
        return cls(server)

    async def _on_table_item_get(self, req: TableKeysReq) -> TableItemsReq:
        table = self._tables.get(req["type"], None)
        if table is None:
            return TableItemsReq(type=req["type"], items={})
        items = await table.get_all(req["items"])
        return TableItemsReq(
            type=req["type"],
            items={
                key: table.serializer.serialize(item) for key, item in items.items()
            },
        )

    async def _on_table_item_fetch(self, req: TableFetchReq) -> Dict[str, Any]:
        table = self._tables.get(req["type"], None)
        if table is None:
            return {}
        items = await table.fetch(req["limit"], req.get("cursor"))
        return {key: table.serializer.serialize(item) for key, item in items.items()}

    async def _on_table_item_size(self, req: TableReq) -> int:
        table = self._tables.get(req["type"], None)
        if table is None:
            return 0
        return await table.size()

    async def _on_table_register(self, session: Session, info: TableInfo) -> None:
        if info.key() in self._tables:
            logger.warning(f"Skipping table {info.key()} already registered")
            return
        table = self.create_table(info, Serializer.noop())
        await table.load()

    async def _on_table_listen(self, session: Session, type: str) -> None:
        table = self._tables.get(type, None)
        if table is None:
            return
        table.attach_session(session)

    async def _on_table_item_add(self, session: Session, event: TableItemsReq) -> None:
        table = self._tables.get(event["type"], None)
        if table is None:
            return
        await table.add(event["items"])

    async def _on_table_item_update(
        self, session: Session, event: TableItemsReq
    ) -> None:
        table = self._tables.get(event["type"], None)
        if table is None:
            return
        await table.update(event["items"])

    async def _on_table_item_remove(
        self, session: Session, event: TableItemsReq
    ) -> None:
        table = self._tables.get(event["type"], None)
        if table is None:
            return
        await table.remove(list(event["items"].keys()))

    async def _on_table_item_clear(self, session: Session, event: TableReq) -> None:
        table = self._tables.get(event["type"], None)
        if table is None:
            return
        await table.clear()

    async def on_disconnected(self, session: Session) -> None:
        for table in self._tables.values():
            table.detach_session(session)

    def create_table(self, info, serializer):
        path = self.get_table_path(info)
        if info.use_database:
            table = SqliteTable.create(path)
        else:
            table = DictTable.create(path)
        server_table = CachedTable(self._server, info, serializer, table)
        self._tables[info.key()] = server_table
        return server_table

    def register_table[T: Keyable, D](
        self, table_type: TableType[T, Any]
    ) -> ServerTable[T]:
        if table_type.info.key() in self._tables:
            raise Exception(f"Table {table_type.info.key()} already registered")
        table = self.create_table(table_type.info, table_type.serializer)
        return table

    def get_table_path(self, info) -> Path:
        path = self._server.data_path / "tables" / info.extension / info.name
        path.mkdir(parents=True, exist_ok=True)
        return path

    async def on_initialized(self) -> None:
        for table in self._tables.values():
            await table.load()

    async def on_shutdown(self) -> None:
        for table in self._tables.values():
            await table.save()
            await table.save()
