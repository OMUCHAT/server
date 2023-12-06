from __future__ import annotations

from typing import Any, Dict
from venv import logger

from omu.extension.table import TableType
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
    TableRegisterEvent,
    TableReq,
)
from omu.interface import Keyable, Serializable, Serializer

from omuserver.extension import Extension
from omuserver.network import NetworkListener
from omuserver.server import Server, ServerListener
from omuserver.session import Session

from .dict_table import DictTable
from .sqlitedict_table import SqlitedictTable
from .table import TableServer


class TableExtension(Extension, NetworkListener, ServerListener):
    def __init__(self, server: Server) -> None:
        self._server = server
        self._tables: Dict[str, TableServer] = {}
        server.endpoints.bind_endpoint(TableItemGetEndpoint, self._on_table_item_get)
        server.endpoints.bind_endpoint(
            TableItemFetchEndpoint, self._on_table_item_fetch
        )
        server.endpoints.bind_endpoint(TableItemSizeEndpoint, self._on_table_item_size)

        server.events.register(
            TableRegisterEvent,
            TableItemAddEvent,
            TableItemUpdateEvent,
            TableItemRemoveEvent,
            TableItemClearEvent,
        )
        server.events.add_listener(TableRegisterEvent, self._on_table_register)
        server.events.add_listener(TableItemAddEvent, self._on_table_item_add)
        server.events.add_listener(TableItemUpdateEvent, self._on_table_item_update)
        server.events.add_listener(TableItemRemoveEvent, self._on_table_item_remove)
        server.events.add_listener(TableItemClearEvent, self._on_table_item_clear)
        server.network.add_listener(self)
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
            logger.debug(f"Skipping table registration for {info.key()}")
            table = self._tables[info.key()]
            table.attach_session(session)
            return
        table = self.register_from_info(info, Serializer.noop())
        table.attach_session(session)
        await table.load()

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
        await table.set(event["items"])

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

    def register_from_info(
        self, info: TableInfo, serializer: Serializable
    ) -> TableServer:
        if info.key() in self._tables:
            table = self._tables[info.key()]
            return table
        path = self._server.data_path / "tables" / info.extension / info.name
        path.mkdir(parents=True, exist_ok=True)
        if info.use_database:
            table = SqlitedictTable(self._server, path, info, serializer)
        else:
            table = DictTable(self._server, path, info, serializer)
        self._tables[info.key()] = table
        return table

    def register[T: Keyable, D](self, table_type: TableType[T, Any]) -> TableServer[T]:
        return self.register_from_info(table_type.info, table_type.serializer)

    def initialize(self) -> None:
        pass

    async def on_initialized(self) -> None:
        for table in self._tables.values():
            await table.load()

    async def on_shutdown(self) -> None:
        for table in self._tables.values():
            await table.save()
