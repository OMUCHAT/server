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
    TableItemRemoveEvent,
    TableItemUpdateEvent,
    TableRegisterEvent,
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
        server.endpoints.bind_endpoint(
            TableItemFetchEndpoint, self._on_table_item_fetch
        )
        server.events.register(
            TableRegisterEvent,
            TableItemAddEvent,
            TableItemUpdateEvent,
            TableItemRemoveEvent,
            TableItemClearEvent,
        )
        server.events.add_listener(TableRegisterEvent, self._on_table_register)
        server.network.add_listener(self)
        server.add_listener(self)

    @classmethod
    def create(cls, server: Server) -> TableExtension:
        return cls(server)

    async def _on_table_item_fetch(self, req: TableFetchReq) -> Dict[str, Any]:
        table = self._tables.get(req["type"], None)
        if table is None:
            return {}
        items = await table.fetch(req["limit"], req.get("cursor"))
        return {key: table.serializer.serialize(item) for key, item in items.items()}

    async def _on_table_register(self, session: Session, info: TableInfo) -> None:
        if info.key() in self._tables:
            logger.debug(f"Skipping table registration for {info.key()}")
            return
        table = self.register_from_info(info, Serializer.noop())
        table.attach_session(session)
        await table.load()

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
