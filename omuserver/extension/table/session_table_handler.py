from typing import Any, Dict

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

from omuserver.extension.table.table import TableListener
from omuserver.session.session import Session


class SessionTableHandler(TableListener):
    def __init__(
        self, info: TableInfo, session: Session, serializer: Serializable
    ) -> None:
        self._info = info
        self._session = session
        self._serializer = serializer

    async def on_add(self, items: Dict[str, Any]) -> None:
        await self._session.send(
            TableItemAddEvent,
            TableItemsReq(
                items={
                    key: self._serializer.serialize(value)
                    for key, value in items.items()
                },
                type=self._info.key(),
            ),
        )

    async def on_update(self, items: Dict[str, Any]) -> None:
        await self._session.send(
            TableItemUpdateEvent,
            TableItemsReq(
                items={
                    key: self._serializer.serialize(value)
                    for key, value in items.items()
                },
                type=self._info.key(),
            ),
        )

    async def on_remove(self, items: Dict[str, Any]) -> None:
        await self._session.send(
            TableItemRemoveEvent,
            TableItemsReq(
                items={
                    key: self._serializer.serialize(value)
                    for key, value in items.items()
                },
                type=self._info.key(),
            ),
        )

    async def on_clear(self) -> None:
        await self._session.send(TableItemClearEvent, TableReq(type=self._info.key()))
