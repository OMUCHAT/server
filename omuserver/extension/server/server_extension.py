from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger
from omu.extension.server.server_extension import AppsTableType

from omuserver.extension import Extension
from omuserver.extension.table import TableExtension
from omuserver.network import NetworkListener
from omuserver.server.server import ServerListener

if TYPE_CHECKING:
    from omuserver.server import Server
    from omuserver.session.session import Session


class ServerExtension(Extension, NetworkListener, ServerListener):
    def __init__(self, server: Server) -> None:
        self._server = server
        server.network.add_listener(self)
        table = server.extensions.get(TableExtension)
        self.apps = table.register_table(AppsTableType)
        pass

    @classmethod
    def create(cls, server: Server) -> ServerExtension:
        return cls(server)

    async def on_initialized(self) -> None:
        await self.apps.clear()

    async def on_connected(self, session: Session) -> None:
        logger.info(f"Connected: {session.app.name}")
        await self.apps.add({session.app.key(): session.app})

    async def on_disconnected(self, session: Session) -> None:
        logger.info(f"Disconnected: {session.app.name}")
        await self.apps.remove([session.app.key()])

    def stop(self) -> None:
        pass
