import asyncio
from pathlib import Path
from typing import List, Optional

from loguru import logger
from omu.connection import Address
from omu.event import EVENTS

from omuserver.event.event_registry import EventRegistry
from omuserver.extension import ExtensionRegistry, ExtensionRegistryServer
from omuserver.extension.endpoint import EndpointExtension
from omuserver.extension.server import ServerExtension
from omuserver.extension.table import TableExtension
from omuserver.network import Network
from omuserver.network.websockets_network import WebsocketsNetwork

from .server import Server, ServerListener


class WebsocketsServer(Server):
    def __init__(
        self,
        address: Address,
        network: Optional[Network] = None,
        extensions: Optional[ExtensionRegistry] = None,
        data_dir: Optional[Path] = None,
    ) -> None:
        self._address = address
        self._listeners: List[ServerListener] = []
        self._network = network or WebsocketsNetwork(self)
        self._events = EventRegistry(self)
        self._events.register(EVENTS.Connect, EVENTS.Ready)
        self._extensions = extensions or ExtensionRegistryServer(self)
        self._data_dir = data_dir or Path.cwd() / "data"
        self._running = False
        self._endpoint = self.extensions.register(EndpointExtension)
        self._tables = self.extensions.register(TableExtension)
        self._server = self.extensions.register(ServerExtension)

    def run(self) -> None:
        loop = asyncio.get_event_loop()

        try:
            loop.set_exception_handler(self.handle_exception)
            loop.create_task(self.start())
            loop.run_forever()
        finally:
            loop.close()
            asyncio.run(self.shutdown())

    def handle_exception(self, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        logger.error(context["message"])
        exception = context.get("exception")
        logger.error(exception)

    async def start(self) -> None:
        self._running = True
        await self._network.start()
        for listener in self._listeners:
            await listener.on_initialized()

    async def shutdown(self) -> None:
        self._running = False
        for listener in self._listeners:
            await listener.on_shutdown()

    def add_listener(self, listener: ServerListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: ServerListener) -> None:
        self._listeners.remove(listener)

    @property
    def address(self) -> Address:
        return self._address

    @property
    def network(self) -> Network:
        return self._network

    @property
    def events(self) -> EventRegistry:
        return self._events

    @property
    def extensions(self) -> ExtensionRegistry:
        return self._extensions

    @property
    def endpoints(self) -> EndpointExtension:
        return self._endpoint

    @property
    def tables(self) -> TableExtension:
        return self._tables

    @property
    def data_path(self) -> Path:
        return self._data_dir

    @property
    def running(self) -> bool:
        return self._running
