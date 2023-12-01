import asyncio
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI
from omu.connection.address import Address
from omu.event.events import EVENTS

from omuserver.event.event_registry import EventRegistry
from omuserver.extension.extension_registry import (
    ExtensionRegistry,
    ExtensionRegistryServer,
)
from omuserver.network import FastAPINetwork, Network

from .server import Server, ServerListener


class FastApiServer(Server):
    def __init__(
        self,
        address: Address,
        app: Optional[FastAPI] = None,
        network: Optional[Network] = None,
        extensions: Optional[ExtensionRegistry] = None,
        data_dir: Optional[Path] = None,
    ) -> None:
        self._address = address
        self._app = app or FastAPI()
        self._listeners: List[ServerListener] = []
        self._network = network or FastAPINetwork(self, self._app)
        self._events = EventRegistry(self)
        self._events.register(EVENTS.Connect, EVENTS.Ready)
        self._extensions = extensions or ExtensionRegistryServer(self)
        self._data_dir = data_dir or Path.cwd() / "data"
        self._running = False

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
    def data_path(self) -> Path:
        return self._data_dir

    @property
    def running(self) -> bool:
        return self._running

    def run(self) -> None:
        loop = asyncio.get_event_loop()

        try:
            loop.create_task(self.start())
            loop.run_forever()
        finally:
            loop.close()
            asyncio.run(self.shutdown())

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
