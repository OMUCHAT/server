from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omu.connection import Address

    from omuserver.event.event_registry import EventRegistry
    from omuserver.extension.extension_registry import ExtensionRegistry
    from omuserver.network.network import Network


class ServerListener:
    async def on_initialized(self) -> None:
        ...

    async def on_shutdown(self) -> None:
        ...


class Server(abc.ABC):
    @property
    @abc.abstractmethod
    def address(self) -> Address:
        ...

    @property
    @abc.abstractmethod
    def network(self) -> Network:
        ...

    @property
    @abc.abstractmethod
    def events(self) -> EventRegistry:
        ...

    @property
    @abc.abstractmethod
    def extensions(self) -> ExtensionRegistry:
        ...

    @property
    @abc.abstractmethod
    def data_path(self) -> Path:
        ...

    @property
    @abc.abstractmethod
    def running(self) -> bool:
        ...

    @abc.abstractmethod
    def run(self) -> None:
        ...

    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    async def shutdown(self) -> None:
        ...

    @abc.abstractmethod
    def add_listener[T: ServerListener](self, listener: T) -> T:
        ...

    @abc.abstractmethod
    def remove_listener[T: ServerListener](self, listener: T) -> T:
        ...
