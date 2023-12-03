from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omuserver.session import Session


class Network(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    def add_listener(self, listener: NetworkListener) -> None:
        ...

    @abc.abstractmethod
    def remove_listener(self, listener: NetworkListener) -> None:
        ...


class NetworkListener:
    async def on_connected(self, session: Session) -> None:
        ...

    async def on_disconnected(self, session: Session) -> None:
        ...
