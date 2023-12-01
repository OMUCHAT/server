from __future__ import annotations

import abc
from typing import Any, Awaitable, Callable

from omu.endpoint.endpoint import EndpointType
from omuserver.session.session import Session


class Network(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    def bind_endpoint[
        ReqData, ResData
    ](
        self,
        type: EndpointType[Any, Any, ReqData, ResData],
        handler: Callable[[ReqData], Awaitable[ResData]],
    ) -> None:
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
