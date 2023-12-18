from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from omu.event import EventJson, EventType
    from omu.extension.server import App


class Session(abc.ABC):
    @property
    @abc.abstractmethod
    def app(self) -> App:
        ...

    @property
    @abc.abstractmethod
    def closed(self) -> bool:
        ...

    @abc.abstractmethod
    async def disconnect(self) -> None:
        ...

    @abc.abstractmethod
    async def send[T](self, type: EventType[Any, T], data: T) -> None:
        ...

    @abc.abstractmethod
    def add_listener(self, listener: SessionListener) -> None:
        ...

    @abc.abstractmethod
    def remove_listener(self, listener: SessionListener) -> None:
        ...


class SessionListener:
    async def on_event(self, session: Session, event: EventJson) -> None:
        ...

    async def on_disconnected(self, session: Session) -> None:
        ...
