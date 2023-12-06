from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List

if TYPE_CHECKING:
    from omu.interface import Serializable

    from omuserver.session import Session


class TableServer[T](abc.ABC):
    @property
    @abc.abstractmethod
    def serializer(self) -> Serializable[T, Any]:
        ...

    @property
    @abc.abstractmethod
    def cache(self) -> Dict[str, T]:
        ...

    @abc.abstractmethod
    def attach_session(self, session: Session) -> None:
        ...

    @abc.abstractmethod
    def detach_session(self, session: Session) -> None:
        ...

    @abc.abstractmethod
    async def save(self) -> None:
        ...

    @abc.abstractmethod
    async def load(self) -> None:
        ...

    @abc.abstractmethod
    async def get(self, key: List[str]) -> T | None:
        ...

    @abc.abstractmethod
    async def get_all(self, keys: List[str]) -> Dict[str, T]:
        ...

    @abc.abstractmethod
    async def add(self, items: Dict[str, T]) -> None:
        ...

    @abc.abstractmethod
    async def set(self, items: Dict[str, T]) -> None:
        ...

    @abc.abstractmethod
    async def remove(self, items: List[str]) -> None:
        ...

    @abc.abstractmethod
    async def clear(self) -> None:
        ...

    @abc.abstractmethod
    async def fetch(self, limit: int = 100, cursor: str | None = None) -> Dict[str, T]:
        ...

    @abc.abstractmethod
    async def iterator(self) -> AsyncIterator[T]:
        ...

    @abc.abstractmethod
    async def size(self) -> int:
        ...

    @abc.abstractmethod
    def add_listener(self, listener: TableListener[T]) -> None:
        ...

    @abc.abstractmethod
    def remove_listener(self, listener: TableListener[T]) -> None:
        ...


class TableListener[T]:
    async def on_add(self, items: Dict[str, T]) -> None:
        ...

    async def on_update(self, items: Dict[str, T]) -> None:
        ...

    async def on_remove(self, items: Dict[str, T]) -> None:
        ...

    async def on_clear(self) -> None:
        ...

    async def on_cache_update(self, cache: Dict[str, T]) -> None:
        ...
