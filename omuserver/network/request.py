import abc

from .headers import Headers


class Request(abc.ABC):
    @property
    @abc.abstractmethod
    def method(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def path(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def query(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def headers(self) -> Headers:
        ...

    @property
    @abc.abstractmethod
    def body(self) -> bytes:
        ...
