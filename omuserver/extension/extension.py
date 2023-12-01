import abc
from typing import Self

from omuserver.server import Server


class Extension(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def create(cls, server: Server) -> Self:
        raise NotImplementedError
