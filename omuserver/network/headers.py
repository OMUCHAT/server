import abc


class Headers(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> str | None:
        ...
