import abc
import datetime
from typing import Any, Optional


class Response(abc.ABC):
    @abc.abstractmethod
    def __init__(self, status: int = 200) -> None:
        ...

    @abc.abstractmethod
    def set_header(self, key: str, value: str) -> None:
        ...

    @abc.abstractmethod
    def set_cookie(
        self,
        key: str,
        value: str,
        *,
        expires: Optional[datetime.datetime] = None,
        max_age: Optional[int] = None,
        domain: Optional[str] = None,
        path: Optional[str] = None,
        secure: bool = False,
        http_only: bool = False,
        same_site: Optional[str] = None,
    ) -> None:
        ...

    @abc.abstractmethod
    def set_body(self, body: bytes) -> None:
        ...

    @abc.abstractmethod
    def set_body_json(self, body: Any) -> None:
        ...

    @abc.abstractmethod
    def set_status(self, status: int) -> None:
        ...

    @abc.abstractmethod
    def set_redirect(self, url: str, status: int = 302) -> None:
        ...
