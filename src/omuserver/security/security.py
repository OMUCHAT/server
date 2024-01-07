import abc
import random
import string
from typing import Dict

import sqlitedict
from omu import App

from omuserver import Server
from omuserver.security import Permission
from omuserver.security.permission import Permissions

type Token = str


class Security(abc.ABC):
    @abc.abstractmethod
    async def get_token(self, app: App, token: Token | None = None) -> Token | None:
        ...

    @abc.abstractmethod
    async def add_permissions(self, token: Token, permissions: Permission) -> None:
        ...

    @abc.abstractmethod
    async def get_permissions(self, token: Token) -> Permission:
        ...


class ServerSecurity(Security):
    def __init__(self, server: Server) -> None:
        self._server = server
        self._permissions: Dict[Token, Permission] = sqlitedict.SqliteDict(
            server.directories.get("security") / "tokens.sqlite", autocommit=True
        )

    async def get_token(self, app: App, token: Token | None = None) -> Token | None:
        if token is None:
            token = self._generate_token()
            self._permissions[token] = Permissions(app.key())
        return token

    async def add_permissions(self, token: Token, permissions: Permission) -> None:
        self._permissions[token] = permissions

    async def get_permissions(self, token: Token) -> Permission:
        return self._permissions[token]

    def _generate_token(self):
        return "".join(random.choices(string.ascii_letters + string.digits, k=16))
