from __future__ import annotations

import abc
from typing import Any, Callable, Coroutine, Dict

from loguru import logger
from omu.extension.endpoint.endpoint_extension import (
    EndpointCallEvent,
    EndpointDataReq,
    EndpointError,
    EndpointErrorEvent,
    EndpointInfo,
    EndpointReceiveEvent,
    EndpointRegisterEvent,
    EndpointsTableType,
    EndpointType,
)

from omuserver.extension import Extension
from omuserver.extension.table import TableExtension
from omuserver.server import Server
from omuserver.server.server import ServerListener
from omuserver.session import Session


class Endpoint(abc.ABC):
    @property
    @abc.abstractmethod
    def info(self) -> EndpointInfo:
        ...

    @abc.abstractmethod
    async def call(self, data: EndpointDataReq, session: Session) -> None:
        ...


class SessionEndpoint(Endpoint):
    def __init__(self, session: Session, info: EndpointInfo) -> None:
        self._session = session
        self._info = info

    @property
    def info(self) -> EndpointInfo:
        return self._info

    async def call(self, data: EndpointDataReq, session: Session) -> None:
        await self._session.send(EndpointCallEvent, data)


type Coro[T, R] = Callable[[T], Coroutine[Any, Any, R]]


class ServerEndpoint[Req, Res, ReqData, ResData](Endpoint):
    def __init__(
        self,
        server: Server,
        endpoint: EndpointType[Req, Res, ReqData, ResData],
        callback: Coro[Req, Res],
    ) -> None:
        self._server = server
        self._endpoint = endpoint
        self._callback = callback

    @property
    def info(self) -> EndpointInfo:
        return self._endpoint.info

    async def call(self, data: EndpointDataReq, session: Session) -> None:
        try:
            req = self._endpoint.request_serializer.deserialize(data["data"])
            res = await self._callback(req)
            json = self._endpoint.response_serializer.serialize(res)
            await session.send(
                EndpointReceiveEvent,
                EndpointDataReq(type=data["type"], key=data["key"], data=json),
            )
        except Exception as e:
            await session.send(
                EndpointErrorEvent,
                EndpointError(type=data["type"], key=data["key"], error=str(e)),
            )
            raise e


class EndpointCall:
    def __init__(self, session: Session, data: EndpointDataReq) -> None:
        self._session = session
        self._data = data

    async def receive(self, data: EndpointDataReq) -> None:
        await self._session.send(EndpointReceiveEvent, data)

    async def error(self, error: str) -> None:
        await self._session.send(
            EndpointErrorEvent,
            EndpointError(type=self._data["type"], key=self._data["key"], error=error),
        )


class EndpointExtension(Extension, ServerListener):
    def __init__(self, server: Server) -> None:
        self._server = server
        self._server.add_listener(self)
        self._endpoints: Dict[str, Endpoint] = {}
        self._calls: Dict[str, EndpointCall] = {}
        server.events.register(
            EndpointRegisterEvent,
            EndpointCallEvent,
            EndpointReceiveEvent,
            EndpointErrorEvent,
        )
        server.events.add_listener(EndpointRegisterEvent, self._on_endpoint_register)
        server.events.add_listener(EndpointCallEvent, self._on_endpoint_call)
        server.events.add_listener(EndpointReceiveEvent, self._on_endpoint_receive)
        server.events.add_listener(EndpointErrorEvent, self._on_endpoint_error)

    async def _on_endpoint_register(self, session: Session, info: EndpointInfo) -> None:
        await self.endpoints.add({info.key(): info})

    def bind_endpoint[Req, Res](
        self, type: EndpointType[Req, Res, Any, Any], callback: Coro[Req, Res]
    ) -> None:
        endpoint = ServerEndpoint(self._server, type, callback)
        self._endpoints[type.info.key()] = endpoint

    async def _on_endpoint_call(self, session: Session, req: EndpointDataReq) -> None:
        endpoint = await self._get_endpoint(req, session)
        if endpoint is None:
            return
        await endpoint.call(req, session)
        self._calls[f"{req['type']}:{req['key']}"] = EndpointCall(session, req)

    async def _on_endpoint_receive(
        self, session: Session, req: EndpointDataReq
    ) -> None:
        call = self._calls.get(f"{req['type']}:{req['key']}")
        if call is None:
            await session.send(
                EndpointErrorEvent,
                EndpointError(
                    type=req["type"], key=req["key"], error="Endpoint not connected"
                ),
            )
            return
        await call.receive(req)

    async def _on_endpoint_error(self, session: Session, error: EndpointError) -> None:
        call = self._calls.get(f"{error['type']}:{error['key']}")
        if call is None:
            await session.send(
                EndpointErrorEvent,
                EndpointError(
                    type=error["type"], key=error["key"], error="Endpoint not connected"
                ),
            )
            return
        await call.error(error["error"])

    @classmethod
    def create(cls, server: Server) -> EndpointExtension:
        return cls(server)

    async def _get_endpoint(
        self, req: EndpointDataReq, session: Session
    ) -> Endpoint | None:
        if await self.endpoints.get(req["type"]) is None:
            await session.send(
                EndpointErrorEvent,
                EndpointError(
                    type=req["type"], key=req["key"], error="Endpoint not found"
                ),
            )
            logger.warning(
                f"{session.app.name} tried to call unknown endpoint {req['type']}"
            )
            return
        endpoint = self._endpoints.get(req["type"])
        if endpoint is None:
            await session.send(
                EndpointErrorEvent,
                EndpointError(
                    type=req["type"], key=req["key"], error="Endpoint not connected"
                ),
            )
            logger.warning(
                f"{session.app.name} tried to call unconnected endpoint {req['type']}"
            )
            return
        return endpoint

    async def on_initialized(self) -> None:
        tables = self._server.extensions.get(TableExtension)
        self.endpoints = tables.register_table(EndpointsTableType)
        await self.endpoints.load()
        for key, endpoint in self._endpoints.items():
            await self.endpoints.add({key: endpoint.info})
        await self.endpoints.save()
