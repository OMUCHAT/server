from typing import Any, Awaitable, Callable, Dict, List

import websockets
from loguru import logger
from omu.endpoint.endpoint import EndpointType

from omuserver.network.network import Network, NetworkListener
from omuserver.server import Server, ServerListener
from omuserver.session.session import Session, SessionListener
from omuserver.session.websockets_session import WebSocketsSession


class WebsocketsNetwork(Network, ServerListener, SessionListener):
    def __init__(self, server: Server) -> None:
        self._server = server
        self._listeners: List[NetworkListener] = []
        self._endpoints: Dict[str, Callable[[Any], Awaitable[Any]]] = {}
        self._sessions: Dict[str, WebSocketsSession] = {}
        self._start = websockets.serve(
            self._websocket_handler,
            self._server.address.host,
            self._server.address.port,
        )
        server.add_listener(self)

    def bind_endpoint[ReqData, ResData](
        self,
        type: EndpointType[Any, Any, ReqData, ResData],
        handler: Callable[[ReqData], Awaitable[ResData]],
    ) -> None:
        key = type.info.key()
        if key in self._endpoints:
            raise ValueError(f"Endpoint {key} already bound")
        self._endpoints[key] = handler

    def _wrap_handler[ReqData, ResData](
        self,
        type: EndpointType[Any, Any, ReqData, ResData],
        handler: Callable[[ReqData], Awaitable[ResData]],
    ) -> Callable[[ReqData], Awaitable[ResData]]:
        async def _handler(req_data: dict) -> Any:
            req = type.request_serializer.deserialize(req_data)  # type: ignore
            res = await handler(req)
            return type.response_serializer.serialize(res)

        return _handler  # type: ignore

    async def _websocket_handler(
        self, websocket: websockets.WebSocketServerProtocol
    ) -> None:
        session = await WebSocketsSession.create(websocket)
        if session.app.key() in self._sessions:
            # raise ValueError(f"Session {session.app.key()} already exists")
            logger.warning(f"Session {session.app.key()} already exists")
            await self._sessions[session.app.key()].disconnect()
        self._sessions[session.app.key()] = session
        session.add_listener(self)
        for listener in self._listeners:
            await listener.on_connected(session)
        await session.start()

    async def on_disconnected(self, session: Session) -> None:
        if session.app.key() in self._sessions:
            del self._sessions[session.app.key()]
        for listener in self._listeners:
            await listener.on_disconnected(session)

    async def start(self) -> None:
        await self._start

    def add_listener(self, listener: NetworkListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: NetworkListener) -> None:
        self._listeners.remove(listener)
