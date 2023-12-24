from __future__ import annotations

from typing import Any, List

from aiohttp import web
from loguru import logger
from omu.event.event import EventJson, EventType
from omu.extension.server.model.app import App, AppJson

from omuserver.session import Session, SessionListener


class AiohttpSession(Session):
    def __init__(self, socket: web.WebSocketResponse, app: App) -> None:
        self.socket = socket
        self._app = app
        self._listeners: List[SessionListener] = []

    @property
    def app(self) -> App:
        return self._app

    @property
    def closed(self) -> bool:
        return self.socket.closed

    @classmethod
    async def create(cls, socket: web.WebSocketResponse) -> AiohttpSession:
        event = EventJson.from_json_as(AppJson, await socket.receive_json())
        try:
            app = App.from_json(event.data)
        except Exception as e:
            raise ValueError(f"Received invalid app: {event}") from e
        return cls(socket, app)

    async def listen(self) -> None:
        try:
            while True:
                try:
                    msg = await self.socket.receive()
                    if msg.type == web.WSMsgType.CLOSE:
                        break
                    elif msg.type == web.WSMsgType.ERROR:
                        break
                    elif msg.type == web.WSMsgType.CLOSED:
                        break
                    elif msg.type == web.WSMsgType.CLOSING:
                        break
                    if msg.data is None:
                        logger.warning(f"Received empty message {msg}")
                        continue
                    json = msg.json()
                    event = EventJson.from_json(json)
                    for listener in self._listeners:
                        await listener.on_event(self, event)
                except RuntimeError:
                    break
        finally:
            await self.disconnect()

    async def disconnect(self) -> None:
        try:
            await self.socket.close()
        except Exception:
            pass
        for listener in self._listeners:
            await listener.on_disconnected(self)

    async def send[T](self, type: EventType[Any, T], data: T) -> None:
        if self.closed:
            raise ValueError("Socket is closed")
        await self.socket.send_json(
            {
                "type": type.type,
                "data": type.serializer.serialize(data),
            }
        )

    def add_listener(self, listener: SessionListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: SessionListener) -> None:
        self._listeners.remove(listener)
