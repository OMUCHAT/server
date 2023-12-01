from __future__ import annotations

from typing import Any, List

from omu.event.event import EventJson, EventType
from omu.extension.server.model.app import App, AppJson
from starlette.websockets import WebSocket, WebSocketDisconnect

from omuserver.session import Session, SessionListener


class WebSocketSession(Session):
    def __init__(self, socket: WebSocket, app: App) -> None:
        self.socket = socket
        self._app = app
        self._listeners: List[SessionListener] = []

    @property
    def app(self) -> App:
        return self._app

    @classmethod
    async def create(cls, socket: WebSocket) -> WebSocketSession:
        event = EventJson.from_json_as(AppJson, await socket.receive_json())
        try:
            app = App.from_json(event.data)
        except Exception as e:
            raise ValueError(f"Received invalid app: {event}") from e
        return cls(socket, app)

    async def _receive(self) -> EventJson:
        return EventJson(**await self.socket.receive_json())

    async def start(self) -> None:
        try:
            while True:
                try:
                    async for message in self.socket.iter_json():
                        event = EventJson(**message)
                        for listener in self._listeners:
                            await listener.on_event(self, event)
                except WebSocketDisconnect:
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
