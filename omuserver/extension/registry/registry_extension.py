from __future__ import annotations

from typing import TYPE_CHECKING, Any

from omu.extension.registry.registry_extension import (
    RegistryEventData,
    RegistryGetEndpoint,
    RegistryListenEvent,
    RegistryUpdateEvent,
)

from omuserver.extension import Extension
from omuserver.session.session import Session

from .registry import Registry

if TYPE_CHECKING:
    from omuserver.server import Server


class RegistryExtension(Extension):
    def __init__(self, server: Server) -> None:
        self._server = server
        server.events.register(RegistryListenEvent, RegistryUpdateEvent)
        server.events.add_listener(RegistryListenEvent, self.on_listen)
        server.events.add_listener(RegistryUpdateEvent, self.on_update)
        server.endpoints.bind_endpoint(RegistryGetEndpoint, self.on_get)
        self.registries: dict[str, Registry] = {}

    @classmethod
    def create(cls, server: Server) -> RegistryExtension:
        return cls(server)

    async def on_listen(self, session: Session, key: str) -> None:
        registry = await self.get(key)
        await registry.attach(session)

    async def on_update(self, session: Session, event: RegistryEventData) -> None:
        registry = await self.get(event["key"])
        await registry.store(event["value"])

    async def on_get(self, session: Session, key: str) -> Any:
        registry = await self.get(key)
        return registry.data

    async def get(self, key: str) -> Registry:
        if ":" not in key:
            raise ValueError(f"Invalid registry key: {key}")
        app, name = key.split(":", 1)
        registry = self.registries.get(key)
        if registry is None:
            registry = Registry(self._server, app, name)
            self.registries[key] = registry
            await registry.load()
        return registry
