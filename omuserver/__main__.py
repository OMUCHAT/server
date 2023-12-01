import asyncio

from omu.connection.address import Address

from omuserver.extension.server.server_extension import ServerExtension
from omuserver.extension.table import TableExtension
from omuserver.fastapi_server import FastApiServer

server = FastApiServer(
    Address(
        host="0.0.0.0",
        port=26423,
        secure=False,
    )
)
server.extensions.register(TableExtension)
server.extensions.register(ServerExtension)


def main() -> None:
    loop = asyncio.get_event_loop()

    try:
        loop.create_task(server.start())
        loop.run_forever()
    finally:
        loop.close()
        asyncio.run(server.shutdown())


if __name__ == "__main__":
    main()
