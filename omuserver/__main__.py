import asyncio

from omu.connection.address import Address

from omuserver.server.websockets_server import WebsocketsServer

server = WebsocketsServer(
    Address(
        host="0.0.0.0",
        port=26423,
        secure=False,
    )
)


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
