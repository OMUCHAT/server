from omu.connection import Address

from omuserver.server.websockets_server import WebsocketsServer

address = Address(
    host="0.0.0.0",
    port=26423,
    secure=False,
)
server = WebsocketsServer(address)


if __name__ == "__main__":
    server.run()
