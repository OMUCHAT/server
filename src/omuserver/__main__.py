from omuserver.server.omuserver import OmuServer

from omu.connection import Address

address = Address(
    host="0.0.0.0",
    port=26423,
    secure=False,
)
server = OmuServer(address)


if __name__ == "__main__":
    server.run()
